package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/agent"
	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestUpstreamContract_DefaultConfigKeepsReviewAutoFixOff(t *testing.T) {
	global, err := config.LoadGlobalFromBytes([]byte("{}\n"))
	if err != nil {
		t.Fatalf("load default global config: %v", err)
	}
	cfg := config.Merge(global, &config.RepoConfig{})

	database, p, run, repo := setupTest(t)
	secondCall := make(chan struct{}, 1)
	var callCount atomic.Int32
	step := &adaptiveCallStep{
		name: types.StepReview,
		fn: func(*StepContext) (*StepOutcome, error) {
			if callCount.Add(1) > 1 {
				secondCall <- struct{}{}
				return &StepOutcome{}, nil
			}
			return &StepOutcome{
				NeedsApproval: true,
				AutoFixable:   true,
				Findings:      `{"findings":[{"severity":"error","description":"bug","action":"auto-fix"}],"summary":"1 issue"}`,
			}, nil
		},
	}

	exec := NewExecutor(database, p, cfg, nil, []Step{step}, nil)
	done, _ := startExecutor(t, exec, run, repo, t.TempDir())

	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-secondCall:
			t.Fatal("default configuration executed a review auto-fix")
		case err := <-done:
			t.Fatalf("executor returned instead of parking review: %v", err)
		case <-deadline.C:
			t.Fatal("review did not park for an owner decision")
		case <-ticker.C:
			steps, getErr := database.GetStepsByRun(run.ID)
			if getErr != nil {
				continue
			}
			for _, got := range steps {
				if got.StepName == types.StepReview && got.Status == types.StepStatusAwaitingApproval {
					if calls := callCount.Load(); calls != 1 {
						t.Fatalf("review calls = %d, want exactly one before approval", calls)
					}
					if err := exec.Respond(types.StepReview, types.ActionApprove, nil); err != nil {
						t.Fatalf("approve review: %v", err)
					}
					waitExecutorDone(t, done)
					return
				}
			}
		}
	}
}

func TestUpstreamContract_ConfiguredAgentTimeoutCancelsInvocation(t *testing.T) {
	global, err := config.LoadGlobalFromBytes([]byte("agent_timeout: 35ms\n"))
	if err != nil {
		t.Fatalf("load global config: %v", err)
	}
	cfg := config.Merge(global, &config.RepoConfig{})

	ag := &hangingAgent{
		name: "configured-timeout-probe",
		runFn: func(ctx context.Context, _ agent.RunOpts) (*agent.Result, error) {
			<-ctx.Done()
			return &agent.Result{Text: "late success"}, nil
		},
	}
	guard, cancel := context.WithCancel(context.Background())
	defer cancel()
	safety := time.AfterFunc(2*time.Second, cancel)
	defer safety.Stop()
	sctx := &StepContext{Ctx: guard, Agent: ag, Config: cfg}

	started := time.Now()
	result, err := sctx.RunAgent(agent.RunOpts{Prompt: "work"})
	if !errors.Is(err, ErrAgentTimeout) {
		t.Fatalf("error = %v, want configured agent timeout", err)
	}
	if result != nil {
		t.Fatalf("result = %+v, want late success rejected", result)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("configured 35ms timeout returned after %s", elapsed)
	}
}
