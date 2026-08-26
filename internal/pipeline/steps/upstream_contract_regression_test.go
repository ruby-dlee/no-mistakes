package steps

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/agent"
	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
)

func TestUpstreamContract_InitialReviewAndRereviewIgnorePersistedSessions(t *testing.T) {
	reviewRound := 0
	mock := &sessionMockAgent{}
	mock.respond = func(opts agent.RunOpts) *agent.Result {
		switch opts.Purpose {
		case "review":
			reviewRound++
			if reviewRound == 1 {
				return &agent.Result{Output: []byte(
					`{"findings":[{"id":"f-1","severity":"error","description":"prescribed design","action":"auto-fix"}],"summary":"1 issue","risk_level":"medium","risk_rationale":"bug"}`,
				)}
			}
			return &agent.Result{Output: []byte(`{"findings":[],"summary":"clean","risk_level":"low","risk_rationale":"clean"}`)}
		case "review-fix":
			return &agent.Result{Output: completeSemanticRepairResult("implement the repair")}
		default:
			t.Errorf("unexpected agent purpose %q", opts.Purpose)
			return &agent.Result{Output: []byte(`{}`)}
		}
	}

	exec, database, run, repo, workDir := reviewSessionHarness(t, mock, []pipeline.Step{&ReviewStep{}})
	if err := database.UpsertRunAgentSession(
		run.ID,
		string(pipeline.SessionRoleReviewer),
		mock.Name(),
		"legacy-reviewer-session",
	); err != nil {
		t.Fatalf("seed reviewer session: %v", err)
	}
	if err := exec.Execute(context.Background(), run, repo, workDir); err != nil {
		t.Fatalf("execute: %v", err)
	}

	reviews := reviewCalls(mock.snapshot())
	if len(reviews) != 2 {
		t.Fatalf("review calls = %d, want initial review and rereview", len(reviews))
	}
	for i, call := range reviews {
		if call.Session != nil {
			t.Fatalf("review round %d reused session %+v", i+1, call.Session)
		}
	}
}

func TestUpstreamContract_ConfiguredReviewAndTestTimeoutsCancelAgents(t *testing.T) {
	global, err := config.LoadGlobalFromBytes([]byte(
		"review_agent_timeout: 40ms\n" +
			"test_agent_timeout: 55ms\n",
	))
	if err != nil {
		t.Fatalf("load global config: %v", err)
	}
	cfg := config.Merge(global, &config.RepoConfig{})

	tests := []struct {
		name    string
		timeout time.Duration
		step    pipeline.Step
	}{
		{name: "review", timeout: 40 * time.Millisecond, step: &ReviewStep{}},
		{name: "test", timeout: 55 * time.Millisecond, step: &TestStep{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir, baseSHA, headSHA := setupGitRepo(t)
			ag := &mockAgent{
				name: tc.name + "-configured-timeout-probe",
				runFn: func(ctx context.Context, _ agent.RunOpts) (*agent.Result, error) {
					<-ctx.Done()
					return &agent.Result{Text: "late success"}, nil
				},
			}
			sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
			sctx.Config = cfg
			guard, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			sctx.Ctx = guard

			started := time.Now()
			outcome, err := tc.step.Execute(sctx)
			if err == nil {
				t.Fatalf("outcome = %+v, want configured timeout failure", outcome)
			}
			if !strings.Contains(err.Error(), fmt.Sprintf("timed out after %s", tc.timeout)) {
				t.Fatalf("error = %q, want configured %s timeout", err, tc.timeout)
			}
			if elapsed := time.Since(started); elapsed >= time.Second {
				t.Fatalf("configured %s timeout returned after %s", tc.timeout, elapsed)
			}
		})
	}
}
