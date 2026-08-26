package lifecycle

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/ipc"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestActiveWorkUsesTwoDaemonExecutionsAndIgnoresDurableCIWaits(t *testing.T) {
	root, err := os.MkdirTemp("", "nm-lc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	t.Setenv("NM_HOME", root)
	p := paths.WithRoot(root)
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	database, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	repo, err := database.InsertRepo(t.TempDir(), "https://example.invalid/liveness.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	var liveIDs []string
	start := time.Now().Truncate(time.Second)
	for i := 0; i < 56; i++ {
		branch := "stale-" + string(rune(0x100+i))
		run, err := database.InsertRun(repo.ID, branch, strings.Repeat("a", 40), strings.Repeat("0", 40))
		if err != nil {
			t.Fatal(err)
		}
		if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
			t.Fatal(err)
		}
		if i < 45 {
			step, err := database.InsertStepResult(run.ID, types.StepCI)
			if err != nil {
				t.Fatal(err)
			}
			stepStatus := types.StepStatusRunning
			if i >= 37 {
				stepStatus = types.StepStatusAwaitingApproval
			}
			if err := database.UpdateStepStatus(step.ID, stepStatus); err != nil {
				t.Fatal(err)
			}
			if i < 37 {
				input := strings.Repeat("f", 64)
				desired, _, _, err := database.AdvanceBranchDesiredState(db.BranchDesiredUpdate{
					RepoID: repo.ID, Branch: branch, HeadSHA: run.HeadSHA, InputDigest: input, UpdatedAt: start,
				})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := database.RegisterCIWait(db.CIWaitSpec{
					RunID: run.ID, RepoID: repo.ID, Branch: branch, PRNumber: int64(i + 1),
					HeadSHA: run.HeadSHA, InputDigest: input, DesiredGeneration: desired.Revision,
					RegisteredAt: start, ReconcileInterval: time.Hour,
				}); err != nil {
					t.Fatal(err)
				}
			}
		} else if i < 54 {
			stepName := []types.StepName{types.StepReview, types.StepTest, types.StepDocument}[(i-45)%3]
			step, err := database.InsertStepResult(run.ID, stepName)
			if err != nil {
				t.Fatal(err)
			}
			if err := database.UpdateStepStatus(step.ID, types.StepStatusAwaitingApproval); err != nil {
				t.Fatal(err)
			}
		} else {
			stepName := types.StepTest
			if i == 55 {
				stepName = types.StepDocument
			}
			step, err := database.InsertStepResult(run.ID, stepName)
			if err != nil {
				t.Fatal(err)
			}
			if err := database.UpdateStepStatus(step.ID, types.StepStatusRunning); err != nil {
				t.Fatal(err)
			}
		}
		if i >= 54 {
			liveIDs = append(liveIDs, run.ID)
		}
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	srv := ipc.NewServer()
	srv.Handle(ipc.MethodGetExecutingRuns, func(context.Context, json.RawMessage) (interface{}, error) {
		return &ipc.GetExecutingRunsResult{RunIDs: liveIDs}, nil
	})
	if err := srv.Listen(p.Socket()); err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ServeReady() }()
	t.Cleanup(func() {
		srv.Close()
		if err := <-errCh; err != nil {
			t.Errorf("serve: %v", err)
		}
	})

	work, err := ActiveWork(p, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if work.Count() != 2 || len(work.LocalRuns) != 2 ||
		work.LocalRuns[0].ID != liveIDs[0] || work.LocalRuns[1].ID != liveIDs[1] || len(work.WorkerLeases) != 0 {
		t.Fatalf("active work = %+v", work)
	}
}

func TestActiveWorkFailsClosedWhenDaemonIdentityExistsButIPCIsUnavailable(t *testing.T) {
	root := t.TempDir()
	t.Setenv("NM_HOME", root)
	p := paths.WithRoot(root)
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.PIDFile(), []byte("123\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ActiveWork(p, time.Now()); err == nil || !strings.Contains(err.Error(), "execution liveness") {
		t.Fatalf("error = %v", err)
	}
}
