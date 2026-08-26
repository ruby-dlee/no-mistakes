package daemon

import (
	"strings"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestCancelActiveRunsSupersedesFailedCoordinatorWait(t *testing.T) {
	p := paths.WithRoot(t.TempDir())
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	database, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	repo, err := database.InsertRepo(t.TempDir(), "https://github.com/test/repo.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	head := strings.Repeat("a", 40)
	run, err := database.InsertRun(repo.ID, "feature/coordinator", head, strings.Repeat("0", 40))
	if err != nil {
		t.Fatal(err)
	}
	step, err := database.InsertStepResult(run.ID, types.StepCI)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateStepStatus(step.ID, types.StepStatusRunning); err != nil {
		t.Fatal(err)
	}
	input := strings.Repeat("b", 64)
	start := time.Now().Truncate(time.Second)
	desired, _, _, err := database.AdvanceBranchDesiredState(db.BranchDesiredUpdate{
		RepoID: repo.ID, Branch: run.Branch, HeadSHA: head, InputDigest: input, UpdatedAt: start,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitID, err := database.RegisterCIWait(db.CIWaitSpec{
		RunID: run.ID, RepoID: repo.ID, Branch: run.Branch, PRNumber: 42,
		HeadSHA: head, InputDigest: input, DesiredGeneration: desired.Revision,
		RegisteredAt: start, ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ScheduleDueCIReconciliations(start, 10); err != nil {
		t.Fatal(err)
	}
	if applied, err := database.ApplyCIReconciliation(db.CIReconciliationResult{
		WaitID: waitID, RepoID: repo.ID, Branch: run.Branch, PRNumber: 42,
		HeadSHA: head, InputDigest: input, DesiredGeneration: desired.Revision,
		Status: db.CIWaitFailed, CheckState: "failed", AppliedAt: start,
	}); err != nil || !applied {
		t.Fatalf("apply failed wait=%v err=%v", applied, err)
	}

	manager := NewRunManager(database, p, nil)
	if err := manager.cancelActiveRuns(repo.ID, run.Branch); err != nil {
		t.Fatal(err)
	}
	storedRun, err := database.GetRun(run.ID)
	if err != nil || storedRun.Status != types.RunCancelled {
		t.Fatalf("superseded run=%+v err=%v", storedRun, err)
	}
	storedStep, err := database.GetStepResult(step.ID)
	if err != nil || storedStep.Status != types.StepStatusSkipped {
		t.Fatalf("superseded step=%+v err=%v", storedStep, err)
	}
	wait, err := database.GetCIWait(waitID)
	if err != nil || wait.Status != db.CIWaitClosed {
		t.Fatalf("superseded wait=%+v err=%v", wait, err)
	}
}
