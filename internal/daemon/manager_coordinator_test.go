package daemon

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
	gitpkg "github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestFailedCoordinatorRunIsTerminalBeforeNextBranchCycle(t *testing.T) {
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
		Status: db.CIWaitFailed, CheckState: "failed", FailureReason: db.CIFailureChecks, AppliedAt: start,
	}); err != nil || !applied {
		t.Fatalf("apply failed wait=%v err=%v", applied, err)
	}

	manager := NewRunManager(database, p, nil)
	if err := manager.cancelActiveRuns(repo.ID, run.Branch); err != nil {
		t.Fatal(err)
	}
	storedRun, err := database.GetRun(run.ID)
	if err != nil || storedRun.Status != types.RunFailed {
		t.Fatalf("failed run=%+v err=%v", storedRun, err)
	}
	storedStep, err := database.GetStepResult(step.ID)
	if err != nil || storedStep.Status != types.StepStatusFailed {
		t.Fatalf("failed step=%+v err=%v", storedStep, err)
	}
	wait, err := database.GetCIWait(waitID)
	if err != nil || wait.Status != db.CIWaitFailed {
		t.Fatalf("failed wait=%+v err=%v", wait, err)
	}
}

func TestCoordinatorCustodyKeepsStreamOpenUntilTerminalCleanup(t *testing.T) {
	p := paths.WithRoot(t.TempDir())
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	database, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	source := filepath.Join(t.TempDir(), "source")
	gitCmd(t, "", "init", source)
	gitCmd(t, source, "config", "user.email", "coordinator@example.invalid")
	gitCmd(t, source, "config", "user.name", "Coordinator Test")
	if err := os.WriteFile(filepath.Join(source, "source.txt"), []byte("source\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, source, "add", "source.txt")
	gitCmd(t, source, "commit", "-m", "source")
	head := gitOutput(t, source, "rev-parse", "HEAD")
	repo, err := database.InsertRepo(source, "https://github.com/test/repo.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	gateDir := p.RepoDir(repo.ID)
	gitCmd(t, "", "init", "--bare", gateDir)
	gitCmd(t, source, "remote", "add", "gate", gateDir)
	gitCmd(t, source, "push", "gate", "HEAD:refs/heads/review")
	run, err := database.InsertRun(repo.ID, "review", head, strings.Repeat("0", 40))
	if err != nil {
		t.Fatal(err)
	}
	workDir := p.WorktreeDir(repo.ID, run.ID)
	if err := gitpkg.WorktreeAdd(context.Background(), gateDir, workDir, head); err != nil {
		t.Fatal(err)
	}
	if err := database.SetRunWorktreeDir(run.ID, workDir); err != nil {
		t.Fatal(err)
	}
	evidenceDir := p.RunEvidenceDir("", run.ID)
	if err := os.MkdirAll(evidenceDir, 0o700); err != nil {
		t.Fatal(err)
	}

	manager := NewRunManager(database, p, nil)
	sub, err := manager.Subscribe(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if _, ok := sub.Next(context.Background()); !ok {
		t.Fatal("initial reconciliation event missing")
	}
	manager.trackCoordinatorCustody(run.ID, repo.ID, gateDir, workDir, &config.Config{})
	manager.subMu.Lock()
	closedBeforeTerminal := sub.mb.closed || manager.completedRuns[run.ID]
	manager.subMu.Unlock()
	if closedBeforeTerminal {
		t.Fatal("coordinator handoff closed a live subscription")
	}
	manager.handleCoordinatorResult(db.CIReconciliationWork{Wait: db.CIWait{RunID: run.ID}}, db.CIReconciliationResult{
		Status: db.CIWaitFailed, FailureReason: db.CIFailureChecks,
	})
	if _, err := os.Stat(workDir); !os.IsNotExist(err) {
		t.Fatalf("terminal coordinator worktree still exists: %v", err)
	}
	if _, err := os.Stat(evidenceDir); !os.IsNotExist(err) {
		t.Fatalf("terminal coordinator evidence still exists: %v", err)
	}
	manager.subMu.Lock()
	closedAfterTerminal := sub.mb.closed && manager.completedRuns[run.ID]
	manager.subMu.Unlock()
	if !closedAfterTerminal {
		t.Fatal("terminal coordinator result did not close the stream")
	}
}

func TestCoordinatorTerminalCleanupAfterRestartUsesPersistedEvidenceRoot(t *testing.T) {
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
	run, err := database.InsertRun(repo.ID, "review", strings.Repeat("a", 40), strings.Repeat("0", 40))
	if err != nil {
		t.Fatal(err)
	}
	customRoot := filepath.Join(t.TempDir(), "custom-evidence")
	evidenceDir := filepath.Join(customRoot, run.ID)
	if err := os.MkdirAll(evidenceDir, 0o700); err != nil {
		t.Fatal(err)
	}

	manager := NewRunManager(database, p, nil) // empty custody map models restart
	manager.handleCoordinatorResult(db.CIReconciliationWork{Wait: db.CIWait{
		RunID: run.ID, EvidenceLocalRoot: customRoot,
	}}, db.CIReconciliationResult{Status: db.CIWaitFailed, FailureReason: db.CIFailureChecks})
	if _, err := os.Stat(evidenceDir); !os.IsNotExist(err) {
		t.Fatalf("persisted coordinator evidence still exists after restart cleanup: %v", err)
	}
}
