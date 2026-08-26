package db

import (
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestApplyCIReconciliationProjectsExactRunAndCIStepState(t *testing.T) {
	for _, tc := range []struct {
		name       string
		waitStatus CIWaitStatus
		checkState string
		runStatus  types.RunStatus
		stepStatus types.StepStatus
		ready      bool
	}{
		{name: "waiting", waitStatus: CIWaitWaiting, checkState: "pending", runStatus: types.RunRunning, stepStatus: types.StepStatusRunning},
		{name: "ready", waitStatus: CIWaitReady, checkState: "passed", runStatus: types.RunRunning, stepStatus: types.StepStatusRunning, ready: true},
		{name: "failed", waitStatus: CIWaitFailed, checkState: "failed", runStatus: types.RunFailed, stepStatus: types.StepStatusFailed},
		{name: "closed", waitStatus: CIWaitClosed, checkState: "closed", runStatus: types.RunCompleted, stepStatus: types.StepStatusCompleted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database := openTestDB(t)
			fixture := newPipelineJobFixture(t, database, false)
			step, err := database.InsertStepResult(fixture.run.ID, types.StepCI)
			if err != nil {
				t.Fatal(err)
			}
			if err := database.UpdateRunStatus(fixture.run.ID, types.RunRunning); err != nil {
				t.Fatal(err)
			}
			if err := database.UpdateStepStatus(step.ID, types.StepStatusRunning); err != nil {
				t.Fatal(err)
			}
			start := time.Unix(1_800_000_000, 0)
			desired, _, _, err := database.AdvanceBranchDesiredState(BranchDesiredUpdate{
				RepoID: fixture.run.RepoID, Branch: fixture.run.Branch, HeadSHA: fixture.run.HeadSHA,
				InputDigest: fixture.spec.InputDigest, UpdatedAt: start,
			})
			if err != nil {
				t.Fatal(err)
			}
			waitID, err := database.RegisterCIWait(CIWaitSpec{
				RunID: fixture.run.ID, RepoID: fixture.run.RepoID, Branch: fixture.run.Branch,
				PRNumber: 41, HeadSHA: fixture.run.HeadSHA, InputDigest: fixture.spec.InputDigest,
				DesiredGeneration: desired.Revision, RegisteredAt: start, ReconcileInterval: time.Hour,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := database.ScheduleDueCIReconciliations(start, 10); err != nil {
				t.Fatal(err)
			}
			applied, err := database.ApplyCIReconciliation(CIReconciliationResult{
				WaitID: waitID, RepoID: fixture.run.RepoID, Branch: fixture.run.Branch,
				PRNumber: 41, HeadSHA: fixture.run.HeadSHA, InputDigest: fixture.spec.InputDigest,
				DesiredGeneration: desired.Revision, Status: tc.waitStatus,
				CheckState: tc.checkState, AppliedAt: start.Add(time.Minute),
				FailureReason: func() string {
					if tc.waitStatus == CIWaitFailed {
						return CIFailureChecks
					}
					return ""
				}(),
			})
			if err != nil || !applied {
				t.Fatalf("applied=%v err=%v", applied, err)
			}
			run, err := database.GetRun(fixture.run.ID)
			if err != nil || run.Status != tc.runStatus || (run.CIReadyAt != nil) != tc.ready {
				t.Fatalf("run=%+v err=%v", run, err)
			}
			gotStep, err := database.GetStepResult(step.ID)
			if err != nil || gotStep.Status != tc.stepStatus {
				t.Fatalf("step=%+v err=%v", gotStep, err)
			}
			if tc.waitStatus == CIWaitFailed && (run.AwaitingAgentSince != nil || run.Error == nil || *run.Error == "") {
				t.Fatal("failed CI did not return terminal rerun custody")
			}
		})
	}
}

func TestApplyCIReconciliationDoesNotProjectStaleGeneration(t *testing.T) {
	database := openTestDB(t)
	fixture := newPipelineJobFixture(t, database, false)
	step, err := database.InsertStepResult(fixture.run.ID, types.StepCI)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(fixture.run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateStepStatus(step.ID, types.StepStatusRunning); err != nil {
		t.Fatal(err)
	}
	start := time.Unix(1_800_000_000, 0)
	desired, _, _, err := database.AdvanceBranchDesiredState(BranchDesiredUpdate{
		RepoID: fixture.run.RepoID, Branch: fixture.run.Branch, HeadSHA: fixture.run.HeadSHA,
		InputDigest: fixture.spec.InputDigest, UpdatedAt: start,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitID, err := database.RegisterCIWait(CIWaitSpec{
		RunID: fixture.run.ID, RepoID: fixture.run.RepoID, Branch: fixture.run.Branch,
		PRNumber: 42, HeadSHA: fixture.run.HeadSHA, InputDigest: fixture.spec.InputDigest,
		DesiredGeneration: desired.Revision, RegisteredAt: start, ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ScheduleDueCIReconciliations(start, 10); err != nil {
		t.Fatal(err)
	}
	_, _, _, err = database.AdvanceBranchDesiredState(BranchDesiredUpdate{
		RepoID: fixture.run.RepoID, Branch: fixture.run.Branch, HeadSHA: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		InputDigest: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", UpdatedAt: start.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ApplyCIReconciliation(CIReconciliationResult{
		WaitID: waitID, RepoID: fixture.run.RepoID, Branch: fixture.run.Branch,
		PRNumber: 42, HeadSHA: fixture.run.HeadSHA, InputDigest: fixture.spec.InputDigest,
		DesiredGeneration: desired.Revision, Status: CIWaitReady, CheckState: "passed", AppliedAt: start.Add(time.Minute),
	}); err == nil {
		t.Fatal("stale generation projected into the run")
	}
	run, err := database.GetRun(fixture.run.ID)
	if err != nil || run.CIReadyAt != nil || run.Status != types.RunRunning {
		t.Fatalf("run=%+v err=%v", run, err)
	}
	gotStep, err := database.GetStepResult(step.ID)
	if err != nil || gotStep.Status != types.StepStatusRunning {
		t.Fatalf("step=%+v err=%v", gotStep, err)
	}
}

func TestApplyCIReconciliationPreservesTrustedNoCIReadiness(t *testing.T) {
	database := openTestDB(t)
	fixture := newPipelineJobFixture(t, database, false)
	step, err := database.InsertStepResult(fixture.run.ID, types.StepCI)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(fixture.run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateStepStatus(step.ID, types.StepStatusRunning); err != nil {
		t.Fatal(err)
	}
	start := time.Unix(1_800_000_000, 0)
	desired, _, _, err := database.AdvanceBranchDesiredState(BranchDesiredUpdate{
		RepoID: fixture.run.RepoID, Branch: fixture.run.Branch, HeadSHA: fixture.run.HeadSHA,
		InputDigest: fixture.spec.InputDigest, UpdatedAt: start,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitID, err := database.RegisterCIWait(CIWaitSpec{
		RunID: fixture.run.ID, RepoID: fixture.run.RepoID, Branch: fixture.run.Branch,
		PRNumber: 43, HeadSHA: fixture.run.HeadSHA, InputDigest: fixture.spec.InputDigest,
		DesiredGeneration: desired.Revision, RegisteredAt: start, ReconcileInterval: time.Hour,
		DeclaredNoCI: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ScheduleDueCIReconciliations(start, 10); err != nil {
		t.Fatal(err)
	}
	if applied, err := database.ApplyCIReconciliation(CIReconciliationResult{
		WaitID: waitID, RepoID: fixture.run.RepoID, Branch: fixture.run.Branch,
		PRNumber: 43, HeadSHA: fixture.run.HeadSHA, InputDigest: fixture.spec.InputDigest,
		DesiredGeneration: desired.Revision, Status: CIWaitReady, CheckState: "unknown",
		DeclaredNoCI: true, AppliedAt: start.Add(time.Minute),
	}); err != nil || !applied {
		t.Fatalf("applied=%v err=%v", applied, err)
	}
	run, err := database.GetRun(fixture.run.ID)
	if err != nil || run.CIReadyAt == nil || !run.CIReadyNoCI {
		t.Fatalf("run=%+v err=%v", run, err)
	}
	wait, err := database.GetCIWait(waitID)
	if err != nil || !wait.DeclaredNoCI {
		t.Fatalf("wait=%+v err=%v", wait, err)
	}
}

func TestTerminalizeLegacyFailedCIWaitsRestoresRerunCustody(t *testing.T) {
	database := openTestDB(t)
	fixture := newPipelineJobFixture(t, database, false)
	step, err := database.InsertStepResult(fixture.run.ID, types.StepCI)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(fixture.run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateStepStatus(step.ID, types.StepStatusRunning); err != nil {
		t.Fatal(err)
	}
	start := time.Unix(1_800_000_000, 0)
	desired, _, _, err := database.AdvanceBranchDesiredState(BranchDesiredUpdate{
		RepoID: fixture.run.RepoID, Branch: fixture.run.Branch, HeadSHA: fixture.run.HeadSHA,
		InputDigest: fixture.spec.InputDigest, UpdatedAt: start,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitID, err := database.RegisterCIWait(CIWaitSpec{
		RunID: fixture.run.ID, RepoID: fixture.run.RepoID, Branch: fixture.run.Branch,
		PRNumber: 44, HeadSHA: fixture.run.HeadSHA, InputDigest: fixture.spec.InputDigest,
		DesiredGeneration: desired.Revision, RegisteredAt: start, ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.sql.Exec(`UPDATE ci_waits SET status = 'failed', check_state = 'failed' WHERE id = ?`, waitID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.sql.Exec(`UPDATE step_results SET status = ? WHERE id = ?`, types.StepStatusAwaitingApproval, step.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.sql.Exec(`UPDATE runs SET awaiting_agent_since = ? WHERE id = ?`, start.Unix(), fixture.run.ID); err != nil {
		t.Fatal(err)
	}
	if count, err := database.TerminalizeLegacyFailedCIWaitRuns(start.Add(time.Minute)); err != nil || count != 1 {
		t.Fatalf("terminalized=%d err=%v", count, err)
	}
	run, err := database.GetRun(fixture.run.ID)
	if err != nil || run.Status != types.RunFailed || run.AwaitingAgentSince != nil {
		t.Fatalf("run=%+v err=%v", run, err)
	}
	storedStep, err := database.GetStepResult(step.ID)
	if err != nil || storedStep.Status != types.StepStatusFailed {
		t.Fatalf("step=%+v err=%v", storedStep, err)
	}
}
