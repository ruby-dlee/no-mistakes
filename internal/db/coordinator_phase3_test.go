package db

import (
	"strings"
	"testing"
	"time"
)

func TestApplyCIReconciliationUsesExactBindingCAS(t *testing.T) {
	database := openTestDB(t)
	fixture := newPipelineJobFixture(t, database, false)
	start := time.Unix(1_800_000_000, 0)
	state, _, _, err := database.AdvanceBranchDesiredState(BranchDesiredUpdate{
		RepoID: fixture.run.RepoID, Branch: fixture.run.Branch, HeadSHA: fixture.run.HeadSHA,
		InputDigest: fixture.spec.InputDigest, UpdatedAt: start,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitID, err := database.RegisterCIWait(CIWaitSpec{
		RunID: fixture.run.ID, RepoID: fixture.run.RepoID, Branch: fixture.run.Branch,
		PRNumber: 21, HeadSHA: fixture.run.HeadSHA, InputDigest: fixture.spec.InputDigest,
		DesiredGeneration: state.Revision, RegisteredAt: start, ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ScheduleDueCIReconciliations(start, 10); err != nil {
		t.Fatal(err)
	}
	work, err := database.PendingCIReconciliationWork(10)
	if err != nil || len(work) != 1 || work[0].Wait.ID != waitID {
		t.Fatalf("work=%+v err=%v", work, err)
	}

	stale := CIReconciliationResult{
		WaitID: waitID, RepoID: fixture.run.RepoID, Branch: fixture.run.Branch,
		PRNumber: 21, HeadSHA: strings.Repeat("9", 40), InputDigest: fixture.spec.InputDigest,
		DesiredGeneration: state.Revision, Status: CIWaitReady, CheckState: "passed", AppliedAt: start,
	}
	if _, err := database.ApplyCIReconciliation(stale); err == nil {
		t.Fatal("wrong-head reconciliation committed")
	}
	if pending, err := database.PendingCIReconciliationWork(10); err != nil || len(pending) != 1 {
		t.Fatalf("stale result consumed reconciliation: pending=%d err=%v", len(pending), err)
	}

	result := stale
	result.HeadSHA = fixture.run.HeadSHA
	applied, err := database.ApplyCIReconciliation(result)
	if err != nil || !applied {
		t.Fatalf("applied=%v err=%v", applied, err)
	}
	wait, err := database.GetCIWait(waitID)
	if err != nil || wait == nil || wait.Status != CIWaitReady || wait.CheckState != "passed" {
		t.Fatalf("wait=%+v err=%v", wait, err)
	}
	if pending, err := database.PendingCIReconciliationWork(10); err != nil || len(pending) != 0 {
		t.Fatalf("completed result left reconciliation: pending=%d err=%v", len(pending), err)
	}
	if _, err := database.ApplyCIReconciliation(result); err == nil {
		t.Fatal("duplicate terminal application should not revive or rewrite a wait")
	}
}

func TestApplyPendingCIReconciliationConsumesOnlyCurrentPoll(t *testing.T) {
	database := openTestDB(t)
	fixture := newPipelineJobFixture(t, database, false)
	start := time.Unix(1_800_000_000, 0)
	state, _, _, err := database.AdvanceBranchDesiredState(BranchDesiredUpdate{
		RepoID: fixture.run.RepoID, Branch: fixture.run.Branch, HeadSHA: fixture.run.HeadSHA,
		InputDigest: fixture.spec.InputDigest, UpdatedAt: start,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitID, err := database.RegisterCIWait(CIWaitSpec{
		RunID: fixture.run.ID, RepoID: fixture.run.RepoID, Branch: fixture.run.Branch,
		PRNumber: 22, HeadSHA: fixture.run.HeadSHA, InputDigest: fixture.spec.InputDigest,
		DesiredGeneration: state.Revision, RegisteredAt: start, ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ScheduleDueCIReconciliations(start, 10); err != nil {
		t.Fatal(err)
	}
	result := CIReconciliationResult{
		WaitID: waitID, RepoID: fixture.run.RepoID, Branch: fixture.run.Branch,
		PRNumber: 22, HeadSHA: fixture.run.HeadSHA, InputDigest: fixture.spec.InputDigest,
		DesiredGeneration: state.Revision, Status: CIWaitWaiting, CheckState: "pending", AppliedAt: start,
	}
	if applied, err := database.ApplyCIReconciliation(result); err != nil || !applied {
		t.Fatalf("applied=%v err=%v", applied, err)
	}
	wait, err := database.GetCIWait(waitID)
	if err != nil || wait == nil || wait.Status != CIWaitWaiting || wait.CheckState != "pending" {
		t.Fatalf("wait=%+v err=%v", wait, err)
	}
	if pending, err := database.PendingCIReconciliationWork(10); err != nil || len(pending) != 0 {
		t.Fatalf("current poll remains pending=%d err=%v", len(pending), err)
	}
	if count, err := database.ScheduleDueCIReconciliations(start.Add(59*time.Minute), 10); err != nil || count != 0 {
		t.Fatalf("early reschedule=%d err=%v", count, err)
	}
	if count, err := database.ScheduleDueCIReconciliations(start.Add(time.Hour), 10); err != nil || count != 1 {
		t.Fatalf("due reschedule=%d err=%v", count, err)
	}
}
