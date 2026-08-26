package db

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestConcurrentDesiredPushesSupersedeLeasedJobAndFenceResult(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	fixture := newPipelineJobFixture(t, first, false)
	start := time.Unix(1_800_000_000, 0)
	state, replay, _, err := first.AdvanceWorkerDesiredState(BranchDesiredUpdate{RepoID: fixture.run.RepoID, Branch: fixture.run.Branch, HeadSHA: fixture.run.HeadSHA, InputDigest: fixture.spec.InputDigest, UpdatedAt: start})
	if err != nil || replay || state.Revision != 1 {
		t.Fatalf("initial desired state = %+v replay=%v err=%v", state, replay, err)
	}
	fixture.spec.DesiredGeneration = state.Revision
	ciState, _, _, err := first.AdvanceBranchDesiredState(BranchDesiredUpdate{RepoID: fixture.run.RepoID, Branch: fixture.run.Branch, HeadSHA: fixture.run.HeadSHA, InputDigest: fixture.spec.InputDigest, UpdatedAt: start})
	if err != nil {
		t.Fatal(err)
	}
	waitID, err := first.RegisterCIWait(CIWaitSpec{RunID: fixture.run.ID, RepoID: fixture.run.RepoID, Branch: fixture.run.Branch, PRNumber: 9, HeadSHA: fixture.run.HeadSHA, InputDigest: fixture.spec.InputDigest, DesiredGeneration: ciState.Revision, RegisteredAt: start, ReconcileInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := first.EnqueuePipelineJob(fixture.spec)
	if err != nil {
		t.Fatal(err)
	}
	leased, err := first.ClaimPipelineJob(job.Kind, "worker-a", start, time.Minute)
	if err != nil || leased == nil {
		t.Fatalf("lease = %+v, %v", leased, err)
	}

	type result struct {
		state      BranchDesiredState
		superseded int
		err        error
	}
	results := make(chan result, 2)
	begin := make(chan struct{})
	var wg sync.WaitGroup
	for i, database := range []*DB{first, second} {
		wg.Add(1)
		go func(i int, database *DB) {
			defer wg.Done()
			<-begin
			head := strings.Repeat(string(rune('2'+i)), 40)
			input := strings.Repeat(string(rune('b'+i)), 64)
			state, _, superseded, err := database.AdvanceWorkerDesiredState(BranchDesiredUpdate{RepoID: fixture.run.RepoID, Branch: fixture.run.Branch, HeadSHA: head, InputDigest: input, UpdatedAt: start.Add(time.Duration(i+1) * time.Second)})
			results <- result{state: state, superseded: superseded, err: err}
		}(i, database)
	}
	close(begin)
	wg.Wait()
	close(results)
	revisions := map[int64]bool{}
	superseded := 0
	for item := range results {
		if item.err != nil {
			t.Fatal(item.err)
		}
		revisions[item.state.Revision] = true
		superseded += item.superseded
	}
	if !revisions[2] || !revisions[3] || superseded != 1 {
		t.Fatalf("revisions=%v superseded=%d", revisions, superseded)
	}
	if _, _, _, err := first.AdvanceBranchDesiredState(BranchDesiredUpdate{RepoID: fixture.run.RepoID, Branch: fixture.run.Branch, HeadSHA: strings.Repeat("8", 40), InputDigest: strings.Repeat("8", 64), UpdatedAt: start.Add(4 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	var waitStatus string
	if err := first.sql.QueryRow(`SELECT status FROM ci_waits WHERE id = ?`, waitID).Scan(&waitStatus); err != nil || waitStatus != "closed" {
		t.Fatalf("superseded CI wait status=%q err=%v", waitStatus, err)
	}
	if _, err := first.CompletePipelineJob(PipelineJobCompletion{JobID: job.ID, LeaseOwner: "worker-a", LeaseFence: leased.LeaseFence, DesiredHeadSHA: job.DesiredHeadSHA, InputDigest: job.InputDigest, OwnerDecisionHead: job.OwnerDecisionHead, DesiredGeneration: job.DesiredGeneration, ResultDigest: pipelineJobResult, OutputHeadSHA: pipelineJobOutputHead, CompletedAt: start.Add(30 * time.Second)}); err == nil {
		t.Fatal("superseded generation completed")
	}
}

func TestWorkerAndCIDesiredStateAreIndependentAcrossBranchCycles(t *testing.T) {
	database := openTestDB(t)
	fixture := newPipelineJobFixture(t, database, false)
	start := time.Unix(1_800_000_000, 0)
	worker, replay, superseded, err := database.AdvanceWorkerDesiredState(BranchDesiredUpdate{
		RepoID: fixture.run.RepoID, Branch: fixture.run.Branch,
		HeadSHA: fixture.run.HeadSHA, InputDigest: fixture.spec.InputDigest, UpdatedAt: start,
	})
	if err != nil || replay || superseded != 0 || worker.Revision != 1 {
		t.Fatalf("worker desired state=%+v replay=%v superseded=%d err=%v", worker, replay, superseded, err)
	}
	fixture.spec.DesiredGeneration = worker.Revision
	job, _, err := database.EnqueuePipelineJob(fixture.spec)
	if err != nil {
		t.Fatal(err)
	}
	ciInput := strings.Repeat("c", 64)
	ci, _, _, err := database.AdvanceBranchDesiredState(BranchDesiredUpdate{
		RepoID: fixture.run.RepoID, Branch: fixture.run.Branch,
		HeadSHA: fixture.run.HeadSHA, InputDigest: ciInput, UpdatedAt: start.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.RegisterCIWait(CIWaitSpec{
		RunID: fixture.run.ID, RepoID: fixture.run.RepoID, Branch: fixture.run.Branch,
		PRNumber: 327, HeadSHA: fixture.run.HeadSHA, InputDigest: ciInput,
		DesiredGeneration: ci.Revision, RegisteredAt: start, ReconcileInterval: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	current, err := database.GetPipelineJob(job.ID)
	if err != nil || current.Status != PipelineJobQueued {
		t.Fatalf("CI registration changed worker job=%+v err=%v", current, err)
	}
	leased, err := database.ClaimPipelineJob(job.Kind, "worker-a", start.Add(2*time.Second), time.Minute)
	if err != nil || leased == nil || leased.ID != job.ID {
		t.Fatalf("claim after CI registration=%+v err=%v", leased, err)
	}
	if _, replay, superseded, err := database.AdvanceWorkerDesiredState(BranchDesiredUpdate{
		RepoID: fixture.run.RepoID, Branch: fixture.run.Branch,
		HeadSHA: strings.Repeat("d", 40), InputDigest: strings.Repeat("e", 64), UpdatedAt: start.Add(3 * time.Second),
	}); err != nil || replay || superseded != 1 {
		t.Fatalf("next worker cycle replay=%v superseded=%d err=%v", replay, superseded, err)
	}
}

func TestRecoverablePipelineJobRunsRequireCurrentExactWorkerBinding(t *testing.T) {
	database := openTestDB(t)
	fixture := newPipelineJobFixture(t, database, false)
	start := time.Unix(1_800_000_000, 0)
	state, _, _, err := database.AdvanceWorkerDesiredState(BranchDesiredUpdate{
		RepoID: fixture.run.RepoID, Branch: fixture.run.Branch,
		HeadSHA: fixture.run.HeadSHA, InputDigest: fixture.spec.InputDigest, UpdatedAt: start,
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.spec.DesiredGeneration = state.Revision
	if _, _, err := database.EnqueuePipelineJob(fixture.spec); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(fixture.run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateStepStatus(fixture.step.ID, types.StepStatusRunning); err != nil {
		t.Fatal(err)
	}
	ids, err := database.RecoverablePipelineJobRunIDs()
	if err != nil || len(ids) != 1 || ids[0] != fixture.run.ID {
		t.Fatalf("recoverable worker runs=%v err=%v", ids, err)
	}
	if _, _, _, err := database.AdvanceWorkerDesiredState(BranchDesiredUpdate{
		RepoID: fixture.run.RepoID, Branch: fixture.run.Branch,
		HeadSHA: strings.Repeat("f", 40), InputDigest: strings.Repeat("1", 64), UpdatedAt: start.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	ids, err = database.RecoverablePipelineJobRunIDs()
	if err != nil || len(ids) != 0 {
		t.Fatalf("stale worker runs=%v err=%v", ids, err)
	}
}

func TestRecoverablePipelineJobRunsAcceptExactPreNamespaceBinding(t *testing.T) {
	database := openTestDB(t)
	fixture := newPipelineJobFixture(t, database, false)
	start := time.Unix(1_800_000_000, 0)
	worker, _, _, err := database.AdvanceWorkerDesiredState(BranchDesiredUpdate{
		RepoID: fixture.run.RepoID, Branch: fixture.run.Branch,
		HeadSHA: fixture.run.HeadSHA, InputDigest: fixture.spec.InputDigest, UpdatedAt: start,
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.spec.DesiredGeneration = worker.Revision
	if _, _, err := database.EnqueuePipelineJob(fixture.spec); err != nil {
		t.Fatal(err)
	}
	if _, err := database.sql.Exec(`DELETE FROM worker_desired_state WHERE repo_id = ? AND branch = ?`, fixture.run.RepoID, fixture.run.Branch); err != nil {
		t.Fatal(err)
	}
	legacy, _, _, err := database.AdvanceBranchDesiredState(BranchDesiredUpdate{
		RepoID: fixture.run.RepoID, Branch: fixture.run.Branch,
		HeadSHA: fixture.run.HeadSHA, InputDigest: fixture.spec.InputDigest, UpdatedAt: start,
	})
	if err != nil || legacy.Revision != worker.Revision {
		t.Fatalf("legacy binding=%+v worker=%+v err=%v", legacy, worker, err)
	}
	if err := database.UpdateRunStatus(fixture.run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateStepStatus(fixture.step.ID, types.StepStatusRunning); err != nil {
		t.Fatal(err)
	}
	ids, err := database.RecoverablePipelineJobRunIDs()
	if err != nil || len(ids) != 1 || ids[0] != fixture.run.ID {
		t.Fatalf("pre-namespace worker run lost at upgrade: ids=%v err=%v", ids, err)
	}
}

func TestGitHubDeliveriesDeduplicateRejectConflictsAndWakeOnlyExactHead(t *testing.T) {
	database := openTestDB(t)
	fixture := newPipelineJobFixture(t, database, false)
	start := time.Unix(1_800_000_000, 0)
	state, _, _, err := database.AdvanceBranchDesiredState(BranchDesiredUpdate{RepoID: fixture.run.RepoID, Branch: fixture.run.Branch, HeadSHA: fixture.run.HeadSHA, InputDigest: fixture.spec.InputDigest, UpdatedAt: start})
	if err != nil {
		t.Fatal(err)
	}
	waitID, err := database.RegisterCIWait(CIWaitSpec{RunID: fixture.run.ID, RepoID: fixture.run.RepoID, Branch: fixture.run.Branch, PRNumber: 17, HeadSHA: fixture.run.HeadSHA, InputDigest: fixture.spec.InputDigest, DesiredGeneration: state.Revision, RegisteredAt: start, ReconcileInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	old := GitHubDelivery{DeliveryID: "old", PayloadDigest: strings.Repeat("c", 64), RepoID: fixture.run.RepoID, PRNumber: 17, HeadSHA: strings.Repeat("9", 40), EventType: "check_suite", ReceivedAt: start}
	if replay, err := database.AdmitGitHubDelivery(old); err != nil || replay {
		t.Fatalf("old delivery replay=%v err=%v", replay, err)
	}
	if count, err := database.ConfirmGitHubDelivery("old", AuthoritativeGitHubState{RepoID: old.RepoID, PRNumber: 17, HeadSHA: old.HeadSHA, CheckState: "failed"}, start); err != nil || count != 0 {
		t.Fatalf("old-head wake=%d err=%v", count, err)
	}
	current := GitHubDelivery{DeliveryID: "current", PayloadDigest: strings.Repeat("d", 64), RepoID: fixture.run.RepoID, PRNumber: 17, HeadSHA: fixture.run.HeadSHA, EventType: "check_run", ReceivedAt: start.Add(time.Second)}
	if _, err := database.AdmitGitHubDelivery(current); err != nil {
		t.Fatal(err)
	}
	if replay, err := database.AdmitGitHubDelivery(current); err != nil || !replay {
		t.Fatalf("exact replay=%v err=%v", replay, err)
	}
	conflict := current
	conflict.PayloadDigest = strings.Repeat("e", 64)
	if _, err := database.AdmitGitHubDelivery(conflict); err == nil {
		t.Fatal("conflicting delivery ID accepted")
	}
	if _, err := database.ConfirmGitHubDelivery("current", AuthoritativeGitHubState{RepoID: current.RepoID, PRNumber: 17, HeadSHA: old.HeadSHA, CheckState: "passed"}, start); err == nil {
		t.Fatal("wrong authoritative head accepted")
	}
	if count, err := database.ConfirmGitHubDelivery("current", AuthoritativeGitHubState{RepoID: current.RepoID, PRNumber: 17, HeadSHA: current.HeadSHA, CheckState: "passed"}, start); err != nil || count != 1 {
		t.Fatalf("current wake=%d err=%v", count, err)
	}
	if _, err := database.ConfirmGitHubDelivery("current", AuthoritativeGitHubState{RepoID: current.RepoID, PRNumber: 17, HeadSHA: current.HeadSHA, CheckState: "passed"}, start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	pending, err := database.PendingCIReconciliations(10)
	if err != nil || len(pending) != 1 || pending[0].WaitID != waitID {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
}

func TestPeriodicCIReconciliationSurvivesRestartAndOnlyTwoOf56CustodyRowsUseCapacity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(1_800_000_000, 0)
	for i := 0; i < 56; i++ {
		repo, err := database.InsertRepo(t.TempDir(), "https://example.invalid/wait.git", "main")
		if err != nil {
			t.Fatal(err)
		}
		head := strings.Repeat("7", 40)
		run, err := database.InsertRun(repo.ID, "wait-"+string(rune(0x100+i)), head, strings.Repeat("0", 40))
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
		if i >= 54 {
			workerInput := strings.Repeat(fmt.Sprintf("%x", i%15+1), 64)
			worker, _, _, err := database.AdvanceWorkerDesiredState(BranchDesiredUpdate{
				RepoID: repo.ID, Branch: run.Branch, HeadSHA: head, InputDigest: workerInput, UpdatedAt: start,
			})
			if err != nil {
				t.Fatal(err)
			}
			kind := PipelineJobReview
			if i == 55 {
				kind = PipelineJobTest
			}
			if _, _, err := database.EnqueuePipelineJob(PipelineJobSpec{
				RunID: run.ID, StepResultID: step.ID, Kind: kind, Round: 1,
				DesiredHeadSHA: head, InputDigest: workerInput,
				DesiredGeneration: worker.Revision, MaxAttempts: 3,
			}); err != nil {
				t.Fatal(err)
			}
		}
		if i >= 39 {
			continue
		}
		input := strings.Repeat("f", 64)
		state, _, _, err := database.AdvanceBranchDesiredState(BranchDesiredUpdate{RepoID: repo.ID, Branch: run.Branch, HeadSHA: head, InputDigest: input, UpdatedAt: start})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.RegisterCIWait(CIWaitSpec{RunID: run.ID, RepoID: repo.ID, Branch: run.Branch, PRNumber: int64(i + 1), HeadSHA: head, InputDigest: input, DesiredGeneration: state.Revision, RegisteredAt: start, ReconcileInterval: time.Hour}); err != nil {
			t.Fatal(err)
		}
		checkState := "passed"
		if i%2 == 1 {
			checkState = "failed"
		}
		if _, err := database.sql.Exec(`UPDATE ci_waits SET check_state = ? WHERE run_id = ?`, checkState, run.ID); err != nil {
			t.Fatal(err)
		}
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	for index, kind := range []PipelineJobKind{PipelineJobReview, PipelineJobTest} {
		if job, err := database.ClaimPipelineJob(kind, fmt.Sprintf("worker-%d", index), start, time.Hour); err != nil || job == nil {
			t.Fatalf("claim %s=%+v err=%v", kind, job, err)
		}
	}
	if count, err := database.ScheduleDueCIReconciliations(start, 100); err != nil || count != 39 {
		t.Fatalf("scheduled=%d err=%v", count, err)
	}
	recoverable, err := database.RecoverableCIWaitRunIDs()
	if err != nil || len(recoverable) != 39 {
		t.Fatalf("recoverable coordinator waits=%d err=%v", len(recoverable), err)
	}
	pending, err := database.PendingCIReconciliations(100)
	if err != nil || len(pending) != 39 {
		t.Fatalf("pending=%d err=%v", len(pending), err)
	}
	liveness, err := database.UpdaterPipelineLiveness(start)
	if err != nil {
		t.Fatal(err)
	}
	if len(liveness.ActiveWorkerLeases) != 2 || liveness.LegacyActiveRowsIgnored != 56 {
		t.Fatalf("liveness=%+v", liveness)
	}
}

func TestNewDesiredHeadSupersedesReadyWait(t *testing.T) {
	database := openTestDB(t)
	fixture := newPipelineJobFixture(t, database, false)
	start := time.Now().Truncate(time.Second)
	first, _, _, err := database.AdvanceBranchDesiredState(BranchDesiredUpdate{
		RepoID: fixture.run.RepoID, Branch: fixture.run.Branch, HeadSHA: fixture.run.HeadSHA,
		InputDigest: fixture.spec.InputDigest, UpdatedAt: start,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitID, err := database.RegisterCIWait(CIWaitSpec{
		RunID: fixture.run.ID, RepoID: fixture.run.RepoID, Branch: fixture.run.Branch,
		PRNumber: 55, HeadSHA: fixture.run.HeadSHA, InputDigest: fixture.spec.InputDigest,
		DesiredGeneration: first.Revision, RegisteredAt: start, ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.sql.Exec(`UPDATE ci_waits SET status = ? WHERE id = ?`, CIWaitReady, waitID); err != nil {
		t.Fatal(err)
	}
	if _, replay, _, err := database.AdvanceBranchDesiredState(BranchDesiredUpdate{
		RepoID: fixture.run.RepoID, Branch: fixture.run.Branch,
		HeadSHA: strings.Repeat("e", 40), InputDigest: strings.Repeat("f", 64),
		UpdatedAt: start.Add(time.Second),
	}); err != nil || replay {
		t.Fatalf("advance new head replay=%v err=%v", replay, err)
	}
	wait, err := database.GetCIWait(waitID)
	if err != nil || wait.Status != CIWaitClosed {
		t.Fatalf("superseded wait=%+v err=%v", wait, err)
	}
}

func TestCoordinatorSchemaStoresNoWebhookPayload(t *testing.T) {
	database := openTestDB(t)
	allowed := map[string]map[string]bool{
		"github_deliveries":  {"delivery_id": true, "payload_digest": true, "repo_id": true, "pr_number": true, "head_sha": true, "event_type": true, "received_at": true, "confirmed_at": true},
		"ci_waits":           {"id": true, "run_id": true, "repo_id": true, "branch": true, "pr_number": true, "head_sha": true, "input_digest": true, "desired_generation": true, "declared_no_ci": true, "evidence_local_root": true, "trusted_config_bound": true, "status": true, "check_state": true, "next_reconcile_at": true, "interval_seconds": true, "last_delivery_id": true, "created_at": true, "updated_at": true},
		"ci_reconciliations": {"wait_id": true, "reason": true, "delivery_id": true, "requested_at": true},
	}
	for table, columns := range allowed {
		rows, err := database.sql.Query(`PRAGMA table_info(` + table + `)`)
		if err != nil {
			t.Fatal(err)
		}
		seen := 0
		for rows.Next() {
			var cid, notNull, primaryKey int
			var name, kind string
			var defaultValue any
			if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultValue, &primaryKey); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			if !columns[name] {
				rows.Close()
				t.Fatalf("unreviewed %s column %q", table, name)
			}
			seen++
		}
		rows.Close()
		if seen != len(columns) {
			t.Fatalf("%s column count=%d want=%d", table, seen, len(columns))
		}
	}
}
