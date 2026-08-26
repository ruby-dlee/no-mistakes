package db

import (
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/ownerdecision"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

const (
	pipelineJobHead       = "1111111111111111111111111111111111111111"
	pipelineJobInput      = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	pipelineJobResult     = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	pipelineJobOutputHead = "2222222222222222222222222222222222222222"
)

type pipelineJobFixture struct {
	database *DB
	run      *Run
	step     *StepResult
	spec     PipelineJobSpec
}

func newPipelineJobFixture(t *testing.T, database *DB, protected bool) pipelineJobFixture {
	t.Helper()
	repo, err := database.InsertRepo(t.TempDir(), "https://example.invalid/job-plane.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	run, err := database.InsertRun(repo.ID, "feature/job-plane", pipelineJobHead, strings.Repeat("0", 40))
	if err != nil {
		t.Fatal(err)
	}
	step, err := database.InsertStepResult(run.ID, types.StepReview)
	if err != nil {
		t.Fatal(err)
	}
	ownerHead := ""
	if protected {
		publicKey, _, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.ProtectRunOwnerDecisions(run.ID, publicKey); err != nil {
			t.Fatal(err)
		}
		var ok bool
		ownerHead, ok, err = database.OwnerDecisionHead(run.ID)
		if err != nil || !ok {
			t.Fatalf("owner decision head = %q, %v, %v", ownerHead, ok, err)
		}
	}
	return pipelineJobFixture{
		database: database,
		run:      run,
		step:     step,
		spec: PipelineJobSpec{
			RunID:             run.ID,
			StepResultID:      step.ID,
			Kind:              PipelineJobReview,
			Round:             1,
			DesiredHeadSHA:    pipelineJobHead,
			InputDigest:       pipelineJobInput,
			OwnerDecisionHead: ownerHead,
			MaxAttempts:       2,
		},
	}
}

func TestPipelineJobsFreshSchemaMigrationAndSemanticIdempotency(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.sqlite")
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Exec(`
		CREATE TABLE repos (id TEXT PRIMARY KEY, working_path TEXT NOT NULL UNIQUE, upstream_url TEXT NOT NULL, default_branch TEXT NOT NULL DEFAULT 'main', created_at INTEGER NOT NULL);
		CREATE TABLE runs (id TEXT PRIMARY KEY, repo_id TEXT NOT NULL, branch TEXT NOT NULL, head_sha TEXT NOT NULL, base_sha TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', pr_url TEXT, error TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);
		CREATE TABLE step_results (id TEXT PRIMARY KEY, run_id TEXT NOT NULL, step_name TEXT NOT NULL, step_order INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'pending');
	`); err != nil {
		legacy.Close()
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	database, err := Open(path)
	if err != nil {
		t.Fatalf("migrate legacy database: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	for _, table := range []string{"pipeline_jobs", "pipeline_job_events"} {
		var name string
		if err := database.sql.QueryRow(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&name); err != nil {
			t.Fatalf("%s missing after migration: %v", table, err)
		}
	}

	fixture := newPipelineJobFixture(t, database, true)
	job, replay, err := database.EnqueuePipelineJob(fixture.spec)
	if err != nil || replay {
		t.Fatalf("first enqueue = job=%+v replay=%v err=%v", job, replay, err)
	}
	if _, err := database.sql.Exec(`UPDATE runs SET head_sha = ? WHERE id = ?`, pipelineJobOutputHead, fixture.run.ID); err != nil {
		t.Fatal(err)
	}
	duplicate, replay, err := database.EnqueuePipelineJob(fixture.spec)
	if err != nil || !replay || duplicate.ID != job.ID || duplicate.IdempotencyKey != job.IdempotencyKey {
		t.Fatalf("duplicate enqueue = job=%+v replay=%v err=%v", duplicate, replay, err)
	}
	changedPolicy := fixture.spec
	changedPolicy.MaxAttempts++
	if _, _, err := database.EnqueuePipelineJob(changedPolicy); err == nil {
		t.Fatal("same semantic job with a different retry policy was accepted")
	}
	var jobs, createdEvents int
	if err := database.sql.QueryRow(`SELECT COUNT(*) FROM pipeline_jobs`).Scan(&jobs); err != nil || jobs != 1 {
		t.Fatalf("job count = %d, %v", jobs, err)
	}
	if err := database.sql.QueryRow(`SELECT COUNT(*) FROM pipeline_job_events WHERE event_type = 'created'`).Scan(&createdEvents); err != nil || createdEvents != 1 {
		t.Fatalf("created event count = %d, %v", createdEvents, err)
	}
}

func TestPipelineJobClaimHasOneWinnerAcrossSQLiteConnections(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { first.Close() })
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { second.Close() })
	fixture := newPipelineJobFixture(t, first, false)
	if _, _, err := first.EnqueuePipelineJob(fixture.spec); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	type result struct {
		job *PipelineJob
		err error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for i, database := range []*DB{first, second} {
		wg.Add(1)
		go func(owner string, database *DB) {
			defer wg.Done()
			<-start
			job, err := database.ClaimPipelineJob(PipelineJobReview, owner, time.Unix(1_800_000_000, 0), 2*time.Minute)
			results <- result{job: job, err: err}
		}(string(rune('a'+i)), database)
	}
	close(start)
	wg.Wait()
	close(results)
	winners := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("claim error: %v", result.err)
		}
		if result.job != nil {
			winners++
			if result.job.Status != PipelineJobLeased || result.job.AttemptsStarted != 1 || result.job.LeaseFence != 1 {
				t.Fatalf("winning lease = %+v", result.job)
			}
		}
	}
	if winners != 1 {
		t.Fatalf("claim winners = %d, want 1", winners)
	}
}

func TestPipelineJobExpiredLeaseRetriesAndRejectsStaleFence(t *testing.T) {
	database := openTestDB(t)
	fixture := newPipelineJobFixture(t, database, false)
	job, _, err := database.EnqueuePipelineJob(fixture.spec)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(1_800_000_000, 0)
	first, err := database.ClaimPipelineJob(job.Kind, "worker-a", start, time.Minute)
	if err != nil || first == nil {
		t.Fatalf("first claim = %+v, %v", first, err)
	}
	if count, err := database.RequeueExpiredPipelineJobs(start.Add(time.Minute)); err != nil || count != 1 {
		t.Fatalf("requeue expired = %d, %v", count, err)
	}
	second, err := database.ClaimPipelineJob(job.Kind, "worker-b", start.Add(time.Minute), time.Minute)
	if err != nil || second == nil || second.LeaseFence <= first.LeaseFence || second.AttemptsStarted != 2 {
		t.Fatalf("second claim = %+v, %v", second, err)
	}
	stale := PipelineJobCompletion{
		JobID:             job.ID,
		LeaseOwner:        "worker-a",
		LeaseFence:        first.LeaseFence,
		DesiredHeadSHA:    job.DesiredHeadSHA,
		InputDigest:       job.InputDigest,
		OwnerDecisionHead: job.OwnerDecisionHead,
		ResultDigest:      pipelineJobResult,
		OutputHeadSHA:     pipelineJobOutputHead,
		CompletedAt:       start.Add(90 * time.Second),
	}
	if _, err := database.CompletePipelineJob(stale); err == nil {
		t.Fatal("stale first lease completed the second attempt")
	}
	if count, err := database.RequeueExpiredPipelineJobs(start.Add(2 * time.Minute)); err != nil || count != 1 {
		t.Fatalf("exhaust expired lease = %d, %v", count, err)
	}
	stored, err := database.GetPipelineJob(job.ID)
	if err != nil || stored.Status != PipelineJobFailed || stored.ErrorCategory == nil || *stored.ErrorCategory != PipelineJobErrorLeaseExpired {
		t.Fatalf("exhausted job = %+v, %v", stored, err)
	}
}

func TestPipelineJobHeartbeatPreventsExpiryAndMissingHeartbeatRequeues(t *testing.T) {
	database := openTestDB(t)
	fixture := newPipelineJobFixture(t, database, false)
	job, _, err := database.EnqueuePipelineJob(fixture.spec)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(1_800_000_000, 0)
	leased, err := database.ClaimPipelineJob(job.Kind, "worker-a", start, time.Minute)
	if err != nil || leased == nil {
		t.Fatalf("claim = %+v, %v", leased, err)
	}
	heartbeat := PipelineJobHeartbeat{
		JobID:             job.ID,
		LeaseOwner:        "worker-a",
		LeaseFence:        leased.LeaseFence,
		DesiredHeadSHA:    job.DesiredHeadSHA,
		InputDigest:       job.InputDigest,
		OwnerDecisionHead: job.OwnerDecisionHead,
		HeartbeatAt:       start.Add(30 * time.Second),
		LeaseDuration:     time.Minute,
	}
	if _, err := database.HeartbeatPipelineJob(heartbeat); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if count, err := database.RequeueExpiredPipelineJobs(start.Add(time.Minute)); err != nil || count != 0 {
		t.Fatalf("heartbeat lease reaped early = %d, %v", count, err)
	}
	if count, err := database.RequeueExpiredPipelineJobs(start.Add(91 * time.Second)); err != nil || count != 1 {
		t.Fatalf("missing next heartbeat did not requeue = %d, %v", count, err)
	}
}

func TestPipelineJobCompletionIsExactAndIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { first.Close() })
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { second.Close() })
	fixture := newPipelineJobFixture(t, first, true)
	job, _, err := first.EnqueuePipelineJob(fixture.spec)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(1_800_000_000, 0)
	leased, err := first.ClaimPipelineJob(job.Kind, "worker-a", start, time.Minute)
	if err != nil || leased == nil {
		t.Fatalf("claim = %+v, %v", leased, err)
	}
	completion := PipelineJobCompletion{
		JobID:             job.ID,
		LeaseOwner:        "worker-a",
		LeaseFence:        leased.LeaseFence,
		DesiredHeadSHA:    job.DesiredHeadSHA,
		InputDigest:       job.InputDigest,
		OwnerDecisionHead: job.OwnerDecisionHead,
		ResultDigest:      pipelineJobResult,
		OutputHeadSHA:     pipelineJobOutputHead,
		CompletedAt:       start.Add(30 * time.Second),
	}
	type completionResult struct {
		replay bool
		err    error
	}
	startCompletion := make(chan struct{})
	results := make(chan completionResult, 2)
	var wg sync.WaitGroup
	for _, database := range []*DB{first, second} {
		wg.Add(1)
		go func(database *DB) {
			defer wg.Done()
			<-startCompletion
			replay, err := database.CompletePipelineJob(completion)
			results <- completionResult{replay: replay, err: err}
		}(database)
	}
	close(startCompletion)
	wg.Wait()
	close(results)
	firstCompletions, replays := 0, 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent completion: %v", result.err)
		}
		if result.replay {
			replays++
		} else {
			firstCompletions++
		}
	}
	if firstCompletions != 1 || replays != 1 {
		t.Fatalf("completion outcomes first=%d replay=%d", firstCompletions, replays)
	}
	if _, err := first.sql.Exec(`UPDATE runs SET head_sha = ? WHERE id = ?`, pipelineJobOutputHead, fixture.run.ID); err != nil {
		t.Fatal(err)
	}
	replay, err := first.CompletePipelineJob(completion)
	if err != nil || !replay {
		t.Fatalf("duplicate completion replay=%v err=%v", replay, err)
	}
	conflicting := completion
	conflicting.ResultDigest = strings.Repeat("e", 64)
	if _, err := first.CompletePipelineJob(conflicting); err == nil {
		t.Fatal("conflicting completion replay was accepted")
	}
	var completedEvents int
	if err := first.sql.QueryRow(`SELECT COUNT(*) FROM pipeline_job_events WHERE job_id = ? AND event_type = 'completed'`, job.ID).Scan(&completedEvents); err != nil || completedEvents != 1 {
		t.Fatalf("completed event count = %d, %v", completedEvents, err)
	}
}

func TestPipelineJobTransitionsRejectWrongBindingsAndSupersedeExactly(t *testing.T) {
	database := openTestDB(t)
	fixture := newPipelineJobFixture(t, database, true)
	job, _, err := database.EnqueuePipelineJob(fixture.spec)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(1_800_000_000, 0)
	leased, err := database.ClaimPipelineJob(job.Kind, "worker-a", start, time.Minute)
	if err != nil || leased == nil {
		t.Fatalf("claim = %+v, %v", leased, err)
	}

	completion := PipelineJobCompletion{
		JobID:             job.ID,
		LeaseOwner:        "worker-a",
		LeaseFence:        leased.LeaseFence,
		DesiredHeadSHA:    job.DesiredHeadSHA,
		InputDigest:       job.InputDigest,
		OwnerDecisionHead: job.OwnerDecisionHead,
		ResultDigest:      pipelineJobResult,
		OutputHeadSHA:     pipelineJobOutputHead,
		CompletedAt:       start.Add(30 * time.Second),
	}
	mutations := []func(*PipelineJobCompletion){
		func(value *PipelineJobCompletion) { value.LeaseFence++ },
		func(value *PipelineJobCompletion) { value.DesiredHeadSHA = strings.Repeat("3", 40) },
		func(value *PipelineJobCompletion) { value.InputDigest = strings.Repeat("c", 64) },
		func(value *PipelineJobCompletion) { value.OwnerDecisionHead = strings.Repeat("d", 64) },
	}
	for _, mutate := range mutations {
		wrong := completion
		mutate(&wrong)
		if _, err := database.CompletePipelineJob(wrong); err == nil {
			t.Fatalf("wrong completion binding was accepted: %+v", wrong)
		}
	}

	replay, err := database.SupersedePipelineJob(PipelineJobSupersession{
		JobID:             job.ID,
		DesiredHeadSHA:    job.DesiredHeadSHA,
		InputDigest:       job.InputDigest,
		OwnerDecisionHead: job.OwnerDecisionHead,
		SupersededAt:      start.Add(40 * time.Second),
	})
	if err != nil || replay {
		t.Fatalf("supersede replay=%v err=%v", replay, err)
	}
	replay, err = database.SupersedePipelineJob(PipelineJobSupersession{
		JobID:             job.ID,
		DesiredHeadSHA:    job.DesiredHeadSHA,
		InputDigest:       job.InputDigest,
		OwnerDecisionHead: job.OwnerDecisionHead,
		SupersededAt:      start.Add(40 * time.Second),
	})
	if err != nil || !replay {
		t.Fatalf("duplicate supersede replay=%v err=%v", replay, err)
	}
	if _, err := database.CompletePipelineJob(completion); err == nil {
		t.Fatal("superseded lease completed")
	}
}

func TestPipelineJobOwnerDecisionHeadInvalidatesLeasedWork(t *testing.T) {
	database, run, round, _, privateKey, challenge := protectedDecisionFixture(t)
	job, _, err := database.EnqueuePipelineJob(PipelineJobSpec{
		RunID:             run.ID,
		StepResultID:      round.StepResultID,
		Kind:              PipelineJobReview,
		Round:             1,
		DesiredHeadSHA:    run.HeadSHA,
		InputDigest:       pipelineJobInput,
		OwnerDecisionHead: challenge.PreviousHead,
		MaxAttempts:       2,
	})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(challenge.IssuedAt, 0)
	leased, err := database.ClaimPipelineJob(PipelineJobReview, "worker-a", start, time.Minute)
	if err != nil || leased == nil {
		t.Fatalf("claim = %+v, %v", leased, err)
	}
	envelope, err := ownerdecision.Sign(privateKey, challenge, ownerdecision.Response{Action: types.ActionApprove})
	if err != nil {
		t.Fatal(err)
	}
	projection := &OwnerDecisionProjection{
		RoundID:            round.ID,
		SelectedFindingIDs: DeclinedSelectionJSON,
		SelectionSource:    RoundSelectionSourceUserDeclined,
	}
	if _, err := database.AppendOwnerDecision(run.ID, "respond:"+round.ID, envelope, challenge, projection, start); err != nil {
		t.Fatal(err)
	}
	active, err := database.ActivePipelineWorkerLeases(start.Add(10 * time.Second))
	if err != nil || len(active) != 0 {
		t.Fatalf("stale owner-decision lease remained active: %+v, %v", active, err)
	}
	if _, err := database.CompletePipelineJob(PipelineJobCompletion{
		JobID:             job.ID,
		LeaseOwner:        "worker-a",
		LeaseFence:        leased.LeaseFence,
		DesiredHeadSHA:    job.DesiredHeadSHA,
		InputDigest:       job.InputDigest,
		OwnerDecisionHead: job.OwnerDecisionHead,
		ResultDigest:      pipelineJobResult,
		OutputHeadSHA:     pipelineJobOutputHead,
		CompletedAt:       start.Add(20 * time.Second),
	}); err == nil {
		t.Fatal("lease under an old owner-decision head completed")
	}
}

func TestActivePipelineWorkerLeasesIgnoreStaleRunsAndCIWaits(t *testing.T) {
	database := openTestDB(t)
	for i := 0; i < 6; i++ {
		repo, err := database.InsertRepo(t.TempDir(), "https://example.invalid/stale.git", "main")
		if err != nil {
			t.Fatal(err)
		}
		run, err := database.InsertRun(repo.ID, "feature/stale-"+string(rune('a'+i)), strings.Repeat("4", 40), strings.Repeat("0", 40))
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
	}
	start := time.Unix(1_800_000_000, 0)
	active, err := database.ActivePipelineWorkerLeases(start)
	if err != nil || len(active) != 0 {
		t.Fatalf("pid-less running rows consumed worker capacity: %+v, %v", active, err)
	}

	review := newPipelineJobFixture(t, database, false)
	if err := database.UpdateRunStatus(review.run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if _, _, err := database.EnqueuePipelineJob(review.spec); err != nil {
		t.Fatal(err)
	}
	leasedReview, err := database.ClaimPipelineJob(PipelineJobReview, "review-worker", start, time.Minute)
	if err != nil || leasedReview == nil {
		t.Fatalf("review claim = %+v, %v", leasedReview, err)
	}

	ciWait := newPipelineJobFixture(t, database, false)
	ciWait.spec.Kind = PipelineJobCIMonitor
	if err := database.UpdateRunStatus(ciWait.run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if _, _, err := database.EnqueuePipelineJob(ciWait.spec); err != nil {
		t.Fatal(err)
	}
	leasedCI, err := database.ClaimPipelineJob(PipelineJobCIMonitor, "ci-reconciler", start, time.Hour)
	if err != nil || leasedCI == nil {
		t.Fatalf("CI claim = %+v, %v", leasedCI, err)
	}
	active, err = database.ActivePipelineWorkerLeases(start.Add(30 * time.Second))
	if err != nil || len(active) != 1 || active[0].ID != leasedReview.ID {
		t.Fatalf("active worker leases = %+v, %v; want only review", active, err)
	}
	active, err = database.ActivePipelineWorkerLeases(start.Add(61 * time.Second))
	if err != nil || len(active) != 0 {
		t.Fatalf("expired review lease remained active: %+v, %v", active, err)
	}
	if _, err := database.RequeueExpiredPipelineJobs(start.Add(61 * time.Second)); err != nil {
		t.Fatal(err)
	}
	storedRun, err := database.GetRun(review.run.ID)
	if err != nil || storedRun.Status != types.RunRunning || storedRun.HeadSHA != review.run.HeadSHA || storedRun.CustodyReturnedAt != nil {
		t.Fatalf("lease expiry mutated legitimate run custody: %+v, %v", storedRun, err)
	}
}

func TestPipelineJobSchemaStoresOnlyBoundedMetadata(t *testing.T) {
	database := openTestDB(t)
	allowed := map[string]map[string]bool{
		"pipeline_jobs": {
			"id": true, "run_id": true, "step_result_id": true, "kind": true,
			"round": true, "desired_head_sha": true, "input_digest": true,
			"owner_decision_head": true, "desired_generation": true, "idempotency_key": true, "status": true,
			"max_attempts": true, "attempts_started": true, "lease_fence": true,
			"lease_owner": true, "lease_expires_at": true, "heartbeat_at": true,
			"result_digest": true, "output_head_sha": true, "error_category": true,
			"superseded_at": true, "completed_at": true, "created_at": true, "updated_at": true,
		},
		"pipeline_job_events": {
			"id": true, "job_id": true, "event_type": true, "status": true,
			"attempt": true, "lease_fence": true, "lease_owner": true,
			"result_digest": true, "output_head_sha": true, "created_at": true,
		},
	}
	for table, columns := range allowed {
		rows, err := database.sql.Query(`PRAGMA table_info(` + table + `)`)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
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
				t.Fatalf("privacy-unsafe or unreviewed %s column %q", table, name)
			}
			seen[name] = true
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if len(seen) != len(columns) {
			t.Fatalf("%s columns = %v, want exact reviewed shape %v", table, seen, columns)
		}
	}
}

func TestPipelineJobRetryCeilingIsSmall(t *testing.T) {
	database := openTestDB(t)
	fixture := newPipelineJobFixture(t, database, false)
	fixture.spec.MaxAttempts = 11
	if _, _, err := database.EnqueuePipelineJob(fixture.spec); err == nil {
		t.Fatal("retry budget above hard ceiling was accepted")
	}
	fixture.spec.MaxAttempts = 10
	if _, _, err := database.EnqueuePipelineJob(fixture.spec); err != nil {
		t.Fatalf("hard ceiling was rejected: %v", err)
	}
}
