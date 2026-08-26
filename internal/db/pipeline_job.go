package db

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/types"
)

// PipelineJobKind identifies independently scalable execution work. It is
// intentionally narrower than pipeline step names: repair is a separate
// execution role, and CI monitoring is not an agent invocation.
type PipelineJobKind string

const (
	PipelineJobReview    PipelineJobKind = "review"
	PipelineJobRepair    PipelineJobKind = "repair"
	PipelineJobTest      PipelineJobKind = "test"
	PipelineJobCIMonitor PipelineJobKind = "ci_monitor"
)

type PipelineJobStatus string

const (
	PipelineJobQueued     PipelineJobStatus = "queued"
	PipelineJobLeased     PipelineJobStatus = "leased"
	PipelineJobCompleted  PipelineJobStatus = "completed"
	PipelineJobFailed     PipelineJobStatus = "failed"
	PipelineJobSuperseded PipelineJobStatus = "superseded"
)

const PipelineJobErrorLeaseExpired = "lease_expired"

// Infrastructure delivery retries are separate from semantic repair policy.
// Keep the hard safety ceiling small; ordinary callers should use two or three.
const maxPipelineJobAttempts = 10

// PipelineJob is bounded execution metadata. Runs and step_results remain the
// semantic source of truth; a job records only how one exact unit was executed.
type PipelineJob struct {
	ID                string
	RunID             string
	StepResultID      string
	Kind              PipelineJobKind
	Round             int
	DesiredHeadSHA    string
	InputDigest       string
	OwnerDecisionHead string
	DesiredGeneration int64
	IdempotencyKey    string
	Status            PipelineJobStatus
	MaxAttempts       int
	AttemptsStarted   int
	LeaseFence        int64
	LeaseOwner        *string
	LeaseExpiresAt    *int64
	HeartbeatAt       *int64
	ResultDigest      *string
	OutputHeadSHA     *string
	ErrorCategory     *string
	SupersededAt      *int64
	CompletedAt       *int64
	CreatedAt         int64
	UpdatedAt         int64
}

type PipelineJobSpec struct {
	RunID             string
	StepResultID      string
	Kind              PipelineJobKind
	Round             int
	DesiredHeadSHA    string
	InputDigest       string
	OwnerDecisionHead string
	DesiredGeneration int64
	MaxAttempts       int
}

type PipelineJobHeartbeat struct {
	JobID             string
	LeaseOwner        string
	LeaseFence        int64
	DesiredHeadSHA    string
	InputDigest       string
	OwnerDecisionHead string
	DesiredGeneration int64
	HeartbeatAt       time.Time
	LeaseDuration     time.Duration
}

type PipelineJobCompletion struct {
	JobID             string
	LeaseOwner        string
	LeaseFence        int64
	DesiredHeadSHA    string
	InputDigest       string
	OwnerDecisionHead string
	DesiredGeneration int64
	ResultDigest      string
	OutputHeadSHA     string
	CompletedAt       time.Time
}

// PipelineJobFailure releases one exact live lease immediately. Retryable
// infrastructure failures return to the queue only while max_attempts remains;
// malformed or stale worker results fail terminally. ErrorCategory is a
// bounded machine label, never raw command or model output.
type PipelineJobFailure struct {
	JobID             string
	LeaseOwner        string
	LeaseFence        int64
	DesiredHeadSHA    string
	InputDigest       string
	OwnerDecisionHead string
	DesiredGeneration int64
	ErrorCategory     string
	Retryable         bool
	FailedAt          time.Time
}

type PipelineJobSupersession struct {
	JobID             string
	DesiredHeadSHA    string
	InputDigest       string
	OwnerDecisionHead string
	DesiredGeneration int64
	SupersededAt      time.Time
}

type PipelineJobEvent struct {
	ID            string
	JobID         string
	EventType     string
	Status        PipelineJobStatus
	Attempt       int
	LeaseFence    int64
	LeaseOwner    *string
	ResultDigest  *string
	OutputHeadSHA *string
	CreatedAt     int64
}

const pipelineJobColumns = `id, run_id, step_result_id, kind, round, desired_head_sha, input_digest, owner_decision_head, desired_generation, idempotency_key, status, max_attempts, attempts_started, lease_fence, lease_owner, lease_expires_at, heartbeat_at, result_digest, output_head_sha, error_category, superseded_at, completed_at, created_at, updated_at`

func scanPipelineJob(scanner interface{ Scan(...any) error }) (*PipelineJob, error) {
	job := &PipelineJob{}
	if err := scanner.Scan(
		&job.ID, &job.RunID, &job.StepResultID, &job.Kind, &job.Round,
		&job.DesiredHeadSHA, &job.InputDigest, &job.OwnerDecisionHead,
		&job.DesiredGeneration,
		&job.IdempotencyKey, &job.Status, &job.MaxAttempts,
		&job.AttemptsStarted, &job.LeaseFence, &job.LeaseOwner,
		&job.LeaseExpiresAt, &job.HeartbeatAt, &job.ResultDigest,
		&job.OutputHeadSHA, &job.ErrorCategory, &job.SupersededAt,
		&job.CompletedAt, &job.CreatedAt, &job.UpdatedAt,
	); err != nil {
		return nil, err
	}
	return job, nil
}

func (d *DB) GetPipelineJob(id string) (*PipelineJob, error) {
	job, err := scanPipelineJob(d.sql.QueryRow(`SELECT `+pipelineJobColumns+` FROM pipeline_jobs WHERE id = ?`, id))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get pipeline job: %w", err)
	}
	return job, nil
}

func getPipelineJobTx(tx *sql.Tx, id string) (*PipelineJob, error) {
	job, err := scanPipelineJob(tx.QueryRow(`SELECT `+pipelineJobColumns+` FROM pipeline_jobs WHERE id = ?`, id))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return job, err
}

// EnqueuePipelineJob creates one semantic unit of execution. The returned
// replay flag is true only when the same exact bindings and retry policy were
// already present. The insert is the transaction's first statement so a
// concurrent head or owner-decision transition cannot enter between canonical
// validation and durable creation.
func (d *DB) EnqueuePipelineJob(spec PipelineJobSpec) (*PipelineJob, bool, error) {
	if err := validatePipelineJobSpec(spec); err != nil {
		return nil, false, err
	}
	key, err := pipelineJobSemanticKey(spec)
	if err != nil {
		return nil, false, err
	}
	ts := now()
	jobID := newID()
	tx, err := d.sql.Begin()
	if err != nil {
		return nil, false, fmt.Errorf("begin enqueue pipeline job: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.Exec(
		`INSERT OR IGNORE INTO pipeline_jobs
		 (id, run_id, step_result_id, kind, round, desired_head_sha, input_digest,
		  owner_decision_head, desired_generation, idempotency_key, status, max_attempts, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		jobID, spec.RunID, spec.StepResultID, spec.Kind, spec.Round,
		spec.DesiredHeadSHA, spec.InputDigest, spec.OwnerDecisionHead, spec.DesiredGeneration, key,
		PipelineJobQueued, spec.MaxAttempts, ts, ts,
	)
	if err != nil {
		return nil, false, fmt.Errorf("enqueue pipeline job: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return nil, false, fmt.Errorf("enqueue pipeline job rows affected: %w", err)
	}
	job, err := scanPipelineJob(tx.QueryRow(`SELECT `+pipelineJobColumns+` FROM pipeline_jobs WHERE idempotency_key = ?`, key))
	if err == sql.ErrNoRows {
		return nil, false, errors.New("enqueue pipeline job: semantic identity conflicts with a different idempotency key")
	}
	if err != nil {
		return nil, false, fmt.Errorf("read enqueued pipeline job: %w", err)
	}
	if !pipelineJobMatchesSpec(job, spec) {
		return nil, false, errors.New("enqueue pipeline job: semantic replay changed its bindings or retry policy")
	}
	if inserted == 1 {
		if err := d.verifyPipelineJobCanonicalBindingsTx(tx, job); err != nil {
			return nil, false, fmt.Errorf("enqueue pipeline job: %w", err)
		}
		if err := insertPipelineJobEventTx(tx, job, "created", ts); err != nil {
			return nil, false, err
		}
	} else if inserted != 0 {
		return nil, false, fmt.Errorf("enqueue pipeline job: inserted %d rows", inserted)
	}
	if err := tx.Commit(); err != nil {
		return nil, false, fmt.Errorf("commit pipeline job enqueue: %w", err)
	}
	return job, inserted == 0, nil
}

// ClaimPipelineJob atomically leases the oldest runnable job of one kind. The
// monotonic fence and attempt increment in the same UPDATE that selects the
// winner, so separate SQLite connections cannot both acquire it.
func (d *DB) ClaimPipelineJob(kind PipelineJobKind, owner string, at time.Time, leaseDuration time.Duration) (*PipelineJob, error) {
	if !validPipelineJobKind(kind) {
		return nil, fmt.Errorf("claim pipeline job: unsupported kind %q", kind)
	}
	if err := validateLeaseOwner(owner); err != nil {
		return nil, fmt.Errorf("claim pipeline job: %w", err)
	}
	leaseSeconds, err := pipelineJobLeaseSeconds(leaseDuration)
	if err != nil {
		return nil, fmt.Errorf("claim pipeline job: %w", err)
	}
	if _, err := d.RequeueExpiredPipelineJobs(at); err != nil {
		return nil, err
	}
	ts := at.Unix()
	tx, err := d.sql.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin pipeline job claim: %w", err)
	}
	defer tx.Rollback()
	job, err := scanPipelineJob(tx.QueryRow(
		`UPDATE pipeline_jobs
		    SET status = ?, attempts_started = attempts_started + 1,
		        lease_fence = lease_fence + 1, lease_owner = ?,
		        lease_expires_at = ?, heartbeat_at = ?, error_category = NULL,
		        updated_at = ?
		  WHERE id = (
		        SELECT j.id
		          FROM pipeline_jobs j
		          JOIN runs r ON r.id = j.run_id AND r.head_sha = j.desired_head_sha
		                     AND r.status IN (?, ?)
		          JOIN step_results s ON s.id = j.step_result_id AND s.run_id = j.run_id
		         WHERE j.kind = ? AND j.status = ? AND j.attempts_started < j.max_attempts
		         ORDER BY j.created_at, j.id
		         LIMIT 1
		  )
		  RETURNING `+pipelineJobColumns,
		PipelineJobLeased, owner, ts+leaseSeconds, ts, ts,
		types.RunPending, types.RunRunning, kind, PipelineJobQueued,
	))
	if err == sql.ErrNoRows {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit empty pipeline job claim: %w", err)
		}
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim pipeline job: %w", err)
	}
	if err := d.verifyPipelineJobCanonicalBindingsTx(tx, job); err != nil {
		return nil, fmt.Errorf("claim pipeline job: %w", err)
	}
	if err := insertPipelineJobEventTx(tx, job, "leased", ts); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit pipeline job claim: %w", err)
	}
	return job, nil
}

// HeartbeatPipelineJob extends only a live exact lease. An expired or stale
// fence cannot be revived, even if the reaper has not observed it yet.
func (d *DB) HeartbeatPipelineJob(heartbeat PipelineJobHeartbeat) (*PipelineJob, error) {
	if err := validatePipelineJobTransition(heartbeat.JobID, heartbeat.LeaseOwner, heartbeat.LeaseFence, heartbeat.DesiredHeadSHA, heartbeat.InputDigest, heartbeat.OwnerDecisionHead); err != nil {
		return nil, fmt.Errorf("heartbeat pipeline job: %w", err)
	}
	leaseSeconds, err := pipelineJobLeaseSeconds(heartbeat.LeaseDuration)
	if err != nil {
		return nil, fmt.Errorf("heartbeat pipeline job: %w", err)
	}
	ts := heartbeat.HeartbeatAt.Unix()
	tx, err := d.sql.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin pipeline job heartbeat: %w", err)
	}
	defer tx.Rollback()
	job, err := scanPipelineJob(tx.QueryRow(
		`UPDATE pipeline_jobs
		    SET lease_expires_at = ?, heartbeat_at = ?, updated_at = ?
		  WHERE id = ? AND status = ? AND lease_owner = ? AND lease_fence = ?
		    AND desired_head_sha = ? AND input_digest = ? AND owner_decision_head = ? AND desired_generation = ?
		    AND lease_expires_at > ?
		    AND EXISTS (SELECT 1 FROM runs r WHERE r.id = pipeline_jobs.run_id AND r.head_sha = pipeline_jobs.desired_head_sha AND r.status IN (?, ?))
		    AND EXISTS (SELECT 1 FROM step_results s WHERE s.id = pipeline_jobs.step_result_id AND s.run_id = pipeline_jobs.run_id)
		  RETURNING `+pipelineJobColumns,
		ts+leaseSeconds, ts, ts, heartbeat.JobID, PipelineJobLeased,
		heartbeat.LeaseOwner, heartbeat.LeaseFence, heartbeat.DesiredHeadSHA,
		heartbeat.InputDigest, heartbeat.OwnerDecisionHead, heartbeat.DesiredGeneration, ts,
		types.RunPending, types.RunRunning,
	))
	if err == sql.ErrNoRows {
		return nil, errors.New("heartbeat pipeline job: lease or exact binding is stale")
	}
	if err != nil {
		return nil, fmt.Errorf("heartbeat pipeline job: %w", err)
	}
	if err := d.verifyPipelineJobCanonicalBindingsTx(tx, job); err != nil {
		return nil, fmt.Errorf("heartbeat pipeline job: %w", err)
	}
	if err := insertPipelineJobEventTx(tx, job, "heartbeat", ts); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit pipeline job heartbeat: %w", err)
	}
	return job, nil
}

// RequeueExpiredPipelineJobs moves expired leases back to queued while retry
// budget remains and fails them closed once max_attempts is exhausted.
func (d *DB) RequeueExpiredPipelineJobs(at time.Time) (int, error) {
	ts := at.Unix()
	tx, err := d.sql.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin expired pipeline job requeue: %w", err)
	}
	defer tx.Rollback()
	rows, err := tx.Query(
		`UPDATE pipeline_jobs
		    SET status = CASE WHEN attempts_started >= max_attempts THEN ? ELSE ? END,
		        lease_owner = NULL, lease_expires_at = NULL, heartbeat_at = NULL,
		        error_category = CASE WHEN attempts_started >= max_attempts THEN ? ELSE NULL END,
		        updated_at = ?
		  WHERE status = ? AND lease_expires_at <= ?
		  RETURNING `+pipelineJobColumns,
		PipelineJobFailed, PipelineJobQueued, PipelineJobErrorLeaseExpired,
		ts, PipelineJobLeased, ts,
	)
	if err != nil {
		return 0, fmt.Errorf("requeue expired pipeline jobs: %w", err)
	}
	var jobs []*PipelineJob
	for rows.Next() {
		job, err := scanPipelineJob(rows)
		if err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan expired pipeline job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("scan expired pipeline jobs: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close expired pipeline jobs: %w", err)
	}
	for _, job := range jobs {
		eventType := "expired_requeued"
		if job.Status == PipelineJobFailed {
			eventType = "expired_failed"
		}
		if err := insertPipelineJobEventTx(tx, job, eventType, ts); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit expired pipeline job requeue: %w", err)
	}
	return len(jobs), nil
}

// CompletePipelineJob admits one result only under the exact live lease,
// semantic bindings, canonical run head, and owner-decision history head.
// Replaying the same result is idempotent; every conflicting replay fails.
func (d *DB) CompletePipelineJob(completion PipelineJobCompletion) (bool, error) {
	if err := validatePipelineJobTransition(completion.JobID, completion.LeaseOwner, completion.LeaseFence, completion.DesiredHeadSHA, completion.InputDigest, completion.OwnerDecisionHead); err != nil {
		return false, fmt.Errorf("complete pipeline job: %w", err)
	}
	if !validDigest(completion.ResultDigest) {
		return false, errors.New("complete pipeline job: result digest must be lowercase SHA-256")
	}
	if !validGitHead(completion.OutputHeadSHA) {
		return false, errors.New("complete pipeline job: output head must be a Git object ID")
	}
	ts := completion.CompletedAt.Unix()
	tx, err := d.sql.Begin()
	if err != nil {
		return false, fmt.Errorf("begin pipeline job completion: %w", err)
	}
	defer tx.Rollback()
	job, err := scanPipelineJob(tx.QueryRow(
		`UPDATE pipeline_jobs
		    SET status = ?, result_digest = ?, output_head_sha = ?, completed_at = ?,
		        lease_expires_at = NULL, heartbeat_at = NULL, updated_at = ?
		  WHERE id = ? AND status = ? AND lease_owner = ? AND lease_fence = ?
		    AND desired_head_sha = ? AND input_digest = ? AND owner_decision_head = ? AND desired_generation = ?
		    AND lease_expires_at > ?
		    AND EXISTS (SELECT 1 FROM runs r WHERE r.id = pipeline_jobs.run_id AND r.head_sha = pipeline_jobs.desired_head_sha AND r.status IN (?, ?))
		    AND EXISTS (SELECT 1 FROM step_results s WHERE s.id = pipeline_jobs.step_result_id AND s.run_id = pipeline_jobs.run_id)
		  RETURNING `+pipelineJobColumns,
		PipelineJobCompleted, completion.ResultDigest, completion.OutputHeadSHA, ts, ts,
		completion.JobID, PipelineJobLeased, completion.LeaseOwner, completion.LeaseFence,
		completion.DesiredHeadSHA, completion.InputDigest, completion.OwnerDecisionHead, completion.DesiredGeneration, ts,
		types.RunPending, types.RunRunning,
	))
	if err == nil {
		if err := d.verifyPipelineJobCanonicalBindingsTx(tx, job); err != nil {
			return false, fmt.Errorf("complete pipeline job: %w", err)
		}
		if err := insertPipelineJobEventTx(tx, job, "completed", ts); err != nil {
			return false, err
		}
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit pipeline job completion: %w", err)
		}
		return false, nil
	}
	if err != sql.ErrNoRows {
		return false, fmt.Errorf("complete pipeline job: %w", err)
	}
	existing, err := getPipelineJobTx(tx, completion.JobID)
	if err != nil {
		return false, fmt.Errorf("read completed pipeline job: %w", err)
	}
	if existing != nil && pipelineJobMatchesCompletion(existing, completion) {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit pipeline job completion replay: %w", err)
		}
		return true, nil
	}
	return false, errors.New("complete pipeline job: lease, result, or exact binding is stale")
}

// FailPipelineJob is the failure-side CAS paired with CompletePipelineJob.
// An exact replay is idempotent; a stale fence, changed binding, or conflicting
// classification is rejected. Retry budget is infrastructure-only and cannot
// expand the semantic repair policy.
func (d *DB) FailPipelineJob(failure PipelineJobFailure) (bool, error) {
	if err := validatePipelineJobTransition(failure.JobID, failure.LeaseOwner, failure.LeaseFence, failure.DesiredHeadSHA, failure.InputDigest, failure.OwnerDecisionHead); err != nil {
		return false, fmt.Errorf("fail pipeline job: %w", err)
	}
	if failure.DesiredGeneration < 0 {
		return false, errors.New("fail pipeline job: desired generation must be non-negative")
	}
	if !validPipelineJobErrorCategory(failure.ErrorCategory) {
		return false, errors.New("fail pipeline job: error category must be a lowercase bounded token")
	}
	ts := failure.FailedAt.Unix()
	tx, err := d.sql.Begin()
	if err != nil {
		return false, fmt.Errorf("begin pipeline job failure: %w", err)
	}
	defer tx.Rollback()
	terminalOnly := 0
	if !failure.Retryable {
		terminalOnly = 1
	}
	job, err := scanPipelineJob(tx.QueryRow(
		`UPDATE pipeline_jobs
		    SET status = CASE WHEN ? = 1 OR attempts_started >= max_attempts THEN ? ELSE ? END,
		        lease_owner = NULL, lease_expires_at = NULL, heartbeat_at = NULL,
		        error_category = ?, updated_at = ?
		  WHERE id = ? AND status = ? AND lease_owner = ? AND lease_fence = ?
		    AND desired_head_sha = ? AND input_digest = ? AND owner_decision_head = ? AND desired_generation = ?
		    AND lease_expires_at > ?
		    AND EXISTS (SELECT 1 FROM runs r WHERE r.id = pipeline_jobs.run_id AND r.head_sha = pipeline_jobs.desired_head_sha AND r.status IN (?, ?))
		    AND EXISTS (SELECT 1 FROM step_results s WHERE s.id = pipeline_jobs.step_result_id AND s.run_id = pipeline_jobs.run_id)
		  RETURNING `+pipelineJobColumns,
		terminalOnly, PipelineJobFailed, PipelineJobQueued, failure.ErrorCategory, ts,
		failure.JobID, PipelineJobLeased, failure.LeaseOwner, failure.LeaseFence,
		failure.DesiredHeadSHA, failure.InputDigest, failure.OwnerDecisionHead, failure.DesiredGeneration, ts,
		types.RunPending, types.RunRunning,
	))
	if err == nil {
		if err := d.verifyPipelineJobCanonicalBindingsTx(tx, job); err != nil {
			return false, fmt.Errorf("fail pipeline job: %w", err)
		}
		retryable := 0
		if failure.Retryable {
			retryable = 1
		}
		if _, err := tx.Exec(
			`INSERT INTO pipeline_job_attempt_failures
			 (id, job_id, attempt, lease_fence, lease_owner, error_category, retryable, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			newID(), job.ID, job.AttemptsStarted, job.LeaseFence, failure.LeaseOwner,
			failure.ErrorCategory, retryable, ts,
		); err != nil {
			return false, fmt.Errorf("insert pipeline job failure attempt: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit pipeline job failure: %w", err)
		}
		return false, nil
	}
	if err != sql.ErrNoRows {
		return false, fmt.Errorf("fail pipeline job: %w", err)
	}
	var owner, category string
	var retryable int
	err = tx.QueryRow(
		`SELECT lease_owner, error_category, retryable
		   FROM pipeline_job_attempt_failures
		  WHERE job_id = ? AND lease_fence = ?`, failure.JobID, failure.LeaseFence,
	).Scan(&owner, &category, &retryable)
	expectedRetryable := 0
	if failure.Retryable {
		expectedRetryable = 1
	}
	existing, existingErr := getPipelineJobTx(tx, failure.JobID)
	exactReplay := existingErr == nil && existing != nil &&
		existing.LeaseFence == failure.LeaseFence &&
		existing.DesiredHeadSHA == failure.DesiredHeadSHA &&
		existing.InputDigest == failure.InputDigest &&
		existing.OwnerDecisionHead == failure.OwnerDecisionHead &&
		existing.DesiredGeneration == failure.DesiredGeneration &&
		(existing.Status == PipelineJobQueued || existing.Status == PipelineJobFailed)
	if err == nil && exactReplay && owner == failure.LeaseOwner && category == failure.ErrorCategory && retryable == expectedRetryable {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit pipeline job failure replay: %w", err)
		}
		return true, nil
	}
	if err != nil && err != sql.ErrNoRows {
		return false, fmt.Errorf("read pipeline job failure replay: %w", err)
	}
	if existingErr != nil {
		return false, fmt.Errorf("read failed pipeline job replay binding: %w", existingErr)
	}
	return false, errors.New("fail pipeline job: lease, classification, or exact binding is stale")
}

// SupersedePipelineJob is the durable latest-head-wins primitive. It
// invalidates queued or leased work without pretending that a run or step has
// otherwise completed. The caller must present the old job's exact bindings.
func (d *DB) SupersedePipelineJob(supersession PipelineJobSupersession) (bool, error) {
	if strings.TrimSpace(supersession.JobID) == "" || !validGitHead(supersession.DesiredHeadSHA) ||
		!validDigest(supersession.InputDigest) || !validOwnerDecisionHead(supersession.OwnerDecisionHead) {
		return false, errors.New("supersede pipeline job: invalid exact binding")
	}
	ts := supersession.SupersededAt.Unix()
	tx, err := d.sql.Begin()
	if err != nil {
		return false, fmt.Errorf("begin pipeline job supersession: %w", err)
	}
	defer tx.Rollback()
	job, err := scanPipelineJob(tx.QueryRow(
		`UPDATE pipeline_jobs
		    SET status = ?, superseded_at = ?, lease_expires_at = NULL,
		        heartbeat_at = NULL, updated_at = ?
		  WHERE id = ? AND status IN (?, ?)
		    AND desired_head_sha = ? AND input_digest = ? AND owner_decision_head = ? AND desired_generation = ?
		  RETURNING `+pipelineJobColumns,
		PipelineJobSuperseded, ts, ts, supersession.JobID,
		PipelineJobQueued, PipelineJobLeased, supersession.DesiredHeadSHA,
		supersession.InputDigest, supersession.OwnerDecisionHead, supersession.DesiredGeneration,
	))
	if err == nil {
		if err := insertPipelineJobEventTx(tx, job, "superseded", ts); err != nil {
			return false, err
		}
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit pipeline job supersession: %w", err)
		}
		return false, nil
	}
	if err != sql.ErrNoRows {
		return false, fmt.Errorf("supersede pipeline job: %w", err)
	}
	existing, err := getPipelineJobTx(tx, supersession.JobID)
	if err != nil {
		return false, fmt.Errorf("read superseded pipeline job: %w", err)
	}
	if existing != nil && existing.Status == PipelineJobSuperseded &&
		existing.DesiredHeadSHA == supersession.DesiredHeadSHA &&
		existing.InputDigest == supersession.InputDigest &&
		existing.OwnerDecisionHead == supersession.OwnerDecisionHead &&
		existing.DesiredGeneration == supersession.DesiredGeneration {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit pipeline job supersession replay: %w", err)
		}
		return true, nil
	}
	return false, errors.New("supersede pipeline job: job is terminal or exact binding changed")
}

// supersedePipelineJobsForOwnerDecisionTx invalidates every queued or leased
// job authorized by an older owner-decision history head. It runs in the same
// transaction that appends the new signed decision, so an advanced authority
// can never leave an older job at the front of the worker queue.
func supersedePipelineJobsForOwnerDecisionTx(tx *sql.Tx, runID, currentHead string, at int64) (int, error) {
	rows, err := tx.Query(
		`UPDATE pipeline_jobs
		    SET status = ?, superseded_at = ?, lease_expires_at = NULL,
		        heartbeat_at = NULL, updated_at = ?
		  WHERE run_id = ? AND status IN (?, ?) AND owner_decision_head <> ?
		  RETURNING `+pipelineJobColumns,
		PipelineJobSuperseded, at, at, runID, PipelineJobQueued, PipelineJobLeased, currentHead,
	)
	if err != nil {
		return 0, fmt.Errorf("supersede stale owner-decision jobs: %w", err)
	}
	defer rows.Close()
	var jobs []*PipelineJob
	for rows.Next() {
		job, err := scanPipelineJob(rows)
		if err != nil {
			return 0, fmt.Errorf("scan stale owner-decision job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("scan stale owner-decision jobs: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close stale owner-decision jobs: %w", err)
	}
	for _, job := range jobs {
		if err := insertPipelineJobEventTx(tx, job, "superseded", at); err != nil {
			return 0, err
		}
	}
	return len(jobs), nil
}

func (d *DB) GetPipelineJobEvents(jobID string) ([]PipelineJobEvent, error) {
	rows, err := d.sql.Query(
		`SELECT id, job_id, event_type, status, attempt, lease_fence, lease_owner,
		        result_digest, output_head_sha, created_at
		   FROM pipeline_job_events WHERE job_id = ? ORDER BY created_at, id`, jobID,
	)
	if err != nil {
		return nil, fmt.Errorf("get pipeline job events: %w", err)
	}
	defer rows.Close()
	var events []PipelineJobEvent
	for rows.Next() {
		var event PipelineJobEvent
		if err := rows.Scan(&event.ID, &event.JobID, &event.EventType, &event.Status,
			&event.Attempt, &event.LeaseFence, &event.LeaseOwner, &event.ResultDigest,
			&event.OutputHeadSHA, &event.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan pipeline job event: %w", err)
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

// ActivePipelineWorkerLeases is the capacity and updater liveness primitive.
// It deliberately ignores daemon-updated timestamps, agent_pid, and CI-monitor
// leases. Only exact, unexpired, fenced review/repair/test leases belonging to
// a nonterminal run consume worker capacity. Invalidated Git or owner-decision
// bindings are omitted; an unreadable/tampered owner-decision journal fails
// closed.
func (d *DB) ActivePipelineWorkerLeases(at time.Time) ([]*PipelineJob, error) {
	tx, err := d.sql.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin active pipeline worker leases: %w", err)
	}
	defer tx.Rollback()
	rows, err := tx.Query(
		`SELECT `+pipelineJobColumns+`
		   FROM pipeline_jobs
		  WHERE status = ? AND lease_expires_at > ?
		    AND lease_fence > 0 AND lease_owner IS NOT NULL
		    AND kind IN (?, ?, ?)
		    AND EXISTS (SELECT 1 FROM runs r WHERE r.id = pipeline_jobs.run_id AND r.head_sha = pipeline_jobs.desired_head_sha AND r.status IN (?, ?))
		    AND EXISTS (SELECT 1 FROM step_results s WHERE s.id = pipeline_jobs.step_result_id AND s.run_id = pipeline_jobs.run_id)
		  ORDER BY created_at, id`,
		PipelineJobLeased, at.Unix(), PipelineJobReview, PipelineJobRepair, PipelineJobTest,
		types.RunPending, types.RunRunning,
	)
	if err != nil {
		return nil, fmt.Errorf("query active pipeline worker leases: %w", err)
	}
	var candidates []*PipelineJob
	for rows.Next() {
		job, err := scanPipelineJob(rows)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan active pipeline worker lease: %w", err)
		}
		candidates = append(candidates, job)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("scan active pipeline worker leases: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close active pipeline worker leases: %w", err)
	}
	active := make([]*PipelineJob, 0, len(candidates))
	for _, job := range candidates {
		current, err := d.pipelineJobOwnerHeadCurrentTx(tx, job)
		if err != nil {
			return nil, fmt.Errorf("verify active pipeline worker lease: %w", err)
		}
		if current {
			active = append(active, job)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit active pipeline worker leases: %w", err)
	}
	return active, nil
}

func insertPipelineJobEventTx(tx *sql.Tx, job *PipelineJob, eventType string, at int64) error {
	if job == nil {
		return errors.New("insert pipeline job event: job is nil")
	}
	if _, err := tx.Exec(
		`INSERT INTO pipeline_job_events
		 (id, job_id, event_type, status, attempt, lease_fence, lease_owner,
		  result_digest, output_head_sha, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		newID(), job.ID, eventType, job.Status, job.AttemptsStarted,
		job.LeaseFence, job.LeaseOwner, job.ResultDigest, job.OutputHeadSHA, at,
	); err != nil {
		return fmt.Errorf("insert pipeline job event: %w", err)
	}
	return nil
}

func (d *DB) verifyPipelineJobCanonicalBindingsTx(tx *sql.Tx, job *PipelineJob) error {
	_, err := d.verifyPipelineJobBindingsTx(tx, job, false)
	return err
}

func (d *DB) verifyPipelineJobBindingsTx(tx *sql.Tx, job *PipelineJob, allowAdoptedRepair bool) (bool, error) {
	var runHead, stepRunID, repoID, branch string
	var runStatus types.RunStatus
	if err := tx.QueryRow(
		`SELECT r.head_sha, s.run_id, r.repo_id, r.branch, r.status
		   FROM runs r JOIN step_results s ON s.id = ?
		  WHERE r.id = ?`, job.StepResultID, job.RunID,
	).Scan(&runHead, &stepRunID, &repoID, &branch, &runStatus); err != nil {
		if err == sql.ErrNoRows {
			return false, errors.New("run or step binding is absent")
		}
		return false, fmt.Errorf("verify run and step binding: %w", err)
	}
	alreadyAdopted := allowAdoptedRepair && job.Kind == PipelineJobRepair && job.Status == PipelineJobCompleted &&
		job.OutputHeadSHA != nil && runHead == *job.OutputHeadSHA
	if stepRunID != job.RunID || (runHead != job.DesiredHeadSHA && !alreadyAdopted) ||
		(runStatus != types.RunPending && runStatus != types.RunRunning) {
		return false, errors.New("run, step, or desired head binding changed")
	}
	var revision int64
	var desiredHead, inputDigest string
	err := tx.QueryRow(`SELECT revision, head_sha, input_digest FROM worker_desired_state WHERE repo_id = ? AND branch = ?`, repoID, branch).
		Scan(&revision, &desiredHead, &inputDigest)
	if err != nil && err != sql.ErrNoRows {
		return false, fmt.Errorf("verify desired generation: %w", err)
	}
	if err == sql.ErrNoRows {
		if job.DesiredGeneration == 0 {
			// Jobs created before desired generations existed remain exact through
			// their run, step, and head bindings above.
		} else {
			// One release used branch_desired_state for both CI custody and worker
			// execution. Accept that table only while the new worker namespace is
			// absent and its complete binding still matches, so an in-flight job
			// survives the first upgraded daemon restart without weakening later
			// worker generations.
			err = tx.QueryRow(`SELECT revision, head_sha, input_digest FROM branch_desired_state WHERE repo_id = ? AND branch = ?`, repoID, branch).
				Scan(&revision, &desiredHead, &inputDigest)
			if err != nil {
				if err == sql.ErrNoRows {
					return false, errors.New("job names a desired generation without worker or migration state")
				}
				return false, fmt.Errorf("verify migrated desired generation: %w", err)
			}
			if revision != job.DesiredGeneration || desiredHead != job.DesiredHeadSHA || inputDigest != job.InputDigest {
				return false, errors.New("migrated desired generation, head, or input binding changed")
			}
		}
	} else if revision != job.DesiredGeneration || desiredHead != job.DesiredHeadSHA || inputDigest != job.InputDigest {
		return false, errors.New("desired generation, head, or input binding changed")
	}
	authority, err := getOwnerDecisionAuthorityTx(tx, job.RunID)
	if err != nil {
		return false, fmt.Errorf("verify owner-decision authority: %w", err)
	}
	if authority == nil {
		if job.OwnerDecisionHead != "" {
			return false, errors.New("unprotected run has an owner-decision head")
		}
		return alreadyAdopted, nil
	}
	if job.OwnerDecisionHead == "" {
		return false, errors.New("protected run has no owner-decision head")
	}
	if _, err := d.verifyOwnerDecisionHistory(job.RunID, authority, job.OwnerDecisionHead, tx); err != nil {
		return false, fmt.Errorf("owner-decision head binding changed: %w", err)
	}
	return alreadyAdopted, nil
}

// RecoverablePipelineJobs returns the exact unconsumed worker round for each
// active review/repair/test step. Terminal jobs are included because a daemon
// can stop after Azure stored a result but before the pipeline consumed it.
func (d *DB) RecoverablePipelineJobs() ([]*PipelineJob, error) {
	tx, err := d.sql.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.Query(`SELECT `+pipelineJobColumns+` FROM pipeline_jobs
		WHERE kind IN (?, ?, ?) AND status IN (?, ?, ?, ?, ?) ORDER BY run_id, id`,
		PipelineJobReview, PipelineJobRepair, PipelineJobTest,
		PipelineJobQueued, PipelineJobLeased, PipelineJobCompleted, PipelineJobFailed, PipelineJobSuperseded)
	if err != nil {
		return nil, err
	}
	var jobs []*PipelineJob
	for rows.Next() {
		job, scanErr := scanPipelineJob(rows)
		if scanErr != nil {
			rows.Close()
			return nil, scanErr
		}
		jobs = append(jobs, job)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	var candidates []*PipelineJob
	for _, job := range jobs {
		if _, err := d.verifyPipelineJobBindingsTx(tx, job, true); err != nil {
			continue
		}
		var stepStatus types.StepStatus
		var completedRounds int
		var latestRemoteJobID sql.NullString
		if err := tx.QueryRow(`SELECT status,
			(SELECT COUNT(*) FROM step_rounds WHERE step_result_id = step_results.id),
			COALESCE((SELECT remote_job_id FROM step_rounds WHERE step_result_id = step_results.id ORDER BY round DESC LIMIT 1), '')
			FROM step_results WHERE id = ? AND run_id = ?`, job.StepResultID, job.RunID).Scan(&stepStatus, &completedRounds, &latestRemoteJobID); err != nil {
			if err == sql.ErrNoRows {
				continue
			}
			return nil, fmt.Errorf("inspect recoverable pipeline step: %w", err)
		}
		unconsumed := job.Round == completedRounds+1
		replayLatest := job.Round == completedRounds && latestRemoteJobID.Valid && latestRemoteJobID.String == job.ID
		if (stepStatus != types.StepStatusRunning && stepStatus != types.StepStatusFixing) || (!unconsumed && !replayLatest) {
			continue
		}
		candidates = append(candidates, job)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	latestRound := make(map[string]int)
	for _, job := range candidates {
		if job.Round > latestRound[job.RunID] {
			latestRound[job.RunID] = job.Round
		}
	}
	recoverable := make([]*PipelineJob, 0, len(candidates))
	for _, job := range candidates {
		if job.Round == latestRound[job.RunID] {
			recoverable = append(recoverable, job)
		}
	}
	return recoverable, nil
}

// RecoverablePipelineJobRunIDs preserves the compatibility surface used by
// startup tests while deriving liveness from exact unconsumed jobs.
func (d *DB) RecoverablePipelineJobRunIDs() ([]string, error) {
	jobs, err := d.RecoverablePipelineJobs()
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(jobs))
	for _, job := range jobs {
		seen[job.RunID] = struct{}{}
	}
	ids := make([]string, 0, len(seen))
	for runID := range seen {
		ids = append(ids, runID)
	}
	sort.Strings(ids)
	return ids, nil
}

func (d *DB) pipelineJobOwnerHeadCurrentTx(tx *sql.Tx, job *PipelineJob) (bool, error) {
	authority, err := getOwnerDecisionAuthorityTx(tx, job.RunID)
	if err != nil {
		return false, fmt.Errorf("read owner-decision authority: %w", err)
	}
	if authority == nil {
		return job.OwnerDecisionHead == "", nil
	}
	if job.OwnerDecisionHead == "" {
		return false, nil
	}
	current, err := d.verifyOwnerDecisionHistory(job.RunID, authority, "", tx)
	if err != nil {
		return false, fmt.Errorf("verify owner-decision history: %w", err)
	}
	return current == job.OwnerDecisionHead, nil
}

func pipelineJobMatchesSpec(job *PipelineJob, spec PipelineJobSpec) bool {
	return job != nil && job.RunID == spec.RunID && job.StepResultID == spec.StepResultID &&
		job.Kind == spec.Kind && job.Round == spec.Round &&
		job.DesiredHeadSHA == spec.DesiredHeadSHA && job.InputDigest == spec.InputDigest &&
		job.OwnerDecisionHead == spec.OwnerDecisionHead && job.DesiredGeneration == spec.DesiredGeneration &&
		job.MaxAttempts == spec.MaxAttempts
}

func pipelineJobMatchesCompletion(job *PipelineJob, completion PipelineJobCompletion) bool {
	return job != nil && job.Status == PipelineJobCompleted &&
		job.LeaseOwner != nil && *job.LeaseOwner == completion.LeaseOwner &&
		job.LeaseFence == completion.LeaseFence &&
		job.DesiredHeadSHA == completion.DesiredHeadSHA &&
		job.InputDigest == completion.InputDigest &&
		job.OwnerDecisionHead == completion.OwnerDecisionHead &&
		job.DesiredGeneration == completion.DesiredGeneration &&
		job.ResultDigest != nil && *job.ResultDigest == completion.ResultDigest &&
		job.OutputHeadSHA != nil && *job.OutputHeadSHA == completion.OutputHeadSHA
}

func validatePipelineJobSpec(spec PipelineJobSpec) error {
	if strings.TrimSpace(spec.RunID) == "" || strings.TrimSpace(spec.StepResultID) == "" {
		return errors.New("enqueue pipeline job: run and step IDs are required")
	}
	if !validPipelineJobKind(spec.Kind) {
		return fmt.Errorf("enqueue pipeline job: unsupported kind %q", spec.Kind)
	}
	if spec.Round < 0 {
		return errors.New("enqueue pipeline job: round must be non-negative")
	}
	if spec.DesiredGeneration < 0 {
		return errors.New("enqueue pipeline job: desired generation must be non-negative")
	}
	if !validGitHead(spec.DesiredHeadSHA) {
		return errors.New("enqueue pipeline job: desired head must be a Git object ID")
	}
	if !validDigest(spec.InputDigest) {
		return errors.New("enqueue pipeline job: input digest must be lowercase SHA-256")
	}
	if !validOwnerDecisionHead(spec.OwnerDecisionHead) {
		return errors.New("enqueue pipeline job: owner-decision head must be empty or lowercase SHA-256")
	}
	if spec.MaxAttempts < 1 || spec.MaxAttempts > maxPipelineJobAttempts {
		return fmt.Errorf("enqueue pipeline job: max attempts must be between 1 and %d", maxPipelineJobAttempts)
	}
	return nil
}

func validatePipelineJobTransition(jobID, owner string, fence int64, desiredHead, inputDigest, ownerHead string) error {
	if strings.TrimSpace(jobID) == "" {
		return errors.New("job ID is required")
	}
	if err := validateLeaseOwner(owner); err != nil {
		return err
	}
	if fence <= 0 {
		return errors.New("lease fence must be positive")
	}
	if !validGitHead(desiredHead) || !validDigest(inputDigest) || !validOwnerDecisionHead(ownerHead) {
		return errors.New("exact job binding is invalid")
	}
	return nil
}

func validateLeaseOwner(owner string) error {
	owner = strings.TrimSpace(owner)
	if owner == "" || len(owner) > 255 || strings.ContainsAny(owner, "\r\n\x00") {
		return errors.New("lease owner must be one bounded line")
	}
	return nil
}

func pipelineJobLeaseSeconds(duration time.Duration) (int64, error) {
	if duration < time.Second || duration > 24*time.Hour || duration%time.Second != 0 {
		return 0, errors.New("lease duration must be whole seconds between one second and 24 hours")
	}
	return int64(duration / time.Second), nil
}

func validPipelineJobKind(kind PipelineJobKind) bool {
	switch kind {
	case PipelineJobReview, PipelineJobRepair, PipelineJobTest, PipelineJobCIMonitor:
		return true
	default:
		return false
	}
}

func validPipelineJobErrorCategory(value string) bool {
	if len(value) < 1 || len(value) > 64 {
		return false
	}
	for _, r := range value {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}
	return true
}

func validGitHead(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	return isLowerHex(value)
}

func validDigest(value string) bool {
	return len(value) == sha256.Size*2 && isLowerHex(value)
}

func validOwnerDecisionHead(value string) bool {
	return value == "" || validDigest(value)
}

func isLowerHex(value string) bool {
	if value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func pipelineJobSemanticKey(spec PipelineJobSpec) (string, error) {
	identity := struct {
		Schema            string          `json:"schema"`
		RunID             string          `json:"run_id"`
		StepResultID      string          `json:"step_result_id"`
		Kind              PipelineJobKind `json:"kind"`
		Round             int             `json:"round"`
		DesiredHeadSHA    string          `json:"desired_head_sha"`
		InputDigest       string          `json:"input_digest"`
		OwnerDecisionHead string          `json:"owner_decision_head"`
		DesiredGeneration int64           `json:"desired_generation"`
	}{
		Schema:            "no-mistakes.pipeline-job/v1",
		RunID:             spec.RunID,
		StepResultID:      spec.StepResultID,
		Kind:              spec.Kind,
		Round:             spec.Round,
		DesiredHeadSHA:    spec.DesiredHeadSHA,
		InputDigest:       spec.InputDigest,
		OwnerDecisionHead: spec.OwnerDecisionHead,
		DesiredGeneration: spec.DesiredGeneration,
	}
	payload, err := json.Marshal(identity)
	if err != nil {
		return "", fmt.Errorf("encode pipeline job semantic identity: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}
