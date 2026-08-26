package db

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
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

const maxPipelineJobAttempts = 100

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
	MaxAttempts       int
}

type PipelineJobHeartbeat struct {
	JobID             string
	LeaseOwner        string
	LeaseFence        int64
	DesiredHeadSHA    string
	InputDigest       string
	OwnerDecisionHead string
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
	ResultDigest      string
	OutputHeadSHA     string
	CompletedAt       time.Time
}

type PipelineJobSupersession struct {
	JobID             string
	DesiredHeadSHA    string
	InputDigest       string
	OwnerDecisionHead string
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

const pipelineJobColumns = `id, run_id, step_result_id, kind, round, desired_head_sha, input_digest, owner_decision_head, idempotency_key, status, max_attempts, attempts_started, lease_fence, lease_owner, lease_expires_at, heartbeat_at, result_digest, output_head_sha, error_category, superseded_at, completed_at, created_at, updated_at`

func scanPipelineJob(scanner interface{ Scan(...any) error }) (*PipelineJob, error) {
	job := &PipelineJob{}
	if err := scanner.Scan(
		&job.ID, &job.RunID, &job.StepResultID, &job.Kind, &job.Round,
		&job.DesiredHeadSHA, &job.InputDigest, &job.OwnerDecisionHead,
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
		  owner_decision_head, idempotency_key, status, max_attempts, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		jobID, spec.RunID, spec.StepResultID, spec.Kind, spec.Round,
		spec.DesiredHeadSHA, spec.InputDigest, spec.OwnerDecisionHead, key,
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
		          JOIN step_results s ON s.id = j.step_result_id AND s.run_id = j.run_id
		         WHERE j.kind = ? AND j.status = ? AND j.attempts_started < j.max_attempts
		         ORDER BY j.created_at, j.id
		         LIMIT 1
		  )
		  RETURNING `+pipelineJobColumns,
		PipelineJobLeased, owner, ts+leaseSeconds, ts, ts, kind, PipelineJobQueued,
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
		    AND desired_head_sha = ? AND input_digest = ? AND owner_decision_head = ?
		    AND lease_expires_at > ?
		    AND EXISTS (SELECT 1 FROM runs r WHERE r.id = pipeline_jobs.run_id AND r.head_sha = pipeline_jobs.desired_head_sha)
		    AND EXISTS (SELECT 1 FROM step_results s WHERE s.id = pipeline_jobs.step_result_id AND s.run_id = pipeline_jobs.run_id)
		  RETURNING `+pipelineJobColumns,
		ts+leaseSeconds, ts, ts, heartbeat.JobID, PipelineJobLeased,
		heartbeat.LeaseOwner, heartbeat.LeaseFence, heartbeat.DesiredHeadSHA,
		heartbeat.InputDigest, heartbeat.OwnerDecisionHead, ts,
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
		    AND desired_head_sha = ? AND input_digest = ? AND owner_decision_head = ?
		    AND lease_expires_at > ?
		    AND EXISTS (SELECT 1 FROM runs r WHERE r.id = pipeline_jobs.run_id AND r.head_sha = pipeline_jobs.desired_head_sha)
		    AND EXISTS (SELECT 1 FROM step_results s WHERE s.id = pipeline_jobs.step_result_id AND s.run_id = pipeline_jobs.run_id)
		  RETURNING `+pipelineJobColumns,
		PipelineJobCompleted, completion.ResultDigest, completion.OutputHeadSHA, ts, ts,
		completion.JobID, PipelineJobLeased, completion.LeaseOwner, completion.LeaseFence,
		completion.DesiredHeadSHA, completion.InputDigest, completion.OwnerDecisionHead, ts,
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
		    AND desired_head_sha = ? AND input_digest = ? AND owner_decision_head = ?
		  RETURNING `+pipelineJobColumns,
		PipelineJobSuperseded, ts, ts, supersession.JobID,
		PipelineJobQueued, PipelineJobLeased, supersession.DesiredHeadSHA,
		supersession.InputDigest, supersession.OwnerDecisionHead,
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
		existing.OwnerDecisionHead == supersession.OwnerDecisionHead {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit pipeline job supersession replay: %w", err)
		}
		return true, nil
	}
	return false, errors.New("supersede pipeline job: job is terminal or exact binding changed")
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
// It deliberately ignores runs.status, daemon-updated timestamps, agent_pid,
// and CI-monitor leases. Only exact, unexpired, fenced review/repair/test
// leases consume worker capacity. Invalidated Git or owner-decision bindings
// are omitted; an unreadable/tampered owner-decision journal fails closed.
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
		    AND EXISTS (SELECT 1 FROM runs r WHERE r.id = pipeline_jobs.run_id AND r.head_sha = pipeline_jobs.desired_head_sha)
		    AND EXISTS (SELECT 1 FROM step_results s WHERE s.id = pipeline_jobs.step_result_id AND s.run_id = pipeline_jobs.run_id)
		  ORDER BY created_at, id`,
		PipelineJobLeased, at.Unix(), PipelineJobReview, PipelineJobRepair, PipelineJobTest,
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
	var runHead, stepRunID string
	if err := tx.QueryRow(
		`SELECT r.head_sha, s.run_id
		   FROM runs r JOIN step_results s ON s.id = ?
		  WHERE r.id = ?`, job.StepResultID, job.RunID,
	).Scan(&runHead, &stepRunID); err != nil {
		if err == sql.ErrNoRows {
			return errors.New("run or step binding is absent")
		}
		return fmt.Errorf("verify run and step binding: %w", err)
	}
	if stepRunID != job.RunID || runHead != job.DesiredHeadSHA {
		return errors.New("run, step, or desired head binding changed")
	}
	authority, err := getOwnerDecisionAuthorityTx(tx, job.RunID)
	if err != nil {
		return fmt.Errorf("verify owner-decision authority: %w", err)
	}
	if authority == nil {
		if job.OwnerDecisionHead != "" {
			return errors.New("unprotected run has an owner-decision head")
		}
		return nil
	}
	if job.OwnerDecisionHead == "" {
		return errors.New("protected run has no owner-decision head")
	}
	if _, err := d.verifyOwnerDecisionHistory(job.RunID, authority, job.OwnerDecisionHead, tx); err != nil {
		return fmt.Errorf("owner-decision head binding changed: %w", err)
	}
	return nil
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
		job.OwnerDecisionHead == spec.OwnerDecisionHead && job.MaxAttempts == spec.MaxAttempts
}

func pipelineJobMatchesCompletion(job *PipelineJob, completion PipelineJobCompletion) bool {
	return job != nil && job.Status == PipelineJobCompleted &&
		job.LeaseOwner != nil && *job.LeaseOwner == completion.LeaseOwner &&
		job.LeaseFence == completion.LeaseFence &&
		job.DesiredHeadSHA == completion.DesiredHeadSHA &&
		job.InputDigest == completion.InputDigest &&
		job.OwnerDecisionHead == completion.OwnerDecisionHead &&
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
	}{
		Schema:            "no-mistakes.pipeline-job/v1",
		RunID:             spec.RunID,
		StepResultID:      spec.StepResultID,
		Kind:              spec.Kind,
		Round:             spec.Round,
		DesiredHeadSHA:    spec.DesiredHeadSHA,
		InputDigest:       spec.InputDigest,
		OwnerDecisionHead: spec.OwnerDecisionHead,
	}
	payload, err := json.Marshal(identity)
	if err != nil {
		return "", fmt.Errorf("encode pipeline job semantic identity: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}
