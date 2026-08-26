package db

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/types"
)

var (
	ErrGitHubDeliveryConflict = errors.New("GitHub delivery conflicts with prior binding")
	ErrGitHubStateMismatch    = errors.New("authoritative GitHub state does not match delivery")
)

type BranchDesiredState struct {
	RepoID      string
	Branch      string
	Revision    int64
	HeadSHA     string
	InputDigest string
	UpdatedAt   int64
}

type BranchDesiredUpdate struct {
	RepoID      string
	Branch      string
	HeadSHA     string
	InputDigest string
	UpdatedAt   time.Time
}

func (d *DB) GetBranchDesiredState(repoID, branch string) (*BranchDesiredState, error) {
	var state BranchDesiredState
	err := d.sql.QueryRow(
		`SELECT repo_id, branch, revision, head_sha, input_digest, updated_at
		   FROM branch_desired_state WHERE repo_id = ? AND branch = ?`, repoID, branch,
	).Scan(&state.RepoID, &state.Branch, &state.Revision, &state.HeadSHA, &state.InputDigest, &state.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get branch desired state: %w", err)
	}
	return &state, nil
}

func (d *DB) GetWorkerDesiredState(repoID, branch string) (*BranchDesiredState, error) {
	var state BranchDesiredState
	err := d.sql.QueryRow(
		`SELECT repo_id, branch, revision, head_sha, input_digest, updated_at
		   FROM worker_desired_state WHERE repo_id = ? AND branch = ?`, repoID, branch,
	).Scan(&state.RepoID, &state.Branch, &state.Revision, &state.HeadSHA, &state.InputDigest, &state.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get worker desired state: %w", err)
	}
	return &state, nil
}

// AdvanceBranchDesiredState coalesces an exact CI replay and advances every new
// CI custody binding by one revision. Worker execution has an independent
// generation namespace in AdvanceWorkerDesiredState.
func (d *DB) AdvanceBranchDesiredState(update BranchDesiredUpdate) (BranchDesiredState, bool, int, error) {
	if strings.TrimSpace(update.RepoID) == "" || strings.TrimSpace(update.Branch) == "" ||
		!validGitHead(update.HeadSHA) || !validDigest(update.InputDigest) {
		return BranchDesiredState{}, false, 0, errors.New("advance desired state: invalid exact binding")
	}
	ts := update.UpdatedAt.Unix()
	tx, err := d.sql.Begin()
	if err != nil {
		return BranchDesiredState{}, false, 0, err
	}
	defer tx.Rollback()
	state := BranchDesiredState{}
	err = tx.QueryRow(
		`INSERT INTO branch_desired_state (repo_id, branch, revision, head_sha, input_digest, updated_at)
		 VALUES (?, ?, 1, ?, ?, ?)
		 ON CONFLICT(repo_id, branch) DO UPDATE SET
		   revision = branch_desired_state.revision + 1,
		   head_sha = excluded.head_sha, input_digest = excluded.input_digest, updated_at = excluded.updated_at
		 WHERE branch_desired_state.head_sha <> excluded.head_sha OR branch_desired_state.input_digest <> excluded.input_digest
		 RETURNING repo_id, branch, revision, head_sha, input_digest, updated_at`,
		update.RepoID, update.Branch, update.HeadSHA, update.InputDigest, ts,
	).Scan(&state.RepoID, &state.Branch, &state.Revision, &state.HeadSHA, &state.InputDigest, &state.UpdatedAt)
	replay := false
	if err == sql.ErrNoRows {
		replay = true
		err = tx.QueryRow(`SELECT repo_id, branch, revision, head_sha, input_digest, updated_at FROM branch_desired_state WHERE repo_id = ? AND branch = ?`, update.RepoID, update.Branch).
			Scan(&state.RepoID, &state.Branch, &state.Revision, &state.HeadSHA, &state.InputDigest, &state.UpdatedAt)
	}
	if err != nil {
		return BranchDesiredState{}, false, 0, fmt.Errorf("advance desired state: %w", err)
	}
	if replay {
		if err := tx.Commit(); err != nil {
			return BranchDesiredState{}, false, 0, err
		}
		return state, true, 0, nil
	}
	if _, err := tx.Exec(`UPDATE ci_waits SET status = 'closed', updated_at = ?
	 WHERE repo_id = ? AND branch = ? AND status IN ('waiting', 'ready', 'failed')
	 AND (desired_generation < ? OR head_sha <> ? OR input_digest <> ?)`,
		ts, update.RepoID, update.Branch, state.Revision, state.HeadSHA, state.InputDigest); err != nil {
		return BranchDesiredState{}, false, 0, err
	}
	if _, err := tx.Exec(`DELETE FROM ci_reconciliations WHERE wait_id IN
	 (SELECT id FROM ci_waits WHERE repo_id = ? AND branch = ? AND status = 'closed')`, update.RepoID, update.Branch); err != nil {
		return BranchDesiredState{}, false, 0, err
	}
	if err := tx.Commit(); err != nil {
		return BranchDesiredState{}, false, 0, err
	}
	return state, false, 0, nil
}

// AdvanceWorkerDesiredState advances only the exact review/repair/test
// execution generation. It supersedes obsolete worker leases without touching
// coordinator CI waits on the same branch.
func (d *DB) AdvanceWorkerDesiredState(update BranchDesiredUpdate) (BranchDesiredState, bool, int, error) {
	if strings.TrimSpace(update.RepoID) == "" || strings.TrimSpace(update.Branch) == "" ||
		!validGitHead(update.HeadSHA) || !validDigest(update.InputDigest) {
		return BranchDesiredState{}, false, 0, errors.New("advance worker desired state: invalid exact binding")
	}
	ts := update.UpdatedAt.Unix()
	tx, err := d.sql.Begin()
	if err != nil {
		return BranchDesiredState{}, false, 0, err
	}
	defer tx.Rollback()
	state := BranchDesiredState{}
	err = tx.QueryRow(
		`INSERT INTO worker_desired_state (repo_id, branch, revision, head_sha, input_digest, updated_at)
		 VALUES (?, ?, 1, ?, ?, ?)
		 ON CONFLICT(repo_id, branch) DO UPDATE SET
		   revision = worker_desired_state.revision + 1,
		   head_sha = excluded.head_sha, input_digest = excluded.input_digest, updated_at = excluded.updated_at
		 WHERE worker_desired_state.head_sha <> excluded.head_sha OR worker_desired_state.input_digest <> excluded.input_digest
		 RETURNING repo_id, branch, revision, head_sha, input_digest, updated_at`,
		update.RepoID, update.Branch, update.HeadSHA, update.InputDigest, ts,
	).Scan(&state.RepoID, &state.Branch, &state.Revision, &state.HeadSHA, &state.InputDigest, &state.UpdatedAt)
	replay := false
	if err == sql.ErrNoRows {
		replay = true
		err = tx.QueryRow(`SELECT repo_id, branch, revision, head_sha, input_digest, updated_at FROM worker_desired_state WHERE repo_id = ? AND branch = ?`, update.RepoID, update.Branch).
			Scan(&state.RepoID, &state.Branch, &state.Revision, &state.HeadSHA, &state.InputDigest, &state.UpdatedAt)
	}
	if err != nil {
		return BranchDesiredState{}, false, 0, fmt.Errorf("advance worker desired state: %w", err)
	}
	if replay {
		if err := tx.Commit(); err != nil {
			return BranchDesiredState{}, false, 0, err
		}
		return state, true, 0, nil
	}
	rows, err := tx.Query(
		`UPDATE pipeline_jobs SET status = ?, superseded_at = ?, lease_expires_at = NULL,
		 heartbeat_at = NULL, updated_at = ?
		 WHERE status IN (?, ?) AND kind IN (?, ?, ?)
		 AND run_id IN (SELECT id FROM runs WHERE repo_id = ? AND branch = ?)
		 AND (desired_generation < ? OR desired_head_sha <> ? OR input_digest <> ?)
		 RETURNING `+pipelineJobColumns,
		PipelineJobSuperseded, ts, ts, PipelineJobQueued, PipelineJobLeased,
		PipelineJobReview, PipelineJobRepair, PipelineJobTest,
		update.RepoID, update.Branch, state.Revision, state.HeadSHA, state.InputDigest,
	)
	if err != nil {
		return BranchDesiredState{}, false, 0, err
	}
	var jobs []*PipelineJob
	for rows.Next() {
		job, scanErr := scanPipelineJob(rows)
		if scanErr != nil {
			rows.Close()
			return BranchDesiredState{}, false, 0, scanErr
		}
		jobs = append(jobs, job)
	}
	if err := rows.Close(); err != nil {
		return BranchDesiredState{}, false, 0, err
	}
	for _, job := range jobs {
		if err := insertPipelineJobEventTx(tx, job, "superseded", ts); err != nil {
			return BranchDesiredState{}, false, 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return BranchDesiredState{}, false, 0, err
	}
	return state, false, len(jobs), nil
}

type GitHubDelivery struct {
	DeliveryID    string
	PayloadDigest string
	RepoID        string
	PRNumber      int64
	HeadSHA       string
	EventType     string
	ReceivedAt    time.Time
}

func (d *DB) AdmitGitHubDelivery(delivery GitHubDelivery) (bool, error) {
	if strings.TrimSpace(delivery.DeliveryID) == "" || len(delivery.DeliveryID) > 255 ||
		!validDigest(delivery.PayloadDigest) || strings.TrimSpace(delivery.RepoID) == "" ||
		delivery.PRNumber <= 0 || !validGitHead(delivery.HeadSHA) ||
		!validGitHubEventType(delivery.EventType) {
		return false, errors.New("admit GitHub delivery: invalid bounded metadata")
	}
	result, err := d.sql.Exec(
		`INSERT OR IGNORE INTO github_deliveries
		 (delivery_id, payload_digest, repo_id, pr_number, head_sha, event_type, received_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`, delivery.DeliveryID, delivery.PayloadDigest,
		delivery.RepoID, delivery.PRNumber, delivery.HeadSHA, delivery.EventType, delivery.ReceivedAt.Unix(),
	)
	if err != nil {
		return false, err
	}
	inserted, _ := result.RowsAffected()
	if inserted == 1 {
		return false, nil
	}
	var digest, repo, head, event string
	var pr int64
	if err := d.sql.QueryRow(`SELECT payload_digest, repo_id, pr_number, head_sha, event_type FROM github_deliveries WHERE delivery_id = ?`, delivery.DeliveryID).
		Scan(&digest, &repo, &pr, &head, &event); err != nil {
		return false, err
	}
	if digest != delivery.PayloadDigest || repo != delivery.RepoID || pr != delivery.PRNumber || head != delivery.HeadSHA || event != delivery.EventType {
		return false, fmt.Errorf("admit GitHub delivery: %w", ErrGitHubDeliveryConflict)
	}
	return true, nil
}

type CIWaitSpec struct {
	RunID, RepoID, Branch, HeadSHA, InputDigest string
	EvidenceLocalRoot                           string
	PRNumber, DesiredGeneration                 int64
	RegisteredAt                                time.Time
	ReconcileInterval                           time.Duration
	DeclaredNoCI                                bool
	TrustedConfigBound                          bool
}

// CIWaitInputDigest is the content-free semantic identity shared by new CI
// handoffs and restart adoption of an already-running CI step.
func CIWaitInputDigest(repoID, branch string, prNumber int64, head string) string {
	payload := fmt.Sprintf("no-mistakes.ci-wait/v1\x00%s\x00%s\x00%d\x00%s", repoID, branch, prNumber, head)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func (d *DB) RegisterCIWait(spec CIWaitSpec) (string, error) {
	if spec.RunID == "" || spec.RepoID == "" || spec.Branch == "" || spec.PRNumber <= 0 ||
		!validGitHead(spec.HeadSHA) || !validDigest(spec.InputDigest) || spec.DesiredGeneration <= 0 ||
		spec.ReconcileInterval < time.Minute || spec.ReconcileInterval > 24*time.Hour || spec.ReconcileInterval%time.Second != 0 {
		return "", errors.New("register CI wait: invalid exact binding or interval")
	}
	id, ts := newID(), spec.RegisteredAt.Unix()
	tx, err := d.sql.Begin()
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO ci_waits
	 (id, run_id, repo_id, branch, pr_number, head_sha, input_digest, desired_generation,
	  declared_no_ci, evidence_local_root, trusted_config_bound, status, check_state, next_reconcile_at, interval_seconds, created_at, updated_at)
	 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'waiting', 'unknown', ?, ?, ?, ?)`,
		id, spec.RunID, spec.RepoID, spec.Branch, spec.PRNumber, spec.HeadSHA,
		spec.InputDigest, spec.DesiredGeneration, spec.DeclaredNoCI, spec.EvidenceLocalRoot, spec.TrustedConfigBound,
		ts, int64(spec.ReconcileInterval/time.Second), ts, ts); err != nil {
		return "", err
	}
	var bound int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM runs r JOIN branch_desired_state d
	 ON d.repo_id = r.repo_id AND d.branch = r.branch
	 WHERE r.id = ? AND r.repo_id = ? AND r.branch = ? AND r.head_sha = ?
	 AND d.revision = ? AND d.head_sha = ? AND d.input_digest = ?`,
		spec.RunID, spec.RepoID, spec.Branch, spec.HeadSHA, spec.DesiredGeneration, spec.HeadSHA, spec.InputDigest).Scan(&bound); err != nil {
		return "", err
	}
	if bound != 1 {
		return "", errors.New("register CI wait: run or desired generation binding changed")
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return id, nil
}

type AuthoritativeGitHubState struct {
	RepoID, HeadSHA, CheckState, Mergeability string
	PRNumber                                  int64
}

const (
	MergeabilityUnknown   = "unknown"
	MergeabilityMergeable = "mergeable"
	MergeabilityConflict  = "conflict"
)

// ConfirmGitHubDelivery accepts only an injected authoritative refetch bound
// to the delivery's repository/PR/head. It coalesces one durable reconciliation
// request per wait; the webhook itself is never treated as truth.
func (d *DB) ConfirmGitHubDelivery(deliveryID string, state AuthoritativeGitHubState, at time.Time) (int, error) {
	tx, err := d.sql.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var repo, deliveryHead string
	var pr int64
	if err := tx.QueryRow(`SELECT repo_id, pr_number, head_sha FROM github_deliveries WHERE delivery_id = ?`, deliveryID).Scan(&repo, &pr, &deliveryHead); err != nil {
		return 0, err
	}
	if state.RepoID != repo || state.PRNumber != pr || state.HeadSHA != deliveryHead || !validGitHead(state.HeadSHA) || !validGitHubCheckState(state.CheckState) {
		return 0, fmt.Errorf("confirm GitHub delivery: %w", ErrGitHubStateMismatch)
	}
	ts := at.Unix()
	rows, err := tx.Query(`SELECT id FROM ci_waits WHERE repo_id = ? AND pr_number = ? AND head_sha = ? AND status IN ('waiting', 'ready')`, repo, pr, state.HeadSHA)
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		if _, err := tx.Exec(`UPDATE ci_waits SET check_state = ?, last_delivery_id = ?, updated_at = ? WHERE id = ?`, state.CheckState, deliveryID, ts, id); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(`INSERT INTO ci_reconciliations (wait_id, reason, delivery_id, requested_at) VALUES (?, 'delivery', ?, ?)
		 ON CONFLICT(wait_id) DO UPDATE SET reason = 'delivery', delivery_id = excluded.delivery_id, requested_at = MIN(ci_reconciliations.requested_at, excluded.requested_at)`, id, deliveryID, ts); err != nil {
			return 0, err
		}
	}
	if _, err := tx.Exec(`UPDATE github_deliveries SET confirmed_at = COALESCE(confirmed_at, ?) WHERE delivery_id = ?`, ts, deliveryID); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(ids), nil
}

type CIReconciliation struct {
	WaitID, Reason string
	DeliveryID     *string
	RequestedAt    int64
}

type CIWaitStatus string

const (
	CIWaitWaiting CIWaitStatus = "waiting"
	CIWaitReady   CIWaitStatus = "ready"
	CIWaitFailed  CIWaitStatus = "failed"
	CIWaitClosed  CIWaitStatus = "closed"
)

type CIWait struct {
	ID, RunID, RepoID, Branch, HeadSHA, InputDigest string
	EvidenceLocalRoot                               string
	PRNumber, DesiredGeneration                     int64
	Status                                          CIWaitStatus
	DeclaredNoCI                                    bool
	TrustedConfigBound                              bool
	CheckState                                      string
	NextReconcileAt, IntervalSeconds                int64
	LastDeliveryID                                  *string
	CreatedAt, UpdatedAt                            int64
}

type CIReconciliationWork struct {
	Reconciliation CIReconciliation
	Wait           CIWait
}

type CIReconciliationResult struct {
	WaitID, RepoID, Branch, HeadSHA, InputDigest string
	PRNumber, DesiredGeneration                  int64
	Status                                       CIWaitStatus
	CheckState                                   string
	DeclaredNoCI                                 bool
	FailureReason                                string
	AppliedAt                                    time.Time
}

const (
	CIFailureChecks        = "checks_failed"
	CIFailureHeadMoved     = "head_moved"
	CIFailureMergeConflict = "merge_conflict"
)

func (d *DB) ScheduleDueCIReconciliations(at time.Time, limit int) (int, error) {
	if limit < 1 || limit > 100 {
		return 0, errors.New("schedule CI reconciliation: limit must be 1..100")
	}
	tx, err := d.sql.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.Query(`SELECT id, interval_seconds FROM ci_waits WHERE status IN ('waiting', 'ready') AND next_reconcile_at <= ? ORDER BY next_reconcile_at, id LIMIT ?`, at.Unix(), limit)
	if err != nil {
		return 0, err
	}
	type due struct {
		id       string
		interval int64
	}
	var dueRows []due
	for rows.Next() {
		var item due
		if err := rows.Scan(&item.id, &item.interval); err != nil {
			rows.Close()
			return 0, err
		}
		dueRows = append(dueRows, item)
	}
	rows.Close()
	for _, item := range dueRows {
		if _, err := tx.Exec(`INSERT OR IGNORE INTO ci_reconciliations (wait_id, reason, requested_at) VALUES (?, 'periodic', ?)`, item.id, at.Unix()); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(`UPDATE ci_waits SET next_reconcile_at = ?, updated_at = ? WHERE id = ?`, at.Unix()+item.interval, at.Unix(), item.id); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(dueRows), nil
}

func (d *DB) PendingCIReconciliations(limit int) ([]CIReconciliation, error) {
	if limit < 1 || limit > 100 {
		return nil, errors.New("pending CI reconciliation: limit must be 1..100")
	}
	rows, err := d.sql.Query(`SELECT wait_id, reason, delivery_id, requested_at FROM ci_reconciliations ORDER BY requested_at, wait_id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CIReconciliation
	for rows.Next() {
		var item CIReconciliation
		if err := rows.Scan(&item.WaitID, &item.Reason, &item.DeliveryID, &item.RequestedAt); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (d *DB) PendingCIReconciliationWork(limit int) ([]CIReconciliationWork, error) {
	if limit < 1 || limit > 100 {
		return nil, errors.New("pending CI reconciliation work: limit must be 1..100")
	}
	rows, err := d.sql.Query(`SELECT r.wait_id, r.reason, r.delivery_id, r.requested_at,
	 w.id, w.run_id, w.repo_id, w.branch, w.pr_number, w.head_sha, w.input_digest,
	 w.desired_generation, w.declared_no_ci, w.evidence_local_root, w.trusted_config_bound,
	 w.status, w.check_state, w.next_reconcile_at,
	 w.interval_seconds, w.last_delivery_id, w.created_at, w.updated_at
	 FROM ci_reconciliations r JOIN ci_waits w ON w.id = r.wait_id
	 WHERE w.status IN ('waiting', 'ready') ORDER BY r.requested_at, r.wait_id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CIReconciliationWork
	for rows.Next() {
		var item CIReconciliationWork
		if err := rows.Scan(
			&item.Reconciliation.WaitID, &item.Reconciliation.Reason,
			&item.Reconciliation.DeliveryID, &item.Reconciliation.RequestedAt,
			&item.Wait.ID, &item.Wait.RunID, &item.Wait.RepoID, &item.Wait.Branch,
			&item.Wait.PRNumber, &item.Wait.HeadSHA, &item.Wait.InputDigest,
			&item.Wait.DesiredGeneration, &item.Wait.DeclaredNoCI, &item.Wait.EvidenceLocalRoot, &item.Wait.TrustedConfigBound,
			&item.Wait.Status, &item.Wait.CheckState,
			&item.Wait.NextReconcileAt, &item.Wait.IntervalSeconds,
			&item.Wait.LastDeliveryID, &item.Wait.CreatedAt, &item.Wait.UpdatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (d *DB) GetCIWait(id string) (*CIWait, error) {
	item := &CIWait{}
	err := d.sql.QueryRow(`SELECT id, run_id, repo_id, branch, pr_number, head_sha,
		 input_digest, desired_generation, declared_no_ci, evidence_local_root, trusted_config_bound, status, check_state, next_reconcile_at,
	 interval_seconds, last_delivery_id, created_at, updated_at FROM ci_waits WHERE id = ?`, id).
		Scan(&item.ID, &item.RunID, &item.RepoID, &item.Branch, &item.PRNumber,
			&item.HeadSHA, &item.InputDigest, &item.DesiredGeneration, &item.DeclaredNoCI,
			&item.EvidenceLocalRoot, &item.TrustedConfigBound, &item.Status,
			&item.CheckState, &item.NextReconcileAt, &item.IntervalSeconds,
			&item.LastDeliveryID, &item.CreatedAt, &item.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}

// GetCIWaitForRun returns the run's single durable CI wait, if registered.
func (d *DB) GetCIWaitForRun(runID string) (*CIWait, error) {
	item := &CIWait{}
	err := d.sql.QueryRow(`SELECT id, run_id, repo_id, branch, pr_number, head_sha,
	 input_digest, desired_generation, declared_no_ci, evidence_local_root, trusted_config_bound, status, check_state, next_reconcile_at,
	 interval_seconds, last_delivery_id, created_at, updated_at FROM ci_waits WHERE run_id = ?`, runID).
		Scan(&item.ID, &item.RunID, &item.RepoID, &item.Branch, &item.PRNumber,
			&item.HeadSHA, &item.InputDigest, &item.DesiredGeneration, &item.DeclaredNoCI,
			&item.EvidenceLocalRoot, &item.TrustedConfigBound, &item.Status,
			&item.CheckState, &item.NextReconcileAt, &item.IntervalSeconds,
			&item.LastDeliveryID, &item.CreatedAt, &item.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}

// BindCIWaitTrustedConfig upgrades a wait created before trusted no_ci and
// evidence cleanup settings were persisted. The marker makes this a one-time
// startup migration instead of a config refetch on every daemon restart.
func (d *DB) BindCIWaitTrustedConfig(runID string, declaredNoCI bool, evidenceLocalRoot string, at time.Time) (bool, error) {
	result, err := d.sql.Exec(`UPDATE ci_waits SET declared_no_ci = ?, evidence_local_root = ?,
		trusted_config_bound = 1, updated_at = ? WHERE run_id = ? AND trusted_config_bound = 0
		AND status IN ('waiting', 'ready')`, declaredNoCI, evidenceLocalRoot, at.Unix(), runID)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	return count == 1, err
}

// CancelCoordinatorCIWaitRun transfers custody from an obsolete durable CI
// wait to its superseding push. It succeeds only for the exact current desired
// generation and terminalizes both the CI step and run in one transaction.
func (d *DB) CancelCoordinatorCIWaitRun(runID string, at time.Time) (bool, error) {
	tx, err := d.sql.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var waitID string
	err = tx.QueryRow(`SELECT w.id FROM ci_waits w
	 JOIN runs r ON r.id = w.run_id
	 JOIN branch_desired_state desired ON desired.repo_id = w.repo_id AND desired.branch = w.branch
	 WHERE w.run_id = ? AND w.status IN ('waiting', 'ready', 'failed')
	 AND r.status IN (?, ?) AND r.repo_id = w.repo_id AND r.branch = w.branch AND r.head_sha = w.head_sha
	 AND desired.revision = w.desired_generation AND desired.head_sha = w.head_sha
	 AND desired.input_digest = w.input_digest`, runID, types.RunPending, types.RunRunning).Scan(&waitID)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	ts := at.Unix()
	result, err := tx.Exec(`UPDATE ci_waits SET status = 'closed', updated_at = ?
	 WHERE id = ? AND status IN ('waiting', 'ready', 'failed')`, ts, waitID)
	if err != nil {
		return false, err
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return false, errors.New("cancel coordinator CI wait: custody changed")
	}
	if _, err := tx.Exec(`DELETE FROM ci_reconciliations WHERE wait_id = ?`, waitID); err != nil {
		return false, err
	}
	result, err = tx.Exec(`UPDATE step_results SET status = ?, completed_at = ?, last_activity_at = ?,
	 last_activity = ?, agent_pid = NULL WHERE run_id = ? AND step_name = ?
	 AND status IN (?, ?, ?, ?)`, types.StepStatusSkipped, ts, ts, "status: skipped (superseded)",
		runID, types.StepCI, types.StepStatusRunning, types.StepStatusAwaitingApproval,
		types.StepStatusFixing, types.StepStatusFixReview)
	if err != nil {
		return false, err
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return false, errors.New("cancel coordinator CI wait: exact active CI step changed")
	}
	result, err = tx.Exec(`UPDATE runs SET status = ?, error = ?, awaiting_agent_since = NULL,
	 updated_at = ? WHERE id = ? AND status IN (?, ?)`, types.RunCancelled,
		types.RunCancelReasonSuperseded, ts, runID, types.RunPending, types.RunRunning)
	if err != nil {
		return false, err
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return false, errors.New("cancel coordinator CI wait: exact active run changed")
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// RecoverableCIWaitRunIDs returns only active runs whose waiting coordinator
// record still matches the current desired generation and one active CI step.
// The daemon uses this exact set to keep coordinator-owned waits alive across
// its generic crash recovery when the coordinator is explicitly enabled.
func (d *DB) RecoverableCIWaitRunIDs() ([]string, error) {
	rows, err := d.sql.Query(`SELECT DISTINCT w.run_id FROM ci_waits w
		JOIN runs r ON r.id = w.run_id
		JOIN branch_desired_state desired ON desired.repo_id = w.repo_id AND desired.branch = w.branch
		WHERE w.status IN (?, ?) AND r.status IN (?, ?)
		AND r.repo_id = w.repo_id AND r.branch = w.branch AND r.head_sha = w.head_sha
		AND desired.revision = w.desired_generation AND desired.head_sha = w.head_sha
		AND desired.input_digest = w.input_digest
		AND 1 = (SELECT COUNT(*) FROM step_results ci WHERE ci.run_id = w.run_id
			AND ci.step_name = ? AND ci.status IN (?, ?, ?, ?))
		ORDER BY w.run_id`,
		CIWaitWaiting, CIWaitReady, types.RunPending, types.RunRunning, types.StepCI,
		types.StepStatusRunning, types.StepStatusAwaitingApproval,
		types.StepStatusFixing, types.StepStatusFixReview)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// TerminalizeLegacyFailedCIWaitRuns upgrades failed waits produced by the
// earlier coordinator projection, which parked a fake approval gate after the
// execution goroutine had already transferred custody. Terminal failure makes
// the existing rerun action authoritative and lets startup clean its resources.
func (d *DB) TerminalizeLegacyFailedCIWaitRuns(at time.Time) (int, error) {
	tx, err := d.sql.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.Query(`SELECT w.run_id FROM ci_waits w
		JOIN runs r ON r.id = w.run_id
		WHERE w.status = ? AND r.status IN (?, ?)
		AND 1 = (SELECT COUNT(*) FROM step_results s WHERE s.run_id = w.run_id
			AND s.step_name = ? AND s.status IN (?, ?))
		ORDER BY w.run_id`, CIWaitFailed, types.RunPending, types.RunRunning,
		types.StepCI, types.StepStatusAwaitingApproval, types.StepStatusFixReview)
	if err != nil {
		return 0, err
	}
	var runIDs []string
	for rows.Next() {
		var runID string
		if err := rows.Scan(&runID); err != nil {
			rows.Close()
			return 0, err
		}
		runIDs = append(runIDs, runID)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	ts := at.Unix()
	reason := coordinatorFailureMessage(CIFailureChecks)
	for _, runID := range runIDs {
		if _, err := tx.Exec(`UPDATE step_results SET status = ?, completed_at = ?, error = ?,
			last_activity_at = ?, last_activity = ?, agent_pid = NULL
			WHERE run_id = ? AND step_name = ? AND status IN (?, ?)`,
			types.StepStatusFailed, ts, reason, ts, "status: failed", runID, types.StepCI,
			types.StepStatusAwaitingApproval, types.StepStatusFixReview); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(`UPDATE runs SET status = ?, error = ?, ci_ready_at = NULL,
			ci_ready_no_ci = 0, awaiting_agent_since = NULL, updated_at = ?
			WHERE id = ? AND status IN (?, ?)`, types.RunFailed, reason, ts, runID,
			types.RunPending, types.RunRunning); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(`DELETE FROM ci_reconciliations WHERE wait_id IN
			(SELECT id FROM ci_waits WHERE run_id = ?)`, runID); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(runIDs), nil
}

// ApplyCIReconciliation commits a terminal reducer result only while every
// exact wait and desired-generation binding is still current. The durable
// reconciliation record is deleted in the same transaction and only after
// the compare-and-swap succeeds.
func (d *DB) ApplyCIReconciliation(result CIReconciliationResult) (bool, error) {
	if strings.TrimSpace(result.WaitID) == "" || strings.TrimSpace(result.RepoID) == "" ||
		strings.TrimSpace(result.Branch) == "" || result.PRNumber <= 0 ||
		!validGitHead(result.HeadSHA) || !validDigest(result.InputDigest) ||
		result.DesiredGeneration <= 0 || !validGitHubCheckState(result.CheckState) ||
		(result.Status != CIWaitWaiting && result.Status != CIWaitReady &&
			result.Status != CIWaitFailed && result.Status != CIWaitClosed) {
		return false, errors.New("apply CI reconciliation: invalid exact result")
	}
	if result.Status == CIWaitFailed {
		switch result.FailureReason {
		case CIFailureChecks, CIFailureHeadMoved, CIFailureMergeConflict:
		default:
			return false, errors.New("apply CI reconciliation: failed result has no bounded recovery reason")
		}
	} else if result.FailureReason != "" {
		return false, errors.New("apply CI reconciliation: non-failed result has failure reason")
	}
	tx, err := d.sql.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	updated, err := tx.Exec(`UPDATE ci_waits SET status = ?, check_state = ?, updated_at = ?
	 WHERE id = ? AND repo_id = ? AND branch = ? AND pr_number = ? AND head_sha = ?
	 AND input_digest = ? AND desired_generation = ? AND status IN ('waiting', 'ready')
	 AND EXISTS (SELECT 1 FROM ci_reconciliations c WHERE c.wait_id = ci_waits.id)
	 AND EXISTS (SELECT 1 FROM branch_desired_state d
	   WHERE d.repo_id = ci_waits.repo_id AND d.branch = ci_waits.branch
	   AND d.revision = ci_waits.desired_generation AND d.head_sha = ci_waits.head_sha
	   AND d.input_digest = ci_waits.input_digest)
	 AND EXISTS (SELECT 1 FROM runs r WHERE r.id = ci_waits.run_id
	   AND r.repo_id = ci_waits.repo_id AND r.branch = ci_waits.branch
	   AND r.head_sha = ci_waits.head_sha)`,
		result.Status, result.CheckState, result.AppliedAt.Unix(), result.WaitID,
		result.RepoID, result.Branch, result.PRNumber, result.HeadSHA,
		result.InputDigest, result.DesiredGeneration)
	if err != nil {
		return false, err
	}
	count, err := updated.RowsAffected()
	if err != nil {
		return false, err
	}
	if count != 1 {
		return false, errors.New("apply CI reconciliation: stale or missing exact binding")
	}
	if err := projectCIReconciliationTx(tx, result); err != nil {
		return false, fmt.Errorf("apply CI reconciliation: %w", err)
	}
	deleted, err := tx.Exec(`DELETE FROM ci_reconciliations WHERE wait_id = ?`, result.WaitID)
	if err != nil {
		return false, err
	}
	count, err = deleted.RowsAffected()
	if err != nil {
		return false, err
	}
	if count != 1 {
		return false, errors.New("apply CI reconciliation: reconciliation custody changed")
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func projectCIReconciliationTx(tx *sql.Tx, result CIReconciliationResult) error {
	var runID string
	if err := tx.QueryRow(`SELECT run_id FROM ci_waits
		WHERE id = ? AND repo_id = ? AND branch = ? AND pr_number = ? AND head_sha = ?
		AND input_digest = ? AND desired_generation = ? AND status = ?`,
		result.WaitID, result.RepoID, result.Branch, result.PRNumber, result.HeadSHA,
		result.InputDigest, result.DesiredGeneration, result.Status).Scan(&runID); err != nil {
		return errors.New("exact wait projection binding changed")
	}
	var activeCISteps int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM step_results
		WHERE run_id = ? AND step_name = ? AND status IN (?, ?, ?, ?)`,
		runID, types.StepCI, types.StepStatusRunning, types.StepStatusAwaitingApproval,
		types.StepStatusFixing, types.StepStatusFixReview).Scan(&activeCISteps); err != nil {
		return err
	}
	if activeCISteps != 1 {
		return errors.New("exact active CI step is missing or ambiguous")
	}
	ts := result.AppliedAt.Unix()
	switch result.Status {
	case CIWaitWaiting:
		return updateExactRunCIState(tx, runID, result, nil, false)
	case CIWaitReady:
		return updateExactRunCIState(tx, runID, result, ts, false)
	case CIWaitFailed:
		reason := coordinatorFailureMessage(result.FailureReason)
		updated, err := tx.Exec(`UPDATE step_results SET status = ?, completed_at = ?, error = ?, last_activity_at = ?,
			last_activity = ?, agent_pid = NULL WHERE run_id = ? AND step_name = ? AND status IN (?, ?, ?)`,
			types.StepStatusFailed, ts, reason, ts, "status: failed", runID, types.StepCI,
			types.StepStatusRunning, types.StepStatusFixing, types.StepStatusFixReview)
		if err != nil {
			return err
		}
		if count, err := updated.RowsAffected(); err != nil || count != 1 {
			return errors.New("failed CI step projection lost custody")
		}
		updated, err = tx.Exec(`UPDATE runs SET status = ?, error = ?, ci_ready_at = NULL,
			ci_ready_no_ci = 0, awaiting_agent_since = NULL, updated_at = ?
			WHERE id = ? AND repo_id = ? AND branch = ? AND head_sha = ? AND status IN (?, ?)`,
			types.RunFailed, reason, ts, runID, result.RepoID, result.Branch, result.HeadSHA,
			types.RunPending, types.RunRunning)
		if err != nil {
			return err
		}
		if count, err := updated.RowsAffected(); err != nil || count != 1 {
			return errors.New("failed CI run projection lost custody")
		}
		return nil
	case CIWaitClosed:
		if err := finalizeTerminalPRRun(tx, runID, ts); err != nil {
			return err
		}
		var completed int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM runs r JOIN step_results s ON s.run_id = r.id
			WHERE r.id = ? AND r.repo_id = ? AND r.branch = ? AND r.head_sha = ?
			AND r.status = ? AND s.step_name = ? AND s.status = ?`,
			runID, result.RepoID, result.Branch, result.HeadSHA, types.RunCompleted,
			types.StepCI, types.StepStatusCompleted).Scan(&completed); err != nil {
			return err
		}
		if completed != 1 {
			return errors.New("closed CI projection did not finalize exact run")
		}
		return nil
	default:
		return errors.New("unsupported CI projection status")
	}
}

func coordinatorFailureMessage(reason string) string {
	switch reason {
	case CIFailureHeadMoved:
		return "coordinator stopped: the PR head moved; rerun no-mistakes against the new exact head"
	case CIFailureMergeConflict:
		return "coordinator stopped: the PR has merge conflicts; resolve them and rerun no-mistakes"
	default:
		return "coordinator stopped: CI checks failed; fix or rerun the checks, then rerun no-mistakes"
	}
}

func updateExactRunCIState(tx *sql.Tx, runID string, result CIReconciliationResult, readyAt any, awaiting bool) error {
	awaitingAt := any(nil)
	if awaiting {
		awaitingAt = result.AppliedAt.Unix()
	}
	declaredNoCI := result.Status == CIWaitReady && result.DeclaredNoCI
	updated, err := tx.Exec(`UPDATE runs SET ci_ready_at = ?, ci_ready_no_ci = ?,
		awaiting_agent_since = ?, updated_at = ? WHERE id = ? AND repo_id = ?
		AND branch = ? AND head_sha = ? AND status IN (?, ?)`,
		readyAt, declaredNoCI, awaitingAt, result.AppliedAt.Unix(), runID, result.RepoID,
		result.Branch, result.HeadSHA, types.RunPending, types.RunRunning)
	if err != nil {
		return err
	}
	if count, err := updated.RowsAffected(); err != nil || count != 1 {
		return errors.New("exact run projection binding changed")
	}
	return nil
}

type UpdaterPipelineLiveness struct {
	ActiveWorkerLeases      []*PipelineJob
	LegacyActiveRowsIgnored int
}

func (d *DB) UpdaterPipelineLiveness(at time.Time) (UpdaterPipelineLiveness, error) {
	leases, err := d.ActivePipelineWorkerLeases(at)
	if err != nil {
		return UpdaterPipelineLiveness{}, err
	}
	var legacy int
	if err := d.sql.QueryRow(`SELECT COUNT(*) FROM runs WHERE status IN ('pending', 'running')`).Scan(&legacy); err != nil {
		return UpdaterPipelineLiveness{}, err
	}
	return UpdaterPipelineLiveness{ActiveWorkerLeases: leases, LegacyActiveRowsIgnored: legacy}, nil
}

func validGitHubEventType(value string) bool {
	switch value {
	case "check_run", "check_suite", "workflow_run", "pull_request", "status":
		return true
	default:
		return false
	}
}

func validGitHubCheckState(value string) bool {
	switch value {
	case "unknown", "pending", "passed", "failed", "closed":
		return true
	default:
		return false
	}
}
