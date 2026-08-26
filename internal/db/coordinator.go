package db

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
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

// AdvanceBranchDesiredState coalesces an exact replay and advances every new
// semantic push by one revision. The same transaction supersedes obsolete
// queued/leased worker jobs, which invalidates their fences before any stale
// completion can commit.
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
	if _, err := tx.Exec(`UPDATE ci_waits SET status = 'closed', updated_at = ?
	 WHERE repo_id = ? AND branch = ? AND status = 'waiting'
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
		return false, errors.New("admit GitHub delivery: delivery ID conflicts with prior digest or binding")
	}
	return true, nil
}

type CIWaitSpec struct {
	RunID, RepoID, Branch, HeadSHA, InputDigest string
	PRNumber, DesiredGeneration                 int64
	RegisteredAt                                time.Time
	ReconcileInterval                           time.Duration
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
	  status, check_state, next_reconcile_at, interval_seconds, created_at, updated_at)
	 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'waiting', 'unknown', ?, ?, ?, ?)`,
		id, spec.RunID, spec.RepoID, spec.Branch, spec.PRNumber, spec.HeadSHA,
		spec.InputDigest, spec.DesiredGeneration, ts, int64(spec.ReconcileInterval/time.Second), ts, ts); err != nil {
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
	RepoID, HeadSHA, CheckState string
	PRNumber                    int64
}

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
		return 0, errors.New("confirm GitHub delivery: authoritative refetch does not match delivery")
	}
	ts := at.Unix()
	rows, err := tx.Query(`SELECT id FROM ci_waits WHERE repo_id = ? AND pr_number = ? AND head_sha = ? AND status = 'waiting'`, repo, pr, state.HeadSHA)
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

func (d *DB) ScheduleDueCIReconciliations(at time.Time, limit int) (int, error) {
	if limit < 1 || limit > 100 {
		return 0, errors.New("schedule CI reconciliation: limit must be 1..100")
	}
	tx, err := d.sql.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.Query(`SELECT id, interval_seconds FROM ci_waits WHERE status = 'waiting' AND next_reconcile_at <= ? ORDER BY next_reconcile_at, id LIMIT ?`, at.Unix(), limit)
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
