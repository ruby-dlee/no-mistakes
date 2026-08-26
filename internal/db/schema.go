package db

const schemaSQL = `
CREATE TABLE IF NOT EXISTS repos (
    id             TEXT PRIMARY KEY,
    working_path   TEXT NOT NULL UNIQUE,
    upstream_url   TEXT NOT NULL,
    fork_url       TEXT,
    default_branch TEXT NOT NULL DEFAULT 'main',
    created_at     INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
    id                   TEXT PRIMARY KEY,
    repo_id              TEXT NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    branch               TEXT NOT NULL,
    head_sha                TEXT NOT NULL,
    base_sha                TEXT NOT NULL,
    worktree_dir            TEXT,
    submitted_head_sha      TEXT,
    no_mistakes_version     TEXT,
    no_mistakes_build_sha   TEXT,
    review_approved_head_sha TEXT,
    status                  TEXT NOT NULL DEFAULT 'pending',
    pr_url                  TEXT,
    pr_state                TEXT,
    pr_state_observed_at    INTEGER,
    ci_ready_at             INTEGER,
    ci_ready_no_ci          INTEGER NOT NULL DEFAULT 0,
    last_pushed_sha         TEXT,
    push_target_kind        TEXT,
    push_target_fingerprint TEXT,
    push_ref                TEXT,
    last_pushed_at          INTEGER,
    push_generation         INTEGER,
    push_active             INTEGER NOT NULL DEFAULT 0,
    terminal_head_verified_at INTEGER,
    error                   TEXT,
    awaiting_agent_since INTEGER,
    parked_ms            INTEGER,
    created_at           INTEGER NOT NULL,
    updated_at           INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS step_results (
    id               TEXT PRIMARY KEY,
    run_id           TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    step_name        TEXT NOT NULL,
    step_order       INTEGER NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending',
    exit_code        INTEGER,
    duration_ms      INTEGER,
    log_path         TEXT,
    findings_json    TEXT,
    error            TEXT,
    started_at       INTEGER,
    completed_at     INTEGER,
    last_activity_at INTEGER,
    last_activity    TEXT,
    agent_pid        INTEGER,
    auto_fix_limit   INTEGER
);

CREATE TABLE IF NOT EXISTS step_rounds (
    id                   TEXT PRIMARY KEY,
    step_result_id       TEXT NOT NULL REFERENCES step_results(id) ON DELETE CASCADE,
    round                INTEGER NOT NULL,
    trigger_type         TEXT NOT NULL,
    findings_json        TEXT,
    reviewed_head_sha    TEXT,
    starting_head_sha    TEXT,
    trusted_config_sha   TEXT,
    global_config_yaml   BLOB,
    repo_config_yaml     BLOB,
    user_findings_json   TEXT,
    selected_finding_ids TEXT,
    selection_source     TEXT,
    fix_summary          TEXT,
    duration_ms          INTEGER NOT NULL,
    created_at           INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_invocations (
    id                    TEXT PRIMARY KEY,
    run_id                TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    step_name             TEXT NOT NULL,
    round                 INTEGER NOT NULL,
    purpose               TEXT NOT NULL,
    agent                 TEXT NOT NULL,
    model                 TEXT,
    requested_model       TEXT,
    served_model          TEXT,
    requested_reasoning   TEXT,
    effective_reasoning   TEXT,
    model_provider        TEXT,
    prompt_version        TEXT,
    prompt_digest         TEXT,
    no_mistakes_version   TEXT,
    no_mistakes_build_sha TEXT,
    harness_name          TEXT,
    harness_version       TEXT,
    session_mode          TEXT NOT NULL,
    session_key           TEXT,
    fallback_reason       TEXT,
    started_at            INTEGER NOT NULL,
    completed_at          INTEGER NOT NULL,
    duration_ms           INTEGER NOT NULL,
    subprocess_wait_ms    INTEGER,
    exit_status           TEXT NOT NULL,
    failure_category      TEXT,
    input_tokens          INTEGER,
    output_tokens         INTEGER,
    cache_read_tokens     INTEGER,
    cache_creation_tokens INTEGER,
    fresh_input_tokens    INTEGER,
    reasoning_tokens      INTEGER,
    delta_input_tokens    INTEGER,
    delta_output_tokens   INTEGER,
    delta_cache_read_tokens INTEGER,
    model_roundtrips      INTEGER,
    tool_calls            INTEGER,
    tool_wait_calls       INTEGER,
    tool_test_lint_calls  INTEGER,
    tool_edit_calls       INTEGER,
    tool_read_calls       INTEGER,
    tool_git_calls        INTEGER,
    tool_other_calls      INTEGER,
    workload_files        INTEGER,
    workload_lines        INTEGER,
    finding_count         INTEGER
);

CREATE INDEX IF NOT EXISTS idx_agent_invocations_run_started_id
    ON agent_invocations (run_id, started_at, id);

-- Local-only, append-only quality labels. A later observation supersedes an
-- earlier row by reference; it never edits the evidence that was observed at
-- the time. Digests and bounded provenance labels identify evidence without
-- retaining prompt, output, diff, or transcript bytes.
CREATE TABLE IF NOT EXISTS quality_outcomes (
    id                  TEXT PRIMARY KEY,
    run_id              TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    job_id              TEXT,
    fix_attempt_id      TEXT,
    root_id             TEXT,
    classification      TEXT NOT NULL CHECK (classification IN (
        'clean_fix', 'same_root_followup', 'introduced_regression',
        'overridden', 'reverted', 'primary_handoff'
    )),
    fixed_head_sha      TEXT NOT NULL,
    observed_head_sha   TEXT NOT NULL,
    evidence_digest     TEXT NOT NULL,
    evidence_provenance TEXT NOT NULL,
    supersedes_id       TEXT REFERENCES quality_outcomes(id) ON DELETE CASCADE,
    created_at          INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_quality_outcomes_run_created_id
    ON quality_outcomes (run_id, created_at, id);

CREATE TRIGGER IF NOT EXISTS quality_outcomes_no_update
BEFORE UPDATE ON quality_outcomes
BEGIN
    SELECT RAISE(ABORT, 'quality outcomes are append-only');
END;

CREATE TRIGGER IF NOT EXISTS quality_outcomes_no_delete
BEFORE DELETE ON quality_outcomes
WHEN EXISTS (SELECT 1 FROM runs WHERE id = OLD.run_id)
BEGIN
    SELECT RAISE(ABORT, 'quality outcomes are append-only');
END;

CREATE TABLE IF NOT EXISTS run_agent_sessions (
    run_id     TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    role       TEXT NOT NULL,
    agent      TEXT NOT NULL,
    session_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (run_id, role)
);

CREATE TABLE IF NOT EXISTS intent_cache (
    cache_key   TEXT PRIMARY KEY,
    summary     TEXT NOT NULL,
    agent_name  TEXT NOT NULL,
    session_id  TEXT NOT NULL,
    created_at  INTEGER NOT NULL
);

-- Per-branch range of pipeline-authored commits whose re-review did not
-- complete. The next run's initial review reads this so it is not cold on
-- uncertified fixer commits. PRIMARY KEY per branch: the latest uncertified
-- HEAD replaces an older range.
CREATE TABLE IF NOT EXISTS uncertified_pipeline_ranges (
    repo_id       TEXT NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    branch        TEXT NOT NULL,
    from_sha      TEXT NOT NULL,
    to_sha        TEXT NOT NULL,
    source_run_id TEXT NOT NULL,
    created_at    INTEGER NOT NULL,
    PRIMARY KEY (repo_id, branch)
);

-- Protected runs bind every owner decision to an immutable Ed25519 public
-- key. Historical runs have no row and retain the legacy protocol.
CREATE TABLE IF NOT EXISTS owner_decision_authorities (
    run_id           TEXT PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE,
    public_key       BLOB NOT NULL,
    key_id           TEXT NOT NULL,
    repo_id          TEXT NOT NULL,
    branch           TEXT NOT NULL,
    initial_head_sha TEXT NOT NULL,
    genesis_head     TEXT NOT NULL,
    created_at       INTEGER NOT NULL
);

-- Append-only signed authorization journal. The externally retained history
-- head chains signed envelopes; this local record digest also covers the exact
-- deterministic round projection so direct edits are detected and refused.
CREATE TABLE IF NOT EXISTS owner_decision_events (
    run_id                    TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    sequence                  INTEGER NOT NULL,
    gate_id                   TEXT NOT NULL,
    previous_head             TEXT NOT NULL,
    record_digest             TEXT NOT NULL,
    history_head              TEXT NOT NULL,
    envelope_json             TEXT NOT NULL,
    projection_round_id       TEXT,
    selected_finding_ids      TEXT,
    selection_source          TEXT,
    user_findings_json        TEXT,
    created_at                INTEGER NOT NULL,
    PRIMARY KEY (run_id, sequence),
    UNIQUE (run_id, gate_id)
);

CREATE INDEX IF NOT EXISTS idx_owner_decision_events_run_gate
    ON owner_decision_events (run_id, gate_id);

-- Pipeline jobs are execution records, never a second source of truth for a
-- run or step. Their semantic key binds one exact unit of work to the run,
-- step, round, Git head, input digest, and externally retained owner-decision
-- head that authorized it. Empty owner_decision_head means the run is
-- unprotected; protected runs require their exact verified journal head.
CREATE TABLE IF NOT EXISTS pipeline_jobs (
    id                    TEXT PRIMARY KEY,
    run_id                TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    step_result_id        TEXT NOT NULL REFERENCES step_results(id) ON DELETE CASCADE,
    kind                  TEXT NOT NULL CHECK (kind IN ('review', 'repair', 'test', 'ci_monitor')),
    round                 INTEGER NOT NULL CHECK (round >= 0),
    desired_head_sha      TEXT NOT NULL,
    input_digest          TEXT NOT NULL,
    owner_decision_head   TEXT NOT NULL DEFAULT '',
    desired_generation    INTEGER NOT NULL DEFAULT 0 CHECK (desired_generation >= 0),
    idempotency_key       TEXT NOT NULL UNIQUE,
    status                TEXT NOT NULL CHECK (status IN ('queued', 'leased', 'completed', 'failed', 'superseded')),
    max_attempts          INTEGER NOT NULL CHECK (max_attempts > 0),
    attempts_started      INTEGER NOT NULL DEFAULT 0 CHECK (attempts_started >= 0),
    lease_fence           INTEGER NOT NULL DEFAULT 0 CHECK (lease_fence >= 0),
    lease_owner           TEXT,
    lease_expires_at      INTEGER,
    heartbeat_at          INTEGER,
    result_digest         TEXT,
    output_head_sha       TEXT,
    error_category        TEXT,
    superseded_at         INTEGER,
    completed_at          INTEGER,
    created_at            INTEGER NOT NULL,
    updated_at            INTEGER NOT NULL,
    UNIQUE (run_id, step_result_id, kind, round, desired_head_sha, input_digest, owner_decision_head, desired_generation)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_claim
    ON pipeline_jobs (kind, status, created_at, id);

-- The event stream is append-only through the DB API. It records only bounded
-- execution metadata and digests: never prompts, model output, diffs, command
-- arguments, or result payloads.
CREATE TABLE IF NOT EXISTS pipeline_job_events (
    id               TEXT PRIMARY KEY,
    job_id           TEXT NOT NULL REFERENCES pipeline_jobs(id) ON DELETE CASCADE,
    event_type       TEXT NOT NULL CHECK (event_type IN ('created', 'leased', 'heartbeat', 'expired_requeued', 'expired_failed', 'completed', 'superseded')),
    status           TEXT NOT NULL,
    attempt          INTEGER NOT NULL CHECK (attempt >= 0),
    lease_fence      INTEGER NOT NULL CHECK (lease_fence >= 0),
    lease_owner      TEXT,
    result_digest    TEXT,
    output_head_sha  TEXT,
    created_at       INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pipeline_job_events_job_created
    ON pipeline_job_events (job_id, created_at, id);

-- Explicit worker failures are a separate append-only attempt stream so an
-- immediate bounded retry is distinguishable from lease expiry. Categories
-- are bounded machine labels only; raw process output never enters SQLite.
CREATE TABLE IF NOT EXISTS pipeline_job_attempt_failures (
    id                TEXT PRIMARY KEY,
    job_id            TEXT NOT NULL REFERENCES pipeline_jobs(id) ON DELETE CASCADE,
    attempt           INTEGER NOT NULL CHECK (attempt > 0),
    lease_fence       INTEGER NOT NULL CHECK (lease_fence > 0),
    lease_owner       TEXT NOT NULL,
    error_category    TEXT NOT NULL,
    retryable         INTEGER NOT NULL CHECK (retryable IN (0, 1)),
    created_at        INTEGER NOT NULL,
    UNIQUE (job_id, lease_fence)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_job_attempt_failures_job
    ON pipeline_job_attempt_failures (job_id, attempt);

CREATE TABLE IF NOT EXISTS branch_desired_state (
    repo_id         TEXT NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    branch          TEXT NOT NULL,
    revision        INTEGER NOT NULL CHECK (revision > 0),
    head_sha        TEXT NOT NULL,
    input_digest    TEXT NOT NULL,
    updated_at      INTEGER NOT NULL,
    PRIMARY KEY (repo_id, branch)
);

-- Worker execution and CI custody advance independently. Keeping their
-- generations in separate tables prevents a CI wait from invalidating an
-- exact review/test lease (and vice versa) on the same branch.
CREATE TABLE IF NOT EXISTS worker_desired_state (
    repo_id         TEXT NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    branch          TEXT NOT NULL,
    revision        INTEGER NOT NULL CHECK (revision > 0),
    head_sha        TEXT NOT NULL,
    input_digest    TEXT NOT NULL,
    updated_at      INTEGER NOT NULL,
    PRIMARY KEY (repo_id, branch)
);

CREATE TABLE IF NOT EXISTS github_deliveries (
    delivery_id     TEXT PRIMARY KEY,
    payload_digest  TEXT NOT NULL,
    repo_id         TEXT NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    pr_number       INTEGER NOT NULL CHECK (pr_number > 0),
    head_sha        TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    received_at     INTEGER NOT NULL,
    confirmed_at    INTEGER
);

CREATE TABLE IF NOT EXISTS ci_waits (
    id                  TEXT PRIMARY KEY,
    run_id              TEXT NOT NULL UNIQUE REFERENCES runs(id) ON DELETE CASCADE,
    repo_id             TEXT NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    branch              TEXT NOT NULL,
    pr_number           INTEGER NOT NULL CHECK (pr_number > 0),
    head_sha            TEXT NOT NULL,
    input_digest        TEXT NOT NULL,
    desired_generation  INTEGER NOT NULL CHECK (desired_generation > 0),
    declared_no_ci      INTEGER NOT NULL DEFAULT 0 CHECK (declared_no_ci IN (0, 1)),
    evidence_local_root TEXT NOT NULL DEFAULT '',
    trusted_config_bound INTEGER NOT NULL DEFAULT 0 CHECK (trusted_config_bound IN (0, 1)),
    status              TEXT NOT NULL CHECK (status IN ('waiting', 'ready', 'failed', 'closed')),
    check_state         TEXT NOT NULL,
    next_reconcile_at   INTEGER NOT NULL,
    interval_seconds    INTEGER NOT NULL CHECK (interval_seconds > 0),
    last_delivery_id    TEXT,
    created_at          INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ci_waits_due
    ON ci_waits (status, next_reconcile_at, id);

CREATE TABLE IF NOT EXISTS ci_reconciliations (
    wait_id          TEXT PRIMARY KEY REFERENCES ci_waits(id) ON DELETE CASCADE,
    reason           TEXT NOT NULL CHECK (reason IN ('delivery', 'periodic')),
    delivery_id      TEXT,
    requested_at     INTEGER NOT NULL
);
`

// migrationStatements hold additive schema changes applied to databases that
// were created before the referenced columns existed. Each statement must be
// idempotent via its error being tolerated when the column already exists.
var migrationStatements = []string{
	`ALTER TABLE pipeline_jobs ADD COLUMN desired_generation INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE ci_waits ADD COLUMN declared_no_ci INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE ci_waits ADD COLUMN evidence_local_root TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE ci_waits ADD COLUMN trusted_config_bound INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE repos ADD COLUMN fork_url TEXT`,
	`ALTER TABLE owner_decision_authorities ADD COLUMN repo_id TEXT`,
	`ALTER TABLE owner_decision_authorities ADD COLUMN branch TEXT`,
	`ALTER TABLE owner_decision_authorities ADD COLUMN initial_head_sha TEXT`,
	`ALTER TABLE step_rounds ADD COLUMN selected_finding_ids TEXT`,
	`ALTER TABLE step_rounds ADD COLUMN selection_source TEXT`,
	`ALTER TABLE step_rounds ADD COLUMN fix_summary TEXT`,
	`ALTER TABLE step_rounds ADD COLUMN user_findings_json TEXT`,
	// A parked round may retain the reviewed commit as a non-authoritative
	// candidate. Only atomic review completion promotes it onto the run.
	`ALTER TABLE step_rounds ADD COLUMN reviewed_head_sha TEXT`,
	`ALTER TABLE step_rounds ADD COLUMN starting_head_sha TEXT`,
	`ALTER TABLE step_rounds ADD COLUMN trusted_config_sha TEXT`,
	`ALTER TABLE step_rounds ADD COLUMN global_config_yaml BLOB`,
	`ALTER TABLE step_rounds ADD COLUMN repo_config_yaml BLOB`,
	`ALTER TABLE runs ADD COLUMN intent TEXT`,
	`ALTER TABLE runs ADD COLUMN intent_source TEXT`,
	`ALTER TABLE runs ADD COLUMN intent_session_id TEXT`,
	`ALTER TABLE runs ADD COLUMN intent_score REAL`,
	`ALTER TABLE runs ADD COLUMN awaiting_agent_since INTEGER`,
	`ALTER TABLE runs ADD COLUMN parked_ms INTEGER`,
	// The CI step's per-check rerun budget. It is durable because a run
	// recovered after a daemon restart would otherwise get a fresh budget and
	// could issue reruns beyond the documented limit; the reservation is
	// written before the provider call, so a crash mid-request spends the
	// budget rather than silently granting a free retry.
	`ALTER TABLE runs ADD COLUMN ci_rerun_state TEXT`,
	// Branch synchronization provenance is intentionally nullable. Historical
	// rows stay unbound because mutable head_sha cannot prove a successful push.
	`ALTER TABLE runs ADD COLUMN submitted_head_sha TEXT`,
	// The directory this run's worktree was created in. It is durable because
	// placement comes from operator configuration (worktree_roots) that may be
	// edited while a run exists: recording it makes such an edit inert for
	// runs already in flight instead of retargeting their resume, diff, and
	// cleanup at a directory they were never created in. Nullable for rows
	// written before the column existed, which resolve to the default
	// <NM_HOME>/worktrees placement at read time - the only one they can have,
	// since this column shipped with the setting that moves it
	// (worktrees.RecordedDir).
	`ALTER TABLE runs ADD COLUMN worktree_dir TEXT`,
	// Build identity is nullable for historical records. New runs record the
	// version and embedded build SHA used by the running binary.
	`ALTER TABLE runs ADD COLUMN no_mistakes_version TEXT`,
	`ALTER TABLE runs ADD COLUMN no_mistakes_build_sha TEXT`,
	// Review authority is nullable and never backfilled. A historical mutable
	// head_sha cannot prove which exact commit a completed review approved.
	`ALTER TABLE runs ADD COLUMN review_approved_head_sha TEXT`,
	`ALTER TABLE runs ADD COLUMN last_pushed_sha TEXT`,
	`ALTER TABLE runs ADD COLUMN push_target_kind TEXT`,
	`ALTER TABLE runs ADD COLUMN push_target_fingerprint TEXT`,
	`ALTER TABLE runs ADD COLUMN push_ref TEXT`,
	`ALTER TABLE runs ADD COLUMN last_pushed_at INTEGER`,
	`ALTER TABLE runs ADD COLUMN push_generation INTEGER`,
	`ALTER TABLE runs ADD COLUMN push_active INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE runs ADD COLUMN terminal_head_verified_at INTEGER`,
	`ALTER TABLE runs ADD COLUMN pr_state TEXT`,
	`ALTER TABLE runs ADD COLUMN pr_state_observed_at INTEGER`,
	`ALTER TABLE runs ADD COLUMN ci_ready_at INTEGER`,
	`ALTER TABLE runs ADD COLUMN ci_ready_no_ci INTEGER NOT NULL DEFAULT 0`,
	// Custody return is nullable: NULL means the pipeline still owns any
	// unpublished head this run produced; a timestamp means an explicit
	// guarded recovery ended that ownership (internal/branchsync).
	`ALTER TABLE runs ADD COLUMN custody_returned_at INTEGER`,
	`ALTER TABLE step_results ADD COLUMN last_activity_at INTEGER`,
	`ALTER TABLE step_results ADD COLUMN last_activity TEXT`,
	`ALTER TABLE step_results ADD COLUMN agent_pid INTEGER`,
	`ALTER TABLE step_results ADD COLUMN auto_fix_limit INTEGER`,
	// Session-fidelity telemetry columns (all nullable so pre-existing rows read
	// back as unknown, never a fabricated zero).
	`ALTER TABLE agent_invocations ADD COLUMN model_provider TEXT`,
	`ALTER TABLE agent_invocations ADD COLUMN fallback_reason TEXT`,
	`ALTER TABLE agent_invocations ADD COLUMN subprocess_wait_ms INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN fresh_input_tokens INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN reasoning_tokens INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN delta_input_tokens INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN delta_output_tokens INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN delta_cache_read_tokens INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN model_roundtrips INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN tool_calls INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN tool_wait_calls INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN tool_test_lint_calls INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN tool_edit_calls INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN tool_read_calls INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN tool_git_calls INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN tool_other_calls INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN workload_files INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN workload_lines INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN finding_count INTEGER`,
	// Quality identity is local-only and content-free. Every new field remains
	// nullable so historical rows and harnesses that cannot report a datum stay
	// unknown rather than acquiring a fabricated default.
	`ALTER TABLE agent_invocations ADD COLUMN requested_model TEXT`,
	`ALTER TABLE agent_invocations ADD COLUMN served_model TEXT`,
	`ALTER TABLE agent_invocations ADD COLUMN requested_reasoning TEXT`,
	`ALTER TABLE agent_invocations ADD COLUMN effective_reasoning TEXT`,
	`ALTER TABLE agent_invocations ADD COLUMN prompt_version TEXT`,
	`ALTER TABLE agent_invocations ADD COLUMN prompt_digest TEXT`,
	`ALTER TABLE agent_invocations ADD COLUMN no_mistakes_version TEXT`,
	`ALTER TABLE agent_invocations ADD COLUMN no_mistakes_build_sha TEXT`,
	`ALTER TABLE agent_invocations ADD COLUMN harness_name TEXT`,
	`ALTER TABLE agent_invocations ADD COLUMN harness_version TEXT`,
}
