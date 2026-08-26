// Package workertransport executes exact-bound pipeline jobs through a trusted
// Firstmate-owned wrapper. It never knows Azure credentials, Pi accounts, or
// Firstmate assignment internals and never calls a cloud API directly.
package workertransport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
	gitx "github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/shellenv"
)

const (
	RequestSchema       = "no-mistakes.firstmate-worker-request/v1"
	ResultSchema        = "no-mistakes.firstmate-worker-result/v1"
	maxInputBytes       = 1 << 20
	maxResultBytes      = 1 << 20
	maxStepOutcomeBytes = 1 << 20
)

var ErrDisabled = errors.New("Azure worker transport is disabled")

// Request is the closed controller-to-wrapper envelope. The wrapper must bind
// the same values into its Firstmate assignment and fm-worker-lifecycle request.
// PayloadDir contains exactly repo.bundle and brief.md. GuestArgv names the
// no-mistakes worker role; the wrapper stages a compatible binary/runtime.
type Request struct {
	Schema                  string             `json:"schema"`
	JobID                   string             `json:"job_id"`
	RunID                   string             `json:"run_id"`
	StepResultID            string             `json:"step_result_id"`
	Kind                    db.PipelineJobKind `json:"kind"`
	Round                   int                `json:"round"`
	DesiredHeadSHA          string             `json:"desired_head_sha"`
	InputDigest             string             `json:"input_digest"`
	OwnerDecisionHead       string             `json:"owner_decision_head"`
	DesiredGeneration       int64              `json:"desired_generation"`
	Attempt                 int                `json:"attempt"`
	LeaseFence              int64              `json:"lease_fence"`
	LeaseOwner              string             `json:"lease_owner"`
	SourceRef               string             `json:"source_ref"`
	SourceBundleSHA256      string             `json:"source_bundle_sha256"`
	SourceBundleSize        int64              `json:"source_bundle_size"`
	GuestArgv               []string           `json:"guest_argv"`
	ExpectedResultSchema    string             `json:"expected_result_schema"`
	ExpectedFirstmateReturn string             `json:"expected_firstmate_return"`
}

// ResultEnvelope is the closed wrapper-to-controller result. It contains only
// exact identities and bounded outcome metadata. Review prose, prompts, diffs,
// command output, credentials, and raw model output are forbidden here.
type ResultEnvelope struct {
	Schema             string             `json:"schema"`
	JobID              string             `json:"job_id"`
	RunID              string             `json:"run_id"`
	StepResultID       string             `json:"step_result_id"`
	Kind               db.PipelineJobKind `json:"kind"`
	Round              int                `json:"round"`
	DesiredHeadSHA     string             `json:"desired_head_sha"`
	InputDigest        string             `json:"input_digest"`
	OwnerDecisionHead  string             `json:"owner_decision_head"`
	DesiredGeneration  int64              `json:"desired_generation"`
	Attempt            int                `json:"attempt"`
	LeaseFence         int64              `json:"lease_fence"`
	LeaseOwner         string             `json:"lease_owner"`
	SourceBundleSHA256 string             `json:"source_bundle_sha256"`
	Outcome            string             `json:"outcome"`
	OutputHeadSHA      string             `json:"output_head_sha"`
	ReturnRef          string             `json:"return_ref,omitempty"`
	ReturnBundleSHA256 string             `json:"return_bundle_sha256,omitempty"`
	StepOutcomeSHA256  string             `json:"step_outcome_sha256,omitempty"`
	ErrorCategory      string             `json:"error_category,omitempty"`
	Retryable          bool               `json:"retryable,omitempty"`
}

type Execution struct {
	JobID          string
	OutputHeadSHA  string
	ResultDigest   string
	ReturnedBranch string
	StepOutcome    StepOutcomeEnvelope
}

type InputProvider interface {
	InputFor(context.Context, *db.PipelineJob) ([]byte, error)
}

type InputProviderFunc func(context.Context, *db.PipelineJob) ([]byte, error)

func (f InputProviderFunc) InputFor(ctx context.Context, job *db.PipelineJob) ([]byte, error) {
	return f(ctx, job)
}

type Service struct {
	database *db.DB
	cfg      config.AzureWorkerConfig
	workRoot string
	owner    string
	input    InputProvider
}

func New(database *db.DB, cfg config.AzureWorkerConfig, workRoot, owner string, input InputProvider) (*Service, error) {
	if database == nil || input == nil {
		return nil, errors.New("Azure worker transport requires a database and input provider")
	}
	if !cfg.Enabled {
		return &Service{database: database, cfg: cfg, workRoot: workRoot, owner: owner, input: input}, nil
	}
	if cfg.LeaseDuration < time.Second || cfg.LeaseDuration > 24*time.Hour || cfg.LeaseDuration%time.Second != 0 {
		return nil, errors.New("Azure worker lease duration must be whole seconds between one second and 24 hours")
	}
	if cfg.HeartbeatInterval <= 0 || cfg.HeartbeatInterval >= cfg.LeaseDuration/2 {
		return nil, errors.New("Azure worker heartbeat interval must be positive and less than half the lease duration")
	}
	if cfg.Timeout <= 0 || cfg.Timeout > 24*time.Hour {
		return nil, errors.New("Azure worker timeout must be positive and at most 24 hours")
	}
	if err := validateTrustedFile(cfg.RunnerPath, true, false); err != nil {
		return nil, fmt.Errorf("validate Firstmate wrapper: %w", err)
	}
	if err := validateTrustedFile(cfg.ConfigPath, false, true); err != nil {
		return nil, fmt.Errorf("validate Firstmate wrapper config: %w", err)
	}
	if !filepath.IsAbs(workRoot) || strings.TrimSpace(owner) == "" || strings.ContainsAny(owner, "\r\n\x00") {
		return nil, errors.New("Azure worker transport requires an absolute work root and bounded lease owner")
	}
	return &Service{database: database, cfg: cfg, workRoot: filepath.Clean(workRoot), owner: owner, input: input}, nil
}

func (s *Service) Enabled() bool { return s != nil && s.cfg.Enabled }

// ProcessOne claims and executes at most one job. A nil execution means the
// queue had no job of kind. The existing local pipeline remains the caller's
// default whenever Enabled reports false.
func (s *Service) ProcessOne(ctx context.Context, kind db.PipelineJobKind) (*Execution, error) {
	if !s.Enabled() {
		return nil, ErrDisabled
	}
	if kind != db.PipelineJobReview && kind != db.PipelineJobRepair && kind != db.PipelineJobTest {
		return nil, fmt.Errorf("Azure model-worker transport does not execute job kind %q", kind)
	}
	claimedAt := time.Now()
	job, err := s.database.ClaimPipelineJob(kind, s.owner, claimedAt, s.cfg.LeaseDuration)
	if err != nil || job == nil {
		return nil, err
	}
	workCtx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()
	heartbeats := s.startHeartbeats(workCtx, cancel, job)

	prepared, runErr := s.executeClaimed(workCtx, job)
	heartbeatErr := heartbeats.stop()
	if heartbeatErr != nil {
		if prepared != nil {
			prepared.rollback()
		}
		return nil, fmt.Errorf("Azure worker heartbeat lost: %w", heartbeatErr)
	}
	if runErr != nil {
		failure := classifyFailure(runErr)
		if _, err := s.database.FailPipelineJob(failureFor(job, failure.category, failure.retryable, time.Now())); err != nil {
			return nil, fmt.Errorf("%v; record Azure worker failure: %w", runErr, err)
		}
		return nil, runErr
	}
	if prepared.remoteFailure != nil {
		failure := prepared.remoteFailure
		if _, err := s.database.FailPipelineJob(failureFor(job, failure.category, failure.retryable, time.Now())); err != nil {
			prepared.rollback()
			return nil, fmt.Errorf("remote worker failed; record failure: %w", err)
		}
		prepared.rollback()
		return nil, fmt.Errorf("remote worker failed with category %s", failure.category)
	}
	replay, err := s.database.CompletePipelineJob(db.PipelineJobCompletion{
		JobID: job.ID, LeaseOwner: s.owner, LeaseFence: job.LeaseFence,
		DesiredHeadSHA: job.DesiredHeadSHA, InputDigest: job.InputDigest,
		OwnerDecisionHead: job.OwnerDecisionHead, DesiredGeneration: job.DesiredGeneration,
		ResultDigest: prepared.resultDigest, OutputHeadSHA: prepared.result.OutputHeadSHA,
		CompletedAt: time.Now(),
	})
	if err != nil {
		prepared.rollback()
		return nil, err
	}
	_ = replay
	prepared.keep()
	return &Execution{
		JobID: job.ID, OutputHeadSHA: prepared.result.OutputHeadSHA,
		ResultDigest: prepared.resultDigest, ReturnedBranch: prepared.returnedBranch,
		StepOutcome: prepared.stepOutcome,
	}, nil
}

type heartbeatLoop struct {
	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan error
}

func (s *Service) startHeartbeats(ctx context.Context, cancel context.CancelFunc, job *db.PipelineJob) *heartbeatLoop {
	loop := &heartbeatLoop{stopCh: make(chan struct{}), doneCh: make(chan error, 1)}
	go func() {
		ticker := time.NewTicker(s.cfg.HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-loop.stopCh:
				loop.doneCh <- nil
				return
			case <-ctx.Done():
				loop.doneCh <- nil
				return
			case at := <-ticker.C:
				_, err := s.database.HeartbeatPipelineJob(db.PipelineJobHeartbeat{
					JobID: job.ID, LeaseOwner: s.owner, LeaseFence: job.LeaseFence,
					DesiredHeadSHA: job.DesiredHeadSHA, InputDigest: job.InputDigest,
					OwnerDecisionHead: job.OwnerDecisionHead, DesiredGeneration: job.DesiredGeneration,
					HeartbeatAt: at, LeaseDuration: s.cfg.LeaseDuration,
				})
				if err != nil {
					cancel()
					loop.doneCh <- err
					return
				}
			}
		}
	}()
	return loop
}

func (l *heartbeatLoop) stop() error {
	l.stopOnce.Do(func() { close(l.stopCh) })
	return <-l.doneCh
}

type preparedResult struct {
	result         ResultEnvelope
	resultDigest   string
	returnedBranch string
	stepOutcome    StepOutcomeEnvelope
	remoteFailure  *transportFailure
	rollbackFunc   func()
}

func (p *preparedResult) rollback() {
	if p != nil && p.rollbackFunc != nil {
		p.rollbackFunc()
	}
}

func (p *preparedResult) keep() {
	if p != nil {
		p.rollbackFunc = nil
	}
}

func (s *Service) executeClaimed(ctx context.Context, job *db.PipelineJob) (*preparedResult, error) {
	run, err := s.database.GetRun(job.RunID)
	if err != nil {
		return nil, failureError("source_invalid", false, fmt.Errorf("read bound run: %w", err))
	}
	if run == nil {
		return nil, failureError("source_invalid", false, errors.New("bound run is absent"))
	}
	repo, err := s.database.GetRepo(run.RepoID)
	if err != nil {
		return nil, failureError("source_invalid", false, fmt.Errorf("read bound repository: %w", err))
	}
	if repo == nil {
		return nil, failureError("source_invalid", false, errors.New("bound repository is absent"))
	}
	input, err := s.input.InputFor(ctx, job)
	if err != nil {
		return nil, failureError("input_unavailable", true, err)
	}
	if len(input) == 0 || len(input) > maxInputBytes || bytes.IndexByte(input, 0) >= 0 || digestBytes(input) != job.InputDigest {
		return nil, failureError("input_mismatch", false, errors.New("worker input is empty, oversized, binary, or digest-mismatched"))
	}
	stage, err := os.MkdirTemp(s.workRoot, "firstmate-worker-")
	if err != nil {
		return nil, failureError("staging_failure", true, err)
	}
	defer os.RemoveAll(stage)
	if err := os.Chmod(stage, 0o700); err != nil {
		return nil, failureError("staging_failure", true, err)
	}
	payload := filepath.Join(stage, "payload")
	if err := os.Mkdir(payload, 0o700); err != nil {
		return nil, failureError("staging_failure", true, err)
	}
	bundlePath := filepath.Join(payload, "repo.bundle")
	sourceDir := repo.WorkingPath
	if run.WorktreeDir != nil && strings.TrimSpace(*run.WorktreeDir) != "" {
		sourceDir = filepath.Clean(*run.WorktreeDir)
	}
	if err := exactCleanSourceBundle(ctx, sourceDir, job.DesiredHeadSHA, bundlePath); err != nil {
		return nil, failureError("source_invalid", false, err)
	}
	if err := os.WriteFile(filepath.Join(payload, "brief.md"), input, 0o600); err != nil {
		return nil, failureError("staging_failure", true, err)
	}
	bundleDigest, bundleSize, err := digestFile(bundlePath)
	if err != nil {
		return nil, failureError("staging_failure", true, err)
	}
	request := requestFor(job, s.owner, bundleDigest, bundleSize)
	requestPath := filepath.Join(stage, "request.json")
	if err := writeJSONFile(requestPath, request); err != nil {
		return nil, failureError("staging_failure", true, err)
	}
	resultPath := filepath.Join(stage, "result.json")
	outcomePath := filepath.Join(stage, "outcome.bundle")
	stepOutcomePath := filepath.Join(stage, "step-outcome.json")
	if err := s.runWrapper(ctx, stage, requestPath, payload, resultPath, outcomePath, stepOutcomePath); err != nil {
		if ctx.Err() != nil {
			return nil, failureError("wrapper_timeout", true, ctx.Err())
		}
		return nil, failureError("wrapper_failure", true, err)
	}
	resultBytes, err := readRegularBounded(resultPath, maxResultBytes)
	if err != nil {
		return nil, failureError("malformed_result", false, err)
	}
	result, err := decodeResult(resultBytes)
	if err != nil {
		return nil, failureError("malformed_result", false, err)
	}
	if !resultMatchesRequest(result, request) {
		return nil, failureError("stale_result", false, errors.New("worker result exact binding mismatch"))
	}
	prepared := &preparedResult{result: result, resultDigest: digestBytes(resultBytes)}
	if result.Outcome == "failed" {
		if !validCategory(result.ErrorCategory) || result.OutputHeadSHA != "" || result.ReturnRef != "" || result.ReturnBundleSHA256 != "" || result.StepOutcomeSHA256 != "" {
			return nil, failureError("malformed_result", false, errors.New("worker failure result has invalid metadata"))
		}
		prepared.remoteFailure = &transportFailure{category: result.ErrorCategory, retryable: result.Retryable}
		return prepared, nil
	}
	if result.Outcome != "succeeded" || result.ErrorCategory != "" || result.Retryable {
		return nil, failureError("malformed_result", false, errors.New("worker result outcome is invalid"))
	}
	stepOutcomeBytes, err := readRegularBounded(stepOutcomePath, maxStepOutcomeBytes)
	if err != nil {
		return nil, failureError("malformed_result", false, fmt.Errorf("read worker step outcome: %w", err))
	}
	if result.StepOutcomeSHA256 == "" || digestBytes(stepOutcomeBytes) != result.StepOutcomeSHA256 {
		return nil, failureError("malformed_result", false, errors.New("worker step outcome digest mismatch"))
	}
	stepOutcome, err := decodeStepOutcome(stepOutcomeBytes, job.Kind, result.OutputHeadSHA)
	if err != nil {
		return nil, failureError("malformed_result", false, fmt.Errorf("decode worker step outcome: %w", err))
	}
	prepared.stepOutcome = stepOutcome
	if job.Kind != db.PipelineJobRepair {
		if result.OutputHeadSHA != job.DesiredHeadSHA || result.ReturnRef != "" || result.ReturnBundleSHA256 != "" {
			return nil, failureError("stale_result", false, errors.New("read-only worker changed the exact head"))
		}
		return prepared, nil
	}
	branch, rollback, err := materializeRepair(ctx, sourceDir, job, result, outcomePath)
	if err != nil {
		return nil, failureError("returned_result_invalid", false, err)
	}
	prepared.returnedBranch = branch
	prepared.rollbackFunc = rollback
	return prepared, nil
}

func requestFor(job *db.PipelineJob, owner, bundleDigest string, bundleSize int64) Request {
	return Request{
		Schema: RequestSchema, JobID: job.ID, RunID: job.RunID, StepResultID: job.StepResultID,
		Kind: job.Kind, Round: job.Round, DesiredHeadSHA: job.DesiredHeadSHA,
		InputDigest: job.InputDigest, OwnerDecisionHead: job.OwnerDecisionHead,
		DesiredGeneration: job.DesiredGeneration, Attempt: job.AttemptsStarted,
		LeaseFence: job.LeaseFence, LeaseOwner: owner, SourceRef: "HEAD",
		SourceBundleSHA256: bundleDigest, SourceBundleSize: bundleSize,
		GuestArgv:            []string{"no-mistakes", "worker", "run", "--role", string(job.Kind), "--brief", "brief.md", "--result", "outcome.json"},
		ExpectedResultSchema: ResultSchema, ExpectedFirstmateReturn: "fm.worker-return-contract/v1",
	}
}

func (s *Service) runWrapper(ctx context.Context, dir, request, payload, result, outcome, stepOutcome string) error {
	// Re-check the executable trust boundary immediately before every dispatch;
	// startup validation alone would allow a later path replacement.
	if err := validateTrustedFile(s.cfg.RunnerPath, true, false); err != nil {
		return fmt.Errorf("revalidate Firstmate wrapper: %w", err)
	}
	if err := validateTrustedFile(s.cfg.ConfigPath, false, true); err != nil {
		return fmt.Errorf("revalidate Firstmate wrapper config: %w", err)
	}
	cmd := exec.CommandContext(ctx, s.cfg.RunnerPath,
		"--config", s.cfg.ConfigPath, "execute",
		"--request", request, "--payload", payload,
		"--result", result, "--outcome", outcome,
		"--step-outcome", stepOutcome,
	)
	cmd.Dir = dir
	home := filepath.Join(dir, "home")
	tmp := filepath.Join(dir, "tmp")
	if err := os.Mkdir(home, 0o700); err != nil {
		return err
	}
	if err := os.Mkdir(tmp, 0o700); err != nil {
		return err
	}
	path := "/usr/bin:/bin"
	if resolved, err := shellenv.Resolve(); err == nil {
		for _, entry := range resolved {
			if strings.HasPrefix(entry, "PATH=") {
				path = strings.TrimPrefix(entry, "PATH=")
				break
			}
		}
	}
	cmd.Env = []string{"PATH=" + path, "HOME=" + home, "TMPDIR=" + tmp, "LANG=C", "LC_ALL=C", "GIT_TERMINAL_PROMPT=0"}
	var output boundedBuffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	shellenv.ConfigureShellCommand(cmd)
	cmd.WaitDelay = 250 * time.Millisecond
	return shellenv.RunShellCommand(cmd)
}

type boundedBuffer struct {
	mu   sync.Mutex
	data []byte
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	const limit = 64 << 10
	written := len(p)
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.data) < limit {
		remaining := limit - len(b.data)
		if len(p) > remaining {
			p = p[:remaining]
		}
		b.data = append(b.data, p...)
	}
	return written, nil
}

func exactCleanSourceBundle(ctx context.Context, repo, head, destination string) error {
	if err := exactCleanHead(ctx, repo, head); err != nil {
		return err
	}
	if _, err := gitx.Run(ctx, repo, "bundle", "create", destination, "HEAD"); err != nil {
		return err
	}
	if err := os.Chmod(destination, 0o600); err != nil {
		return err
	}
	listed, err := gitx.Run(ctx, repo, "bundle", "list-heads", destination)
	if err != nil {
		return err
	}
	lines := nonemptyLines(listed)
	if len(lines) != 1 || !strings.HasPrefix(lines[0], head+" ") || !strings.HasSuffix(lines[0], " HEAD") {
		return fmt.Errorf("source bundle must contain exactly bound HEAD, got %q", listed)
	}
	return exactCleanHead(ctx, repo, head)
}

func exactCleanHead(ctx context.Context, repo, head string) error {
	current, err := gitx.Run(ctx, repo, "rev-parse", "HEAD")
	if err != nil {
		return fmt.Errorf("read source HEAD: %w", err)
	}
	if current != head {
		return fmt.Errorf("source HEAD is %q, want %q", current, head)
	}
	status, err := gitx.Run(ctx, repo, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil {
		return fmt.Errorf("read source worktree status: %w", err)
	}
	if status != "" {
		return errors.New("source worktree must be exactly clean")
	}
	return nil
}

func materializeRepair(ctx context.Context, repo string, job *db.PipelineJob, result ResultEnvelope, bundle string) (string, func(), error) {
	if result.OutputHeadSHA == "" || result.OutputHeadSHA == job.DesiredHeadSHA || result.ReturnRef == "" || result.ReturnBundleSHA256 == "" {
		return "", nil, errors.New("repair result is missing a new head, return ref, or bundle digest")
	}
	if err := exactCleanHead(ctx, repo, job.DesiredHeadSHA); err != nil {
		return "", nil, err
	}
	digest, _, err := digestFile(bundle)
	if err != nil {
		return "", nil, fmt.Errorf("digest repair bundle: %w", err)
	}
	if digest != result.ReturnBundleSHA256 {
		return "", nil, errors.New("repair bundle digest mismatch")
	}
	if _, err := gitx.Run(ctx, repo, "bundle", "verify", bundle); err != nil {
		return "", nil, err
	}
	listed, err := gitx.Run(ctx, repo, "bundle", "list-heads", bundle)
	if err != nil {
		return "", nil, err
	}
	lines := nonemptyLines(listed)
	if len(lines) != 1 || lines[0] != result.OutputHeadSHA+" "+result.ReturnRef {
		return "", nil, fmt.Errorf("repair bundle must contain exactly returned ref %s at %s", result.ReturnRef, result.OutputHeadSHA)
	}
	identity := sha256.Sum256([]byte(job.ID))
	quarantine := "refs/no-mistakes/azure-quarantine/" + hex.EncodeToString(identity[:8]) + "-" + strconv.FormatInt(job.LeaseFence, 10)
	defer gitx.Run(context.Background(), repo, "update-ref", "-d", quarantine)
	if _, err := gitx.Run(ctx, repo, "fetch", "--no-tags", bundle, "+"+result.ReturnRef+":"+quarantine); err != nil {
		return "", nil, err
	}
	fetched, err := gitx.Run(ctx, repo, "rev-parse", quarantine+"^{commit}")
	if err != nil || fetched != result.OutputHeadSHA {
		return "", nil, errors.New("repair bundle did not materialize the declared commit")
	}
	if _, err := gitx.Run(ctx, repo, "merge-base", "--is-ancestor", job.DesiredHeadSHA, result.OutputHeadSHA); err != nil {
		return "", nil, errors.New("repair head is not a descendant of the bound input head")
	}
	branch := "no-mistakes/azure-results/" + hex.EncodeToString(identity[:8]) + "-" + strconv.FormatInt(job.LeaseFence, 10)
	if _, err := gitx.Run(ctx, repo, "check-ref-format", "--branch", branch); err != nil {
		return "", nil, err
	}
	zero := strings.Repeat("0", len(result.OutputHeadSHA))
	if _, err := gitx.Run(ctx, repo, "update-ref", "refs/heads/"+branch, result.OutputHeadSHA, zero); err != nil {
		return "", nil, err
	}
	rollback := func() {
		_, _ = gitx.Run(context.Background(), repo, "update-ref", "-d", "refs/heads/"+branch, result.OutputHeadSHA)
	}
	if err := exactCleanHead(ctx, repo, job.DesiredHeadSHA); err != nil {
		rollback()
		return "", nil, err
	}
	return branch, rollback, nil
}

func decodeResult(data []byte) (ResultEnvelope, error) {
	var result ResultEnvelope
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return result, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return result, errors.New("worker result has trailing data")
	}
	return result, nil
}

func resultMatchesRequest(result ResultEnvelope, request Request) bool {
	return result.Schema == ResultSchema && result.JobID == request.JobID &&
		result.RunID == request.RunID && result.StepResultID == request.StepResultID &&
		result.Kind == request.Kind && result.Round == request.Round &&
		result.DesiredHeadSHA == request.DesiredHeadSHA && result.InputDigest == request.InputDigest &&
		result.OwnerDecisionHead == request.OwnerDecisionHead && result.DesiredGeneration == request.DesiredGeneration &&
		result.Attempt == request.Attempt && result.LeaseFence == request.LeaseFence && result.LeaseOwner == request.LeaseOwner &&
		result.SourceBundleSHA256 == request.SourceBundleSHA256
}

type transportFailure struct {
	category  string
	retryable bool
	err       error
}

func (e *transportFailure) Error() string { return e.err.Error() }
func (e *transportFailure) Unwrap() error { return e.err }

func failureError(category string, retryable bool, err error) error {
	return &transportFailure{category: category, retryable: retryable, err: err}
}

func classifyFailure(err error) *transportFailure {
	var failure *transportFailure
	if errors.As(err, &failure) {
		return failure
	}
	return &transportFailure{category: "transport_failure", retryable: true, err: err}
}

func failureFor(job *db.PipelineJob, category string, retryable bool, at time.Time) db.PipelineJobFailure {
	return db.PipelineJobFailure{
		JobID: job.ID, LeaseOwner: jobLeaseOwner(job), LeaseFence: job.LeaseFence,
		DesiredHeadSHA: job.DesiredHeadSHA, InputDigest: job.InputDigest,
		OwnerDecisionHead: job.OwnerDecisionHead, DesiredGeneration: job.DesiredGeneration,
		ErrorCategory: category, Retryable: retryable, FailedAt: at,
	}
}

func jobLeaseOwner(job *db.PipelineJob) string {
	if job != nil && job.LeaseOwner != nil {
		return *job.LeaseOwner
	}
	return ""
}

func validateTrustedFile(path string, executable, private bool) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("path must be absolute and clean")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 {
		return errors.New("path must be a non-symlink regular file not writable by group or others")
	}
	if executable && info.Mode().Perm()&0o100 == 0 {
		return errors.New("runner is not owner-executable")
	}
	if private && info.Mode().Perm()&0o077 != 0 {
		return errors.New("config must not be accessible by group or others")
	}
	return nil
}

func writeJSONFile(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o600)
}

func readRegularBounded(path string, limit int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() < 1 || info.Size() > limit {
		return nil, errors.New("artifact is not a bounded regular file")
	}
	return os.ReadFile(path)
}

func digestFile(path string) (string, int64, error) {
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return "", 0, err
	}
	if !pathInfo.Mode().IsRegular() || pathInfo.Mode()&os.ModeSymlink != 0 {
		return "", 0, errors.New("artifact is not a non-symlink regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || !os.SameFile(pathInfo, info) {
		return "", 0, errors.New("artifact is not regular")
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(hash.Sum(nil)), info.Size(), nil
}

func digestBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func nonemptyLines(value string) []string {
	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(value), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func validCategory(value string) bool {
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
