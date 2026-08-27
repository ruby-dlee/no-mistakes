package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
	gitpkg "github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/types"
	"github.com/kunchenguid/no-mistakes/internal/workertransport"
)

type azureWorkerRuntime struct {
	database       *db.DB
	store          *workertransport.DurableStore
	service        *workertransport.Service
	wake           map[db.PipelineJobKind]chan struct{}
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	start          sync.Once
	close          sync.Once
	concurrency    map[db.PipelineJobKind]int
	ownerFixesOnly bool
}

type azureRemoteRecovery struct {
	job     *db.PipelineJob
	request pipeline.RemoteStepRequest
}

func newAzureWorkerRuntime(cfg config.AzureWorkerConfig, database *db.DB, p *paths.Paths) (*azureWorkerRuntime, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	root := filepath.Join(p.Root(), "azure-worker")
	store, err := workertransport.NewDurableStore(root)
	if err != nil {
		return nil, err
	}
	staging := filepath.Join(root, "staging")
	if err := os.MkdirAll(staging, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(staging, 0o700); err != nil {
		return nil, err
	}
	owner := fmt.Sprintf("daemon-%d", os.Getpid())
	service, err := workertransport.New(database, cfg, staging, owner, store, store)
	if err != nil {
		return nil, err
	}
	reviewConcurrency := cfg.ReviewConcurrency
	repairConcurrency := cfg.RepairConcurrency
	testConcurrency := cfg.TestConcurrency
	if reviewConcurrency == 0 {
		reviewConcurrency = 1
	}
	if repairConcurrency == 0 {
		repairConcurrency = 1
	}
	if testConcurrency == 0 {
		testConcurrency = 1
	}
	return &azureWorkerRuntime{
		database: database, store: store, service: service,
		ownerFixesOnly: cfg.OwnerFixesOnly,
		concurrency: map[db.PipelineJobKind]int{
			db.PipelineJobReview: reviewConcurrency,
			db.PipelineJobRepair: repairConcurrency,
			db.PipelineJobTest:   testConcurrency,
		},
		wake: map[db.PipelineJobKind]chan struct{}{
			db.PipelineJobReview: make(chan struct{}, 1),
			db.PipelineJobRepair: make(chan struct{}, 1),
			db.PipelineJobTest:   make(chan struct{}, 1),
		},
	}, nil
}

func (r *azureWorkerRuntime) Start(parent context.Context) {
	if r == nil {
		return
	}
	r.start.Do(func() {
		ctx, cancel := context.WithCancel(parent)
		r.cancel = cancel
		if count, err := r.database.RequeueExpiredPipelineJobs(time.Now()); err != nil {
			slog.Warn("Azure worker startup lease recovery failed", "error", err)
		} else if count > 0 {
			slog.Info("Azure worker recovered expired leases", "count", count)
		}
		for _, kind := range []db.PipelineJobKind{db.PipelineJobReview, db.PipelineJobRepair, db.PipelineJobTest} {
			for index := 0; index < r.concurrency[kind]; index++ {
				r.wg.Add(1)
				go r.workerLoop(ctx, kind)
			}
		}
	})
}

func (r *azureWorkerRuntime) workerLoop(ctx context.Context, kind db.PipelineJobKind) {
	defer r.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		execution, err := r.service.ProcessOne(ctx, kind)
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, workertransport.ErrDisabled) {
			slog.Warn("Azure worker execution failed closed", "kind", kind, "error", err)
		}
		if execution != nil {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-r.wake[kind]:
		case <-ticker.C:
		}
	}
}

func (r *azureWorkerRuntime) Close() {
	if r == nil {
		return
	}
	r.close.Do(func() {
		if r.cancel != nil {
			r.cancel()
		}
		r.wg.Wait()
		if err := r.service.Close(); err != nil {
			slog.Warn("Azure worker runtime snapshot cleanup failed", "error", err)
		}
	})
}

func (r *azureWorkerRuntime) ExecuteRemoteStep(ctx context.Context, request pipeline.RemoteStepRequest) (*pipeline.RemoteStepExecution, error) {
	if r != nil && r.ownerFixesOnly && request.Fixing && request.RecoveryJobID == "" {
		return nil, errors.New("owner_fixes_only is enabled: Azure refuses new repair jobs; the calling agent owns the fix")
	}
	if r == nil || r.service == nil {
		return nil, errors.New("Azure worker runtime is unavailable")
	}
	kind := db.PipelineJobReview
	if request.Step == types.StepTest {
		kind = db.PipelineJobTest
	} else if request.Step != types.StepReview {
		return nil, fmt.Errorf("Azure worker does not execute step %q", request.Step)
	}
	if request.Fixing {
		kind = db.PipelineJobRepair
	}
	var job *db.PipelineJob
	if request.RecoveryJobID != "" {
		var err error
		job, err = r.database.GetPipelineJob(request.RecoveryJobID)
		if err != nil {
			return nil, err
		}
		if job == nil {
			return nil, errors.New("recovered Azure worker job disappeared")
		}
		inputBytes, err := r.store.InputFor(ctx, job)
		if err != nil {
			return nil, fmt.Errorf("read recovered Azure worker input: %w", err)
		}
		input, err := workertransport.DecodeStepInput(inputBytes)
		if err != nil {
			return nil, fmt.Errorf("decode recovered Azure worker input: %w", err)
		}
		if err := validateAzureRecoveryBinding(job, input, request); err != nil {
			return nil, err
		}
	} else {
		baseSHA, resolveErr := resolveAzureWorkerBaseSHA(ctx, request)
		if resolveErr != nil {
			return nil, resolveErr
		}
		request.BaseSHA = baseSHA
		var err error
		job, err = r.enqueueRemoteStep(request, kind)
		if err != nil {
			return nil, err
		}
	}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, err := r.database.GetPipelineJob(job.ID)
		if err != nil {
			return nil, err
		}
		if current == nil {
			return nil, errors.New("Azure worker job disappeared")
		}
		switch current.Status {
		case db.PipelineJobCompleted:
			result, err := r.store.ReadResult(current)
			if err != nil {
				return nil, fmt.Errorf("read completed Azure worker result: %w", err)
			}
			qualityExpected := request.QualityOutcomeAuthority == "semantic-rereview" && request.Fixing && request.Step == types.StepReview
			if qualityExpected != (result.StepOutcome.QualityOutcome != nil) {
				return nil, errors.New("completed Azure worker result has mismatched semantic quality authority")
			}
			var qualityOutcome *db.QualityOutcome
			if result.StepOutcome.QualityOutcome != nil {
				quality := result.StepOutcome.QualityOutcome
				fixAttemptID := quality.FixAttemptID
				var rootID *string
				if quality.RootID != "" {
					root := quality.RootID
					rootID = &root
				}
				jobID := current.ID
				qualityOutcome = &db.QualityOutcome{
					RunID: request.RunID, JobID: &jobID, FixAttemptID: &fixAttemptID, RootID: rootID,
					Classification: db.QualityClassification(quality.Classification),
					FixedHeadSHA:   quality.FixedHeadSHA, ObservedHeadSHA: quality.ObservedHeadSHA,
					EvidenceDigest: quality.EvidenceDigest, EvidenceProvenance: quality.EvidenceProvenance,
				}
			}
			return &pipeline.RemoteStepExecution{
				JobID: current.ID,
				Outcome: pipeline.StepOutcome{
					NeedsApproval:         result.StepOutcome.NeedsApproval,
					AutoFixable:           result.StepOutcome.AutoFixable,
					Findings:              result.StepOutcome.FindingsJSON,
					ExitCode:              result.StepOutcome.ExitCode,
					FixSummary:            result.StepOutcome.FixSummary,
					ReviewApprovedHeadSHA: result.StepOutcome.ReviewApprovedHeadSHA,
					Skipped:               result.StepOutcome.Skipped,
					SkipRemaining:         result.StepOutcome.SkipRemaining,
				},
				OutputHeadSHA: result.OutputHeadSHA, ReturnedBranch: result.ReturnedBranch,
				QualityOutcome: qualityOutcome,
			}, nil
		case db.PipelineJobFailed:
			category := "unknown"
			if current.ErrorCategory != nil {
				category = *current.ErrorCategory
			}
			return nil, fmt.Errorf("Azure worker job failed closed: %s", category)
		case db.PipelineJobSuperseded:
			return nil, errors.New("Azure worker job was superseded by newer exact state")
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func resolveAzureWorkerBaseSHA(ctx context.Context, request pipeline.RemoteStepRequest) (string, error) {
	if request.WorkDir == "" {
		return "", errors.New("Azure worker request is missing its source worktree")
	}
	if defaultBranch := strings.TrimSpace(request.DefaultBranch); defaultBranch != "" {
		for _, ref := range []string{"origin/" + defaultBranch, defaultBranch} {
			baseSHA, err := gitpkg.Run(ctx, request.WorkDir, "merge-base", request.DesiredHeadSHA, ref)
			if err == nil && baseSHA != "" {
				return baseSHA, nil
			}
		}
	}
	if gitpkg.IsZeroSHA(request.BaseSHA) {
		return "", errors.New("Azure worker could not resolve a commit base for the new branch")
	}
	if _, err := gitpkg.Run(ctx, request.WorkDir, "cat-file", "-e", request.BaseSHA+"^{commit}"); err != nil {
		return "", errors.New("Azure worker fallback base commit is unavailable")
	}
	if _, err := gitpkg.Run(ctx, request.WorkDir, "merge-base", "--is-ancestor", request.BaseSHA, request.DesiredHeadSHA); err != nil {
		return "", errors.New("Azure worker fallback base is not an ancestor of the exact head")
	}
	return request.BaseSHA, nil
}

func (r *azureWorkerRuntime) enqueueRemoteStep(request pipeline.RemoteStepRequest, kind db.PipelineJobKind) (*db.PipelineJob, error) {
	if r.ownerFixesOnly && kind == db.PipelineJobRepair {
		return nil, errors.New("owner_fixes_only is enabled: Azure refuses new repair jobs; the calling agent owns the fix")
	}
	inputBytes, err := json.Marshal(workertransport.StepInputEnvelope{
		Schema: workertransport.StepInputSchema, RunID: request.RunID, RepoID: request.RepoID,
		StepResultID: request.StepResultID, Step: request.Step, Round: request.Round,
		DesiredHeadSHA: request.DesiredHeadSHA, BaseSHA: request.BaseSHA, Branch: request.Branch,
		DefaultBranch:   request.DefaultBranch,
		RuntimeIdentity: r.service.RuntimeIdentity(),
		Fixing:          request.Fixing, PreviousFindings: request.PreviousFindings,
		UserIntent: request.UserIntent, UserIntentSource: request.UserIntentSource,
		PriorRoundHistory:       request.PriorRoundHistory,
		UncertifiedRoundHistory: request.UncertifiedRoundHistory,
		RepairAttempt:           request.RepairAttempt,
		QualityOutcomeAuthority: request.QualityOutcomeAuthority,
	})
	if err != nil {
		return nil, err
	}
	inputBytes = append(inputBytes, '\n')
	inputDigest, err := r.store.PutInput(inputBytes)
	if err != nil {
		return nil, err
	}
	ownerHead, _, err := r.database.OwnerDecisionHead(request.RunID)
	if err != nil {
		return nil, err
	}
	desired, _, _, err := r.database.AdvanceWorkerDesiredState(db.BranchDesiredUpdate{
		RepoID: request.RepoID, Branch: request.Branch, HeadSHA: request.DesiredHeadSHA,
		InputDigest: inputDigest, UpdatedAt: time.Now(),
	})
	if err != nil {
		return nil, err
	}
	job, _, err := r.database.EnqueuePipelineJob(db.PipelineJobSpec{
		RunID: request.RunID, StepResultID: request.StepResultID, Kind: kind, Round: request.Round,
		DesiredHeadSHA: request.DesiredHeadSHA, InputDigest: inputDigest,
		OwnerDecisionHead: ownerHead, DesiredGeneration: desired.Revision, MaxAttempts: 3,
	})
	if err != nil {
		return nil, err
	}
	select {
	case r.wake[kind] <- struct{}{}:
	default:
	}
	return job, nil
}

func validateAzureRecoveryBinding(job *db.PipelineJob, input workertransport.StepInputEnvelope, request pipeline.RemoteStepRequest) error {
	wantKind := db.PipelineJobReview
	if input.Step == types.StepTest {
		wantKind = db.PipelineJobTest
	} else if input.Fixing {
		wantKind = db.PipelineJobRepair
	}
	if job.ID != request.RecoveryJobID || job.Kind != wantKind || job.RunID != input.RunID ||
		job.StepResultID != input.StepResultID || job.Round != input.Round || job.DesiredHeadSHA != input.DesiredHeadSHA {
		return errors.New("recovered Azure worker job and durable input binding changed")
	}
	if request.RunID != input.RunID || request.RepoID != input.RepoID || request.StepResultID != input.StepResultID ||
		request.Step != input.Step || request.Round != input.Round || request.DesiredHeadSHA != input.DesiredHeadSHA ||
		request.BaseSHA != input.BaseSHA || request.Branch != input.Branch || request.DefaultBranch != input.DefaultBranch ||
		request.Fixing != input.Fixing || request.PreviousFindings != input.PreviousFindings ||
		request.UserIntent != input.UserIntent || request.UserIntentSource != input.UserIntentSource ||
		request.PriorRoundHistory != input.PriorRoundHistory || request.UncertifiedRoundHistory != input.UncertifiedRoundHistory ||
		request.RepairAttempt != input.RepairAttempt || request.QualityOutcomeAuthority != input.QualityOutcomeAuthority {
		return errors.New("recovered Azure worker request does not match its durable input")
	}
	return nil
}

func (r *azureWorkerRuntime) recoverableRemoteSteps(ctx context.Context) ([]azureRemoteRecovery, error) {
	jobs, err := r.database.RecoverablePipelineJobs()
	if err != nil {
		return nil, err
	}
	byRun := make(map[string]azureRemoteRecovery, len(jobs))
	duplicates := make(map[string]bool)
	for _, job := range jobs {
		data, readErr := r.store.InputFor(ctx, job)
		if readErr != nil {
			slog.Warn("discarding unrecoverable Azure worker custody", "run_id", job.RunID, "job_id", job.ID, "error", readErr)
			continue
		}
		input, decodeErr := workertransport.DecodeStepInput(data)
		if decodeErr != nil {
			slog.Warn("discarding malformed Azure worker recovery input", "run_id", job.RunID, "job_id", job.ID, "error", decodeErr)
			continue
		}
		if input.RuntimeIdentity != r.service.RuntimeIdentity() {
			slog.Warn("discarding Azure worker recovery from a different runtime revision", "run_id", job.RunID, "job_id", job.ID)
			continue
		}
		request := pipeline.RemoteStepRequest{
			RunID: input.RunID, RepoID: input.RepoID, StepResultID: input.StepResultID,
			Step: input.Step, Round: input.Round, DesiredHeadSHA: input.DesiredHeadSHA,
			BaseSHA: input.BaseSHA, Branch: input.Branch, DefaultBranch: input.DefaultBranch,
			Fixing: input.Fixing, PreviousFindings: input.PreviousFindings,
			UserIntent: input.UserIntent, UserIntentSource: input.UserIntentSource,
			PriorRoundHistory: input.PriorRoundHistory, UncertifiedRoundHistory: input.UncertifiedRoundHistory,
			RepairAttempt: input.RepairAttempt, QualityOutcomeAuthority: input.QualityOutcomeAuthority,
			RecoveryJobID: job.ID,
		}
		run, runErr := r.database.GetRun(job.RunID)
		if runErr != nil || run == nil {
			slog.Warn("discarding Azure worker recovery without a running run", "run_id", job.RunID, "job_id", job.ID, "error", runErr)
			continue
		}
		if job.Kind == db.PipelineJobRepair && job.Status == db.PipelineJobCompleted && job.OutputHeadSHA != nil && run.HeadSHA == *job.OutputHeadSHA {
			request.RecoveryAdoptedHeadSHA = *job.OutputHeadSHA
		}
		if err := validateAzureRecoveryBinding(job, input, request); err != nil {
			slog.Warn("discarding inexact Azure worker recovery", "run_id", job.RunID, "job_id", job.ID, "error", err)
			continue
		}
		if _, exists := byRun[job.RunID]; exists {
			duplicates[job.RunID] = true
		}
		byRun[job.RunID] = azureRemoteRecovery{job: job, request: request}
	}
	result := make([]azureRemoteRecovery, 0, len(byRun))
	for runID, recovery := range byRun {
		if duplicates[runID] {
			slog.Warn("discarding ambiguous Azure worker recovery", "run_id", runID)
			continue
		}
		result = append(result, recovery)
	}
	return result, nil
}
