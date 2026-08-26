package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/types"
	"github.com/kunchenguid/no-mistakes/internal/workertransport"
)

type azureWorkerRuntime struct {
	database *db.DB
	store    *workertransport.DurableStore
	service  *workertransport.Service
	wake     map[db.PipelineJobKind]chan struct{}
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	start    sync.Once
	close    sync.Once
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
	return &azureWorkerRuntime{
		database: database, store: store, service: service,
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
			r.wg.Add(1)
			go r.workerLoop(ctx, kind)
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
	})
}

func (r *azureWorkerRuntime) ExecuteRemoteStep(ctx context.Context, request pipeline.RemoteStepRequest) (*pipeline.RemoteStepExecution, error) {
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
	desiredGeneration := int64(0)
	if desired, err := r.database.GetBranchDesiredState(request.RepoID, request.Branch); err != nil {
		return nil, err
	} else if desired != nil {
		if desired.HeadSHA != request.DesiredHeadSHA || desired.InputDigest != inputDigest {
			return nil, errors.New("Azure worker branch desired-state binding does not match this exact step input")
		}
		desiredGeneration = desired.Revision
	}
	job, _, err := r.database.EnqueuePipelineJob(db.PipelineJobSpec{
		RunID: request.RunID, StepResultID: request.StepResultID, Kind: kind, Round: request.Round,
		DesiredHeadSHA: request.DesiredHeadSHA, InputDigest: inputDigest,
		OwnerDecisionHead: ownerHead, DesiredGeneration: desiredGeneration, MaxAttempts: 3,
	})
	if err != nil {
		return nil, err
	}
	select {
	case r.wake[kind] <- struct{}{}:
	default:
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
