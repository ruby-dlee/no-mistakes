package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	gitx "github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

// RemoteStepRequest is the exact semantic work handed to an optional external
// review/test execution plane. The implementation owns durable serialization;
// pipeline remains the canonical owner of step and run state.
type RemoteStepRequest struct {
	RunID                   string
	RepoID                  string
	StepResultID            string
	Step                    types.StepName
	Round                   int
	DesiredHeadSHA          string
	BaseSHA                 string
	Branch                  string
	DefaultBranch           string
	Fixing                  bool
	PreviousFindings        string
	UserIntent              string
	UserIntentSource        string
	WorkDir                 string
	PriorRoundHistory       string
	UncertifiedRoundHistory string
	RepairAttempt           int
	QualityOutcomeAuthority string
}

type RemoteStepContext struct {
	PriorRoundHistory       string
	UncertifiedRoundHistory string
	RepairAttempt           int
	QualityOutcomeAuthority string
}

type RemoteStepContextProvider interface {
	RemoteStepContext(*StepContext) RemoteStepContext
}

type RemoteStepExecution struct {
	Outcome        StepOutcome
	OutputHeadSHA  string
	ReturnedBranch string
	QualityOutcome *db.QualityOutcome
}

type RemoteStepRunner interface {
	ExecuteRemoteStep(context.Context, RemoteStepRequest) (*RemoteStepExecution, error)
}

func (e *Executor) SetRemoteStepRunner(runner RemoteStepRunner) {
	if e != nil {
		e.remoteSteps = runner
	}
}

func (e *Executor) executeRemoteStep(ctx context.Context, request RemoteStepRequest, runHead *string) (*StepOutcome, error) {
	if e.remoteSteps == nil {
		return nil, errors.New("remote step runner is not configured")
	}
	execution, err := e.remoteSteps.ExecuteRemoteStep(ctx, request)
	if err != nil {
		return nil, err
	}
	if execution == nil || strings.TrimSpace(execution.OutputHeadSHA) == "" {
		return nil, errors.New("remote step returned no exact output head")
	}
	if !request.Fixing {
		if execution.QualityOutcome != nil {
			return nil, errors.New("read-only remote step returned quality-write authority")
		}
		if execution.OutputHeadSHA != request.DesiredHeadSHA || execution.ReturnedBranch != "" {
			return nil, errors.New("read-only remote step changed the exact head")
		}
		return &execution.Outcome, nil
	}
	if err := e.adoptRemoteRepair(ctx, request, execution, runHead); err != nil {
		return nil, err
	}
	qualityExpected := request.Step == types.StepReview && request.QualityOutcomeAuthority == "semantic-rereview"
	if qualityExpected != (execution.QualityOutcome != nil) {
		return nil, e.rollbackRemoteRepair(ctx, request, execution, runHead, errors.New("remote repair quality outcome authority mismatch"))
	}
	if execution.QualityOutcome != nil {
		outcome := *execution.QualityOutcome
		outcome.RunID = request.RunID
		if _, err := e.db.InsertQualityOutcome(outcome); err != nil {
			return nil, e.rollbackRemoteRepair(ctx, request, execution, runHead, fmt.Errorf("record remote semantic quality outcome: %w", err))
		}
	}
	return &execution.Outcome, nil
}

func (e *Executor) rollbackRemoteRepair(ctx context.Context, request RemoteStepRequest, execution *RemoteStepExecution, runHead *string, cause error) error {
	_, worktreeErr := gitx.Run(ctx, request.WorkDir, "reset", "--hard", request.DesiredHeadSHA)
	reverted, dbErr := e.db.AdvanceRunHeadSHA(request.RunID, execution.OutputHeadSHA, request.DesiredHeadSHA)
	if dbErr == nil && reverted {
		*runHead = request.DesiredHeadSHA
	}
	if worktreeErr != nil || dbErr != nil || !reverted {
		return fmt.Errorf("%w (repair rollback failed: worktree=%v database=%v reverted=%t)", cause, worktreeErr, dbErr, reverted)
	}
	return cause
}

func (e *Executor) adoptRemoteRepair(ctx context.Context, request RemoteStepRequest, execution *RemoteStepExecution, runHead *string) error {
	if execution.OutputHeadSHA == request.DesiredHeadSHA ||
		!strings.HasPrefix(execution.ReturnedBranch, "no-mistakes/azure-results/") {
		return errors.New("remote repair is missing a new exact head or controller-owned result branch")
	}
	if err := exactCleanRemoteHead(ctx, request.WorkDir, request.DesiredHeadSHA); err != nil {
		return err
	}
	branchHead, err := gitx.Run(ctx, request.WorkDir, "rev-parse", execution.ReturnedBranch+"^{commit}")
	if err != nil || branchHead != execution.OutputHeadSHA {
		return errors.New("remote repair branch does not resolve to its declared output head")
	}
	if _, err := gitx.Run(ctx, request.WorkDir, "merge-base", "--is-ancestor", request.DesiredHeadSHA, execution.OutputHeadSHA); err != nil {
		return errors.New("remote repair output is not a descendant of the exact input head")
	}
	if _, err := gitx.Run(ctx, request.WorkDir, "merge", "--ff-only", "--no-edit", execution.ReturnedBranch); err != nil {
		return fmt.Errorf("fast-forward remote repair: %w", err)
	}
	if err := exactCleanRemoteHead(ctx, request.WorkDir, execution.OutputHeadSHA); err != nil {
		rollbackErr := rollbackAdoptedRemoteRepair(ctx, request.WorkDir, execution.OutputHeadSHA, request.DesiredHeadSHA)
		return errors.Join(fmt.Errorf("verify adopted remote repair: %w", err), rollbackErr)
	}
	updated, err := e.db.AdvanceRunHeadSHA(request.RunID, request.DesiredHeadSHA, execution.OutputHeadSHA)
	if err != nil || !updated {
		rollbackErr := rollbackAdoptedRemoteRepair(ctx, request.WorkDir, execution.OutputHeadSHA, request.DesiredHeadSHA)
		if err != nil {
			return errors.Join(fmt.Errorf("record adopted remote repair: %w", err), rollbackErr)
		}
		return errors.Join(errors.New("record adopted remote repair: run head binding is stale"), rollbackErr)
	}
	*runHead = execution.OutputHeadSHA
	_, _ = gitx.Run(context.Background(), request.WorkDir, "update-ref", "-d", "refs/heads/"+execution.ReturnedBranch, execution.OutputHeadSHA)
	return nil
}

// rollbackAdoptedRemoteRepair restores the pre-adoption head only while HEAD
// still names the exact adopted commit and the index/worktree can make the
// guarded tree transition without overwriting a concurrent edit. It never
// uses reset --hard: if read-tree refuses, the ref is compare-and-swap restored
// to the adopted head so the operator's edit and the admitted repair remain in
// one coherent worktree for recovery.
func rollbackAdoptedRemoteRepair(parent context.Context, workDir, adoptedHead, originalHead string) error {
	ctx := parent
	if ctx == nil || ctx.Err() != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
	}
	if _, err := gitx.Run(ctx, workDir, "update-ref", "HEAD", originalHead, adoptedHead); err != nil {
		return fmt.Errorf("guarded remote repair rollback refused because HEAD moved: %w", err)
	}
	if _, err := gitx.Run(ctx, workDir, "read-tree", "-m", "-u", adoptedHead, originalHead); err != nil {
		if _, restoreErr := gitx.Run(ctx, workDir, "update-ref", "HEAD", adoptedHead, originalHead); restoreErr != nil {
			return fmt.Errorf("guarded remote repair rollback refused worktree changes: %v; restoring adopted HEAD also failed: %w", err, restoreErr)
		}
		return fmt.Errorf("guarded remote repair rollback refused worktree changes and retained adopted HEAD: %w", err)
	}
	if err := exactCleanRemoteHead(ctx, workDir, originalHead); err != nil {
		return fmt.Errorf("guarded remote repair rollback did not reach the original clean head: %w", err)
	}
	return nil
}

func exactCleanRemoteHead(ctx context.Context, workDir, head string) error {
	current, err := gitx.Run(ctx, workDir, "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	if current != head {
		return fmt.Errorf("remote step worktree HEAD is %s, want %s", current, head)
	}
	status, err := gitx.Run(ctx, workDir, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil {
		return err
	}
	if status != "" {
		return errors.New("remote step worktree is not exactly clean")
	}
	return nil
}
