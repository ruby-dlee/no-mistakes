package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
	qualityExpected := request.Step == types.StepReview && request.QualityOutcomeAuthority == "semantic-rereview"
	if qualityExpected != (execution.QualityOutcome != nil) {
		return nil, errors.New("remote repair quality outcome authority mismatch")
	}
	if err := e.adoptRemoteRepair(ctx, request, execution, runHead); err != nil {
		return nil, err
	}
	if execution.QualityOutcome != nil {
		outcome := *execution.QualityOutcome
		outcome.RunID = request.RunID
		if _, err := e.db.InsertQualityOutcome(outcome); err != nil {
			return nil, fmt.Errorf("record remote semantic quality outcome after retaining adopted repair custody: %w", err)
		}
	}
	return &execution.Outcome, nil
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
		return fmt.Errorf("verify adopted remote repair; retained forward custody without reverse worktree mutation: %w", err)
	}
	updated, err := e.db.AdvanceRunHeadSHA(request.RunID, request.DesiredHeadSHA, execution.OutputHeadSHA)
	if err != nil || !updated {
		if err != nil {
			return fmt.Errorf("record adopted remote repair; retained forward custody without reverse worktree mutation: %w", err)
		}
		return errors.New("record adopted remote repair: run head binding is stale; retained forward custody without reverse worktree mutation")
	}
	*runHead = execution.OutputHeadSHA
	_, _ = gitx.Run(context.Background(), request.WorkDir, "update-ref", "-d", "refs/heads/"+execution.ReturnedBranch, execution.OutputHeadSHA)
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
