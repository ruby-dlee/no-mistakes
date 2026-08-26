package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestExecutorDeferredStepLeavesDurableCustodyActive(t *testing.T) {
	database, p, run, repo := setupTest(t)
	step := &mockStep{name: types.StepCI, outcome: &StepOutcome{Deferred: true}}
	executor := NewExecutor(database, p, &config.Config{}, nil, []Step{step}, nil)

	err := executor.Execute(context.Background(), run, repo, t.TempDir())
	if !errors.Is(err, ErrPipelineDeferred) {
		t.Fatalf("Execute error = %v, want ErrPipelineDeferred", err)
	}
	storedRun, err := database.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if storedRun.Status != types.RunRunning || storedRun.Error != nil {
		t.Fatalf("deferred run = %+v, want active without error", storedRun)
	}
	steps, err := database.GetStepsByRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 1 || steps[0].Status != types.StepStatusRunning {
		t.Fatalf("deferred steps = %+v, want one running CI step", steps)
	}
}
