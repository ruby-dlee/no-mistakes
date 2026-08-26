package daemon

import (
	"context"
	"slices"
	"testing"
)

func TestActiveExecutionRunIDsReportsOnlyOwnedGoroutinesInStableOrder(t *testing.T) {
	mgr := NewRunManager(nil, nil, nil)
	mgr.cancels["run-z"] = context.CancelCauseFunc(func(error) {})
	mgr.cancels["run-a"] = context.CancelCauseFunc(func(error) {})
	mgr.pendingProtected["parked-recovery"] = recoveredRunPlan{}

	if got, want := mgr.ActiveExecutionRunIDs(), []string{"run-a", "run-z"}; !slices.Equal(got, want) {
		t.Fatalf("active executions = %v, want %v", got, want)
	}
}
