package pipeline

import (
	"errors"
	"strings"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/db"
)

type lifecycleQualityRecorder struct {
	outcomes []db.QualityOutcome
	err      error
}

func (r *lifecycleQualityRecorder) InsertQualityOutcome(out db.QualityOutcome) (*db.QualityOutcome, error) {
	if r.err != nil {
		return nil, r.err
	}
	r.outcomes = append(r.outcomes, out)
	return &out, nil
}

func TestQualityLifecycleOutcomesRequireExplicitLifecycleCalls(t *testing.T) {
	recorder := &lifecycleQualityRecorder{}
	binding := QualityLifecycleBinding{
		RunID:            "run-1",
		RootID:           "public-contract",
		FixedHeadSHA:     strings.Repeat("1", 40),
		ObservedHeadSHA:  strings.Repeat("2", 40),
		LifecycleEventID: "owner-decision-1",
		SupersedesID:     stringPointer("prior-quality-outcome"),
	}

	if len(recorder.outcomes) != 0 {
		t.Fatal("constructing a lifecycle binding must not fabricate an outcome")
	}
	if err := RecordQualityOverride(recorder, binding); err != nil {
		t.Fatal(err)
	}
	if len(recorder.outcomes) != 1 || recorder.outcomes[0].Classification != db.QualityOverridden {
		t.Fatalf("override outcomes = %+v", recorder.outcomes)
	}
	if recorder.outcomes[0].SupersedesID == nil || *recorder.outcomes[0].SupersedesID != "prior-quality-outcome" {
		t.Fatalf("override did not retain supersession identity: %+v", recorder.outcomes[0])
	}
	if err := RecordQualityRevert(recorder, binding); err != nil {
		t.Fatal(err)
	}
	if len(recorder.outcomes) != 2 || recorder.outcomes[1].Classification != db.QualityReverted {
		t.Fatalf("revert outcomes = %+v", recorder.outcomes)
	}
}

func stringPointer(value string) *string { return &value }

func TestQualityLifecycleOutcomeFailureIsReturned(t *testing.T) {
	err := RecordQualityOverride(&lifecycleQualityRecorder{err: errors.New("write failed")}, QualityLifecycleBinding{
		RunID:            "run-1",
		FixedHeadSHA:     strings.Repeat("1", 40),
		ObservedHeadSHA:  strings.Repeat("2", 40),
		LifecycleEventID: "owner-decision-1",
	})
	if err == nil || !strings.Contains(err.Error(), "write failed") {
		t.Fatalf("lifecycle recording error = %v", err)
	}
}

func TestQualityLifecycleOutcomeRejectsMissingEventIdentity(t *testing.T) {
	recorder := &lifecycleQualityRecorder{}
	err := RecordQualityRevert(recorder, QualityLifecycleBinding{
		RunID: "run-1", FixedHeadSHA: strings.Repeat("1", 40), ObservedHeadSHA: strings.Repeat("2", 40),
	})
	if err == nil || len(recorder.outcomes) != 0 {
		t.Fatalf("missing lifecycle event identity recorded an outcome: err=%v outcomes=%+v", err, recorder.outcomes)
	}
}
