package workertransport

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/types"
)

func validStepInputBytes(t *testing.T) []byte {
	t.Helper()
	data, err := json.Marshal(StepInputEnvelope{
		Schema: StepInputSchema, RunID: "run-1", RepoID: "repo-1", StepResultID: "step-1",
		Step: types.StepReview, Round: 1,
		DesiredHeadSHA: strings.Repeat("a", 40), BaseSHA: strings.Repeat("b", 40),
		Branch: "feature", DefaultBranch: "main",
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestDecodeStepInputStrictAndRoleBounded(t *testing.T) {
	valid := validStepInputBytes(t)
	if _, err := DecodeStepInput(valid); err != nil {
		t.Fatalf("valid input: %v", err)
	}
	unknown := append(valid[:len(valid)-1], []byte(`,"unexpected":true}`)...)
	if _, err := DecodeStepInput(unknown); err == nil {
		t.Fatal("accepted unknown field")
	}
	if _, err := DecodeStepInput(append(valid, []byte(` {}`)...)); err == nil {
		t.Fatal("accepted trailing object")
	}
}

func TestDecodeStepInputRequiresFindingsOnlyForRepair(t *testing.T) {
	var input StepInputEnvelope
	if err := json.Unmarshal(validStepInputBytes(t), &input); err != nil {
		t.Fatal(err)
	}
	input.Fixing = true
	data, _ := json.Marshal(input)
	if _, err := DecodeStepInput(data); err == nil {
		t.Fatal("accepted repair without findings")
	}
	input.PreviousFindings = `{"findings":[]}`
	data, _ = json.Marshal(input)
	if _, err := DecodeStepInput(data); err != nil {
		t.Fatalf("repair with findings: %v", err)
	}
}
