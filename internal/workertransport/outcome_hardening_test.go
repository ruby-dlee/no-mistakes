package workertransport

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDecodeStepOutcomeRejectsFalsePassFlags(t *testing.T) {
	head := strings.Repeat("a", 40)
	findings, err := json.Marshal(map[string]any{
		"findings": []map[string]any{{"severity": "error", "description": "remote blocker", "action": "auto-fix"}},
		"summary":  "blocked",
	})
	if err != nil {
		t.Fatal(err)
	}
	outcome := StepOutcomeEnvelope{
		Schema: StepOutcomeSchema, Step: StepOutcomeReview, FindingsJSON: string(findings),
		ReviewApprovedHeadSHA: head,
	}
	data, _ := json.Marshal(outcome)
	if _, err := decodeStepOutcome(data, StepOutcomeReview, head); err == nil {
		t.Fatal("accepted blocking findings with false pass flags")
	}

	outcome.FindingsJSON = ""
	outcome.ExitCode = 1
	data, _ = json.Marshal(outcome)
	if _, err := decodeStepOutcome(data, StepOutcomeReview, head); err == nil {
		t.Fatal("accepted nonzero exit code as a clear outcome")
	}
}

func TestDecodeStepOutcomeRejectsInventedApprovalFlags(t *testing.T) {
	head := strings.Repeat("b", 40)
	findings := `{"findings":[],"summary":"clear"}`
	outcome := StepOutcomeEnvelope{
		Schema: StepOutcomeSchema, Step: StepOutcomeTest, FindingsJSON: findings,
		NeedsApproval: true, AutoFixable: true,
	}
	data, _ := json.Marshal(outcome)
	if _, err := decodeStepOutcome(data, StepOutcomeTest, head); err == nil {
		t.Fatal("accepted approval flags unsupported by findings or exit code")
	}
}

func TestDecodeStepOutcomeRejectsUnknownFindingVocabulary(t *testing.T) {
	head := strings.Repeat("c", 40)
	for name, tc := range map[string]struct {
		finding       map[string]any
		needsApproval bool
	}{
		"severity":              {finding: map[string]any{"severity": "critical", "description": "remote blocker", "action": "ask-user"}},
		"action":                {finding: map[string]any{"severity": "warning", "description": "remote blocker", "action": "escalate"}, needsApproval: true},
		"noncanonical_severity": {finding: map[string]any{"severity": "Warning", "description": "remote blocker", "action": "ask-user"}, needsApproval: true},
		"noncanonical_action":   {finding: map[string]any{"severity": "warning", "description": "remote blocker", "action": "ASK-USER"}, needsApproval: true},
	} {
		t.Run(name, func(t *testing.T) {
			findings, err := json.Marshal(map[string]any{
				"findings": []map[string]any{tc.finding},
				"summary":  "blocked",
			})
			if err != nil {
				t.Fatal(err)
			}
			outcome := StepOutcomeEnvelope{
				Schema: StepOutcomeSchema, Step: StepOutcomeReview, FindingsJSON: string(findings),
				NeedsApproval: tc.needsApproval, AutoFixable: true, ReviewApprovedHeadSHA: head,
			}
			data, _ := json.Marshal(outcome)
			if _, err := decodeStepOutcome(data, StepOutcomeReview, head); err == nil {
				t.Fatalf("accepted unknown finding %s vocabulary", name)
			}
		})
	}
}
