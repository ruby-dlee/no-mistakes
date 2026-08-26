package workertransport

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/kunchenguid/no-mistakes/internal/types"
)

const StepInputSchema = "no-mistakes.azure-worker-step-input/v1"

// StepInputEnvelope is the exact, content-bounded semantic brief shared by the
// controller and the standalone guest worker.
type StepInputEnvelope struct {
	Schema                  string         `json:"schema"`
	RunID                   string         `json:"run_id"`
	RepoID                  string         `json:"repo_id"`
	StepResultID            string         `json:"step_result_id"`
	Step                    types.StepName `json:"step"`
	Round                   int            `json:"round"`
	DesiredHeadSHA          string         `json:"desired_head_sha"`
	BaseSHA                 string         `json:"base_sha"`
	Branch                  string         `json:"branch"`
	DefaultBranch           string         `json:"default_branch"`
	Fixing                  bool           `json:"fixing"`
	PreviousFindings        string         `json:"previous_findings_json,omitempty"`
	UserIntent              string         `json:"user_intent,omitempty"`
	UserIntentSource        string         `json:"user_intent_source,omitempty"`
	PriorRoundHistory       string         `json:"prior_round_history,omitempty"`
	UncertifiedRoundHistory string         `json:"uncertified_round_history,omitempty"`
	RepairAttempt           int            `json:"repair_attempt,omitempty"`
	QualityOutcomeAuthority string         `json:"quality_outcome_authority,omitempty"`
}

func DecodeStepInput(data []byte) (StepInputEnvelope, error) {
	var input StepInputEnvelope
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return input, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return input, errors.New("worker step input has trailing data")
	}
	if input.Schema != StepInputSchema {
		return input, fmt.Errorf("worker step input schema is %q", input.Schema)
	}
	if input.Step != types.StepReview && input.Step != types.StepTest {
		return input, fmt.Errorf("unsupported worker step %q", input.Step)
	}
	if input.Round < 1 || input.Round > 100 {
		return input, errors.New("worker step round is outside 1..100")
	}
	for name, value := range map[string]string{
		"run_id": input.RunID, "repo_id": input.RepoID, "step_result_id": input.StepResultID,
		"branch": input.Branch, "default_branch": input.DefaultBranch,
	} {
		if strings.TrimSpace(value) == "" || len(value) > 256 || strings.ContainsAny(value, "\r\n\x00") {
			return input, fmt.Errorf("worker step %s is empty, oversized, or multiline", name)
		}
	}
	for name, value := range map[string]string{"desired_head_sha": input.DesiredHeadSHA, "base_sha": input.BaseSHA} {
		if len(value) != 40 || strings.Trim(value, "0123456789abcdef") != "" {
			return input, fmt.Errorf("worker step %s is not a lowercase full SHA-1", name)
		}
	}
	if len(input.PreviousFindings) > maxFindingsBytes || (input.PreviousFindings != "" && !json.Valid([]byte(input.PreviousFindings))) {
		return input, errors.New("worker previous findings are oversized or invalid JSON")
	}
	if input.Fixing && input.PreviousFindings == "" {
		return input, errors.New("worker repair is missing previous findings")
	}
	if !input.Fixing && input.PreviousFindings != "" {
		return input, errors.New("read-only worker step unexpectedly carries previous findings")
	}
	if len(input.UserIntent) > 16<<10 || len(input.UserIntentSource) > 128 || strings.IndexByte(input.UserIntent, 0) >= 0 || strings.ContainsAny(input.UserIntentSource, "\r\n\x00") {
		return input, errors.New("worker intent is oversized or binary")
	}
	for name, value := range map[string]string{
		"prior_round_history":       input.PriorRoundHistory,
		"uncertified_round_history": input.UncertifiedRoundHistory,
	} {
		if len(value) > 48<<10 || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
			return input, fmt.Errorf("worker %s is oversized or invalid UTF-8", name)
		}
	}
	if input.RepairAttempt < 0 || input.RepairAttempt > 10 {
		return input, errors.New("worker repair attempt is outside 0..10")
	}
	qualityExpected := input.Fixing && input.Step == types.StepReview
	if qualityExpected {
		if input.RepairAttempt < 1 || input.QualityOutcomeAuthority != "semantic-rereview" {
			return input, errors.New("review repair is missing recurrence or quality-outcome authority")
		}
	} else if input.RepairAttempt != 0 || input.QualityOutcomeAuthority != "" {
		return input, errors.New("non-review-repair input asserted recurrence or quality-outcome authority")
	}
	return input, nil
}
