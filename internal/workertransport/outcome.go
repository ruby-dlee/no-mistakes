package workertransport

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/types"
)

const (
	StepOutcomeSchema  = "no-mistakes.worker-step-outcome/v1"
	maxFindingsBytes   = 512 << 10
	maxFixSummaryBytes = 256
)

type StepOutcomeStep string

const (
	StepOutcomeReview StepOutcomeStep = "review"
	StepOutcomeTest   StepOutcomeStep = "test"
)

// StepOutcomeEnvelope is the bounded semantic result produced by the guest
// worker. It deliberately excludes prompts, command output, diffs, credentials,
// PR mutations, and guest-selected timing. ResultEnvelope binds these bytes by
// SHA-256 before the controller admits any pipeline outcome.
type StepOutcomeEnvelope struct {
	Schema                string                  `json:"schema"`
	Step                  StepOutcomeStep         `json:"step"`
	NeedsApproval         bool                    `json:"needs_approval"`
	AutoFixable           bool                    `json:"auto_fixable"`
	FindingsJSON          string                  `json:"findings_json,omitempty"`
	ExitCode              int                     `json:"exit_code"`
	FixSummary            string                  `json:"fix_summary,omitempty"`
	ReviewApprovedHeadSHA string                  `json:"review_approved_head_sha,omitempty"`
	Skipped               bool                    `json:"skipped"`
	SkipRemaining         bool                    `json:"skip_remaining"`
	QualityOutcome        *QualityOutcomeEnvelope `json:"quality_outcome,omitempty"`
}

// QualityOutcomeEnvelope is the content-free semantic-rereview observation
// a controller-authorized review repair may return. The controller supplies
// run/job custody and performs the durable append.
type QualityOutcomeEnvelope struct {
	FixAttemptID       string `json:"fix_attempt_id"`
	RootID             string `json:"root_id,omitempty"`
	Classification     string `json:"classification"`
	FixedHeadSHA       string `json:"fixed_head_sha"`
	ObservedHeadSHA    string `json:"observed_head_sha"`
	EvidenceDigest     string `json:"evidence_digest"`
	EvidenceProvenance string `json:"evidence_provenance"`
}

func decodeStepOutcome(data []byte, wantStep StepOutcomeStep, outputHead string) (StepOutcomeEnvelope, error) {
	var outcome StepOutcomeEnvelope
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&outcome); err != nil {
		return outcome, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return outcome, errors.New("worker step outcome has trailing data")
	}
	if outcome.Schema != StepOutcomeSchema {
		return outcome, fmt.Errorf("worker step outcome schema is %q", outcome.Schema)
	}
	if wantStep != StepOutcomeReview && wantStep != StepOutcomeTest {
		return outcome, fmt.Errorf("unsupported worker step outcome binding %q", wantStep)
	}
	if outcome.Step != wantStep {
		return outcome, fmt.Errorf("worker step outcome is for %q, want %q", outcome.Step, wantStep)
	}
	if outcome.ExitCode < 0 || outcome.ExitCode > 255 {
		return outcome, errors.New("worker step outcome exit code is outside 0..255")
	}
	if len(outcome.FindingsJSON) > maxFindingsBytes || strings.IndexByte(outcome.FindingsJSON, 0) >= 0 {
		return outcome, errors.New("worker step findings are oversized or binary")
	}
	if outcome.FindingsJSON != "" && !json.Valid([]byte(outcome.FindingsJSON)) {
		return outcome, errors.New("worker step findings are not valid JSON")
	}
	if (outcome.NeedsApproval || outcome.AutoFixable) && outcome.FindingsJSON == "" {
		return outcome, errors.New("blocking or auto-fixable worker outcome is missing findings")
	}
	wantApproval, wantAutoFixable, err := derivedStepOutcomeFlags(outcome)
	if err != nil {
		return outcome, err
	}
	if outcome.NeedsApproval != wantApproval || outcome.AutoFixable != wantAutoFixable {
		return outcome, fmt.Errorf("worker step outcome flags are inconsistent with findings and exit code: approval=%t auto_fixable=%t, want %t/%t",
			outcome.NeedsApproval, outcome.AutoFixable, wantApproval, wantAutoFixable)
	}
	if len(outcome.FixSummary) > maxFixSummaryBytes || strings.ContainsAny(outcome.FixSummary, "\r\n\x00") {
		return outcome, errors.New("worker fix summary is oversized or multiline")
	}
	if wantStep == StepOutcomeReview {
		if outcome.ReviewApprovedHeadSHA != outputHead {
			return outcome, errors.New("worker review approval is not bound to the output head")
		}
	} else if outcome.ReviewApprovedHeadSHA != "" {
		return outcome, errors.New("non-review worker outcome asserted review approval")
	}
	if outcome.QualityOutcome != nil {
		if wantStep != StepOutcomeReview {
			return outcome, errors.New("non-review worker outcome asserted semantic quality authority")
		}
		if err := validateQualityOutcomeEnvelope(*outcome.QualityOutcome, outputHead); err != nil {
			return outcome, err
		}
	}
	return outcome, nil
}

func validateQualityOutcomeEnvelope(outcome QualityOutcomeEnvelope, outputHead string) error {
	switch outcome.Classification {
	case "clean_fix", "same_root_followup", "introduced_regression", "primary_handoff":
	default:
		return fmt.Errorf("worker semantic quality classification is unsupported: %q", outcome.Classification)
	}
	if !strings.HasPrefix(outcome.FixAttemptID, "review-fix-") || len(outcome.FixAttemptID) > 64 || strings.ContainsAny(outcome.FixAttemptID, "\r\n\x00") {
		return errors.New("worker semantic quality fix attempt is invalid")
	}
	if len(outcome.RootID) > 128 || strings.ContainsAny(outcome.RootID, "\r\n\x00") {
		return errors.New("worker semantic quality root is invalid")
	}
	if outcome.FixedHeadSHA != outputHead || outcome.ObservedHeadSHA != outputHead {
		return errors.New("worker semantic quality observation is not bound to the output head")
	}
	if outcome.EvidenceProvenance != "semantic_rereview" || len(outcome.EvidenceDigest) != len("sha256:")+64 || !strings.HasPrefix(outcome.EvidenceDigest, "sha256:") || strings.Trim(strings.TrimPrefix(outcome.EvidenceDigest, "sha256:"), "0123456789abcdef") != "" {
		return errors.New("worker semantic quality evidence binding is invalid")
	}
	return nil
}

func derivedStepOutcomeFlags(outcome StepOutcomeEnvelope) (bool, bool, error) {
	items := []types.Finding(nil)
	if outcome.FindingsJSON != "" {
		findings, err := types.ParseFindingsJSON(outcome.FindingsJSON)
		if err != nil {
			return false, false, fmt.Errorf("worker step findings do not match the findings contract: %w", err)
		}
		items = findings.Items
	}
	blocking := outcome.ExitCode != 0
	for _, item := range items {
		severity := types.NormalizeFindingSeverity(item.Severity)
		if !types.IsKnownFindingSeverity(item.Severity) || item.Severity != severity {
			return false, false, fmt.Errorf("worker step finding severity is unsupported: %q", item.Severity)
		}
		action := types.NormalizeFindingAction(item.Action)
		if !types.IsKnownFindingAction(item.Action) || item.Action != action {
			return false, false, fmt.Errorf("worker step finding action is unsupported: %q", item.Action)
		}
		if severity == types.FindingSeverityError || severity == types.FindingSeverityWarning {
			blocking = true
		}
	}
	autoFixable := blocking
	if outcome.Step == StepOutcomeReview {
		autoFixable = len(items) > 0
	}
	return blocking, autoFixable, nil
}

// ValidateStepOutcome applies the same closed result contract used by the
// controller to an in-memory guest outcome before it is materialized.
func ValidateStepOutcome(outcome StepOutcomeEnvelope, wantStep StepOutcomeStep, outputHead string) error {
	data, err := json.Marshal(outcome)
	if err != nil {
		return err
	}
	_, err = decodeStepOutcome(data, wantStep, outputHead)
	return err
}
