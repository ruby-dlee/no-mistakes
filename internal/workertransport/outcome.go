package workertransport

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/db"
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
	Schema                string          `json:"schema"`
	Step                  StepOutcomeStep `json:"step"`
	NeedsApproval         bool            `json:"needs_approval"`
	AutoFixable           bool            `json:"auto_fixable"`
	FindingsJSON          string          `json:"findings_json,omitempty"`
	ExitCode              int             `json:"exit_code"`
	FixSummary            string          `json:"fix_summary,omitempty"`
	ReviewApprovedHeadSHA string          `json:"review_approved_head_sha,omitempty"`
	Skipped               bool            `json:"skipped"`
	SkipRemaining         bool            `json:"skip_remaining"`
}

func decodeStepOutcome(data []byte, kind db.PipelineJobKind, outputHead string) (StepOutcomeEnvelope, error) {
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
	wantStep := StepOutcomeReview
	if kind == db.PipelineJobTest {
		wantStep = StepOutcomeTest
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
	return outcome, nil
}
