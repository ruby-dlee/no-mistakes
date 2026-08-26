package steps

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/agent"
	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

func completeSemanticRepairResult(summary string) json.RawMessage {
	payload, _ := json.Marshal(map[string]any{
		"summary":                            summary,
		"repair_complete":                    true,
		"semantic_family":                    "product-behavior",
		"semantic_root":                      "public-regression",
		"public_executable_check":            "go test ./internal/widget -run TestPublicRegression",
		"fail_before":                        "TestPublicRegression failed against the pre-fix implementation",
		"pass_after":                         "TestPublicRegression passed after the repair",
		"integration_consumer_compatibility": "go test ./internal/consumer passed",
		"generated_artifacts": map[string]any{
			"touched":           false,
			"source_updated":    false,
			"emitter_available": false,
			"emitter_run":       false,
			"disposition":       "not applicable; no generated artifacts changed",
		},
	})
	return payload
}

func incompleteSemanticRepairResult(summary string) json.RawMessage {
	payload, _ := json.Marshal(map[string]any{
		"summary":                            summary,
		"repair_complete":                    false,
		"semantic_family":                    "generated-artifact",
		"semantic_root":                      "generated-client-contract",
		"public_executable_check":            "not established",
		"fail_before":                        "not established",
		"pass_after":                         "not established",
		"integration_consumer_compatibility": "not established",
		"generated_artifacts": map[string]any{
			"touched":           false,
			"source_updated":    false,
			"emitter_available": false,
			"emitter_run":       false,
			"disposition":       "emitter unavailable; generated output left untouched",
		},
	})
	return payload
}

func TestReviewStep_HighRiskFindingsNeverRemainAutoFixable(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	ag := &mockAgent{
		name: "high-risk-reviewer",
		runFn: func(context.Context, agent.RunOpts) (*agent.Result, error) {
			return &agent.Result{Output: json.RawMessage(`{
				"findings":[{"id":"review-1","severity":"error","file":"feature.txt","line":1,"description":"authorization boundary is ambiguous","action":"auto-fix","review_scope":"source"}],
				"summary":"unsafe without owner decision",
				"risk_level":"high",
				"risk_rationale":"changes an authorization boundary",
				"risk_scope":"source-or-external"
			}`)}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})

	outcome, err := (&ReviewStep{}).Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	findings, err := types.ParseFindingsJSON(outcome.Findings)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings.Items) != 1 || findings.Items[0].Action != types.ActionAskUser {
		t.Fatalf("high-risk finding action = %+v, want ask-user", findings.Items)
	}
	if !outcome.NeedsApproval {
		t.Fatal("high-risk review must park for an owner decision")
	}
	if fixable := types.AutoFixableFindings(findings); len(fixable.Items) != 0 {
		t.Fatalf("high-risk review retained auto-fixable findings: %+v", fixable.Items)
	}
	if !strings.Contains(ag.calls[0].Prompt, "A high-risk review must never classify a finding as auto-fix") {
		t.Fatalf("review prompt missing high-risk action contract:\n%s", ag.calls[0].Prompt)
	}
}

func TestReviewStep_MisScoredSemanticFindingCannotRemainAutoFixable(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	ag := &mockAgent{
		name: "mis-scored-semantic-reviewer",
		runFn: func(context.Context, agent.RunOpts) (*agent.Result, error) {
			return &agent.Result{Output: json.RawMessage(`{
				"findings":[{"id":"review-1","severity":"warning","file":"internal/auth/middleware.go","line":8,"description":"permission check accepts the wrong tenant","action":"auto-fix","review_scope":"source","semantic_family":"auth-permission","semantic_root":"tenant-authorization-boundary"}],
				"summary":"mis-scored authorization defect",
				"risk_level":"medium",
				"risk_rationale":"model incorrectly called it follow-up safe",
				"risk_scope":"source-or-external"
			}`)}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})

	outcome, err := (&ReviewStep{}).Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	findings, err := types.ParseFindingsJSON(outcome.Findings)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings.Items) != 1 || findings.Items[0].Action != types.ActionAskUser {
		t.Fatalf("mis-scored semantic action = %+v, want ask-user", findings.Items)
	}
	if findings.Items[0].SemanticFamily != "auth-permission" || findings.Items[0].SemanticRoot != "tenant-authorization-boundary" {
		t.Fatalf("semantic identity was not preserved: %+v", findings.Items[0])
	}
	if !outcome.NeedsApproval {
		t.Fatal("mis-scored semantic risk must park")
	}
}

func TestReviewStep_FixerGetsCompleteSemanticContextAndProofContract(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	gitCmd(t, dir, "checkout", "--detach", headSHA)

	call := 0
	ag := &mockAgent{
		name: "semantic-fixer",
		runFn: func(_ context.Context, opts agent.RunOpts) (*agent.Result, error) {
			call++
			if call == 1 {
				if err := os.WriteFile(filepath.Join(dir, "semantic-repair.txt"), []byte("fixed"), 0o644); err != nil {
					t.Fatal(err)
				}
				return &agent.Result{Output: completeSemanticRepairResult("repair public behavior")}, nil
			}
			return &agent.Result{Output: json.RawMessage(`{"findings":[],"summary":"clean","risk_level":"low","risk_rationale":"verified","risk_scope":"source-or-external"}`)}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.QualityOutcomes = sctx.DB
	sctx.Fixing = true
	sctx.PreviousFindings = `{"findings":[{"id":"review-1","severity":"error","file":"feature.txt","line":1,"description":"public behavior regressed","action":"auto-fix","review_scope":"source"}],"risk_level":"medium","risk_rationale":"regression","risk_scope":"source-or-external"}`
	sctx.UserIntent = "Preserve the public widget behavior and its generated client contract."
	sctx.IntentSource = db.RunIntentSourceAgent
	sctx.Config.Review.PathInstructions = []config.PathInstruction{{
		Path:         "*.txt",
		Instructions: "The text fixture is consumed by the widget integration; preserve its normalized contract.",
	}}
	priorFindings := `{"findings":[{"id":"old-1","severity":"warning","file":"feature.txt","description":"previous rejected approach","action":"ask-user"}]}`
	priorSummary := "attempted prior semantic patch"
	sctx.UncertifiedPriorRounds = []*db.StepRound{{Round: 4, Trigger: "auto_fix", FindingsJSON: &priorFindings, FixSummary: &priorSummary}}

	outcome, err := (&ReviewStep{}).Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.NeedsApproval {
		t.Fatalf("complete proven repair unexpectedly parked: %s", outcome.Findings)
	}
	quality, err := sctx.DB.GetQualityOutcomesByRun(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(quality) != 1 || quality[0].Classification != db.QualityCleanFix ||
		quality[0].FixedHeadSHA != outcome.ReviewApprovedHeadSHA || quality[0].ObservedHeadSHA != outcome.ReviewApprovedHeadSHA {
		t.Fatalf("automatic semantic quality observation = %+v, outcome=%+v", quality, outcome)
	}
	for _, recorded := range quality {
		if recorded.Classification == db.QualityOverridden || recorded.Classification == db.QualityReverted {
			t.Fatalf("review loop fabricated lifecycle classification: %+v", recorded)
		}
	}
	if len(ag.calls) != 2 {
		t.Fatalf("calls = %d, want fix plus cold rereview", len(ag.calls))
	}
	fixPrompt := ag.calls[0].Prompt
	for _, want := range []string{
		"AUTHORITATIVE acceptance criteria",
		"Preserve the public widget behavior",
		"Previous run (uncertified fixer commits)",
		"previous rejected approach",
		config.ReviewPathInstructionsHeading,
		"consumed by the widget integration",
		"trace the affected public interface through contract owners, callers, consumers, and integration boundaries",
		"Do not hand-edit generated output",
		"If its emitter is unavailable, leave the generated artifact untouched",
		"fail against the pre-fix behavior and pass after the repair",
	} {
		if !strings.Contains(fixPrompt, want) {
			t.Errorf("fixer prompt missing semantic context %q:\n%s", want, fixPrompt)
		}
	}
	for _, field := range []string{
		"semantic_family",
		"semantic_root",
		"repair_complete",
		"public_executable_check",
		"fail_before",
		"pass_after",
		"integration_consumer_compatibility",
		"generated_artifacts",
	} {
		if !strings.Contains(string(ag.calls[0].JSONSchema), field) {
			t.Errorf("review repair schema missing %q: %s", field, ag.calls[0].JSONSchema)
		}
	}
	rereviewPrompt := ag.calls[1].Prompt
	for _, want := range []string{
		"Semantic-repair proof (fixer claims, not independent evidence)",
		"TestPublicRegression failed against the pre-fix implementation",
		"go test ./internal/consumer passed",
		"Compare these claims against the current diff and executable regression",
	} {
		if !strings.Contains(rereviewPrompt, want) {
			t.Errorf("rereview prompt missing repair proof %q:\n%s", want, rereviewPrompt)
		}
	}
}

func TestSemanticRepairSameFamilyDifferentFileParksAfterFreshAttempt(t *testing.T) {
	previous := `{"findings":[{"id":"old","severity":"error","file":"parser/input.go","description":"audience parser drops exclusions","action":"auto-fix","semantic_family":"parser-serialization","semantic_root":"effective-audience"}]}`
	findings := Findings{Items: []Finding{{
		ID:             "new",
		Severity:       types.FindingSeverityError,
		File:           "api/audience.go",
		Description:    "the decoded audience still drops exclusions",
		Action:         types.ActionAutoFix,
		SemanticFamily: "parser-serialization",
		SemanticRoot:   "effective-audience",
	}}}
	evidence := semanticRepairEvidence{RepairComplete: true}

	got := enforceSemanticRepairHandoff(findings, previous, 2, evidence, true)
	if got.Items[0].Action != types.ActionAskUser {
		t.Fatalf("same semantic family moved files and remained %q", got.Items[0].Action)
	}
	if !strings.Contains(got.Items[0].Description, "behavioral family") {
		t.Fatalf("same-family handoff lacks explanation: %q", got.Items[0].Description)
	}
}

func TestReviewStep_IncompleteSemanticRepairProofParksEvenIfReviewerReturnsClean(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	gitCmd(t, dir, "checkout", "--detach", headSHA)

	call := 0
	ag := &mockAgent{
		name: "incomplete-semantic-fixer",
		runFn: func(context.Context, agent.RunOpts) (*agent.Result, error) {
			call++
			if call == 1 {
				return &agent.Result{Output: incompleteSemanticRepairResult("cannot prove repair")}, nil
			}
			return &agent.Result{Output: json.RawMessage(`{"findings":[],"summary":"clean","risk_level":"low","risk_rationale":"no visible defect","risk_scope":"source-or-external"}`)}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.QualityOutcomes = sctx.DB
	sctx.Fixing = true
	sctx.PreviousFindings = `{"findings":[{"id":"review-1","severity":"error","file":"feature.txt","description":"generated client is stale","action":"auto-fix","review_scope":"source"}]}`

	outcome, err := (&ReviewStep{}).Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	findings, err := types.ParseFindingsJSON(outcome.Findings)
	if err != nil {
		t.Fatal(err)
	}
	if !outcome.NeedsApproval || len(findings.Items) != 1 || findings.Items[0].Action != types.ActionAskUser {
		t.Fatalf("incomplete semantic proof did not park: outcome=%+v findings=%+v", outcome, findings.Items)
	}
	if !strings.Contains(findings.Items[0].Description, "could not prove the semantic repair") {
		t.Fatalf("handoff finding did not explain missing proof: %+v", findings.Items[0])
	}
	quality, err := sctx.DB.GetQualityOutcomesByRun(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(quality) != 1 || quality[0].Classification != db.QualityPrimaryHandoff {
		t.Fatalf("incomplete-proof quality observation = %+v", quality)
	}
}

func TestReviewStep_QualityOutcomeWriteFailureBlocksCleanRepair(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	gitCmd(t, dir, "checkout", "--detach", headSHA)

	call := 0
	ag := &mockAgent{
		name: "semantic-quality-failure",
		runFn: func(context.Context, agent.RunOpts) (*agent.Result, error) {
			call++
			if call == 1 {
				return &agent.Result{Output: completeSemanticRepairResult("repair public behavior")}, nil
			}
			return &agent.Result{Output: json.RawMessage(`{"findings":[],"summary":"clean","risk_level":"low","risk_rationale":"verified","risk_scope":"source-or-external"}`)}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.QualityOutcomes = &qualityOutcomeRecorderStub{err: errors.New("quality store unavailable")}
	sctx.Fixing = true
	sctx.PreviousFindings = `{"findings":[{"id":"review-1","severity":"error","file":"feature.txt","description":"public behavior regressed","action":"auto-fix","semantic_family":"product-behavior","semantic_root":"public-regression"}]}`

	_, err := (&ReviewStep{}).Execute(sctx)
	if err == nil || !strings.Contains(err.Error(), "record semantic repair quality outcome") || !strings.Contains(err.Error(), "quality store unavailable") {
		t.Fatalf("review error = %v, want fail-closed quality recording error", err)
	}
}

func TestSemanticRepairGeneratedDispositionRejectsHandEditWithoutEmitter(t *testing.T) {
	evidence := semanticRepairEvidence{
		Summary:                          "repair generated artifact",
		RepairComplete:                   true,
		SemanticFamily:                   "generated-artifact",
		SemanticRoot:                     "generated-client-contract",
		PublicExecutableCheck:            "public test",
		FailBefore:                       "failed before",
		PassAfter:                        "passed after",
		IntegrationConsumerCompatibility: "consumer passed",
		GeneratedArtifacts: generatedArtifactDisposition{
			Touched:          true,
			SourceUpdated:    false,
			EmitterAvailable: false,
			EmitterRun:       false,
			Disposition:      "hand-edited generated output",
		},
	}
	if err := evidence.validate(); err == nil || !strings.Contains(err.Error(), "generated artifacts") {
		t.Fatalf("generated hand edit validation error = %v, want generated-artifact rejection", err)
	}
}

func TestSemanticRepairCompleteProofRejectsUnobservedPlaceholders(t *testing.T) {
	evidence := semanticRepairEvidence{
		Summary:                          "claim an unproven repair",
		RepairComplete:                   true,
		SemanticFamily:                   "product-behavior",
		SemanticRoot:                     "public-contract",
		PublicExecutableCheck:            "go test ./public -run TestContract",
		FailBefore:                       "not run",
		PassAfter:                        "TestContract passed",
		IntegrationConsumerCompatibility: "consumer passed",
		GeneratedArtifacts: generatedArtifactDisposition{
			Disposition: "not applicable; no generated artifacts changed",
		},
	}
	if err := evidence.validate(); err == nil || !strings.Contains(err.Error(), "observed result") {
		t.Fatalf("placeholder proof validation error = %v, want observed-result rejection", err)
	}
}
