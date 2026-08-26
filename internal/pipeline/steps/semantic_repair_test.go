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
		"summary":                    summary,
		"repair_complete":            true,
		"semantic_family":            "product-behavior",
		"semantic_root":              "public-regression",
		"public_executable_check":    "go test ./internal/widget -run TestPublicRegression",
		"integration_consumer_check": "go test ./internal/consumer -run TestCompatibility",
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
		"summary":                    summary,
		"repair_complete":            false,
		"semantic_family":            "generated-artifact",
		"semantic_root":              "generated-client-contract",
		"public_executable_check":    "not established",
		"integration_consumer_check": "not established",
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
		"pipeline will execute that command against both the pre-fix commit and repaired commit",
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
		"integration_consumer_check",
		"generated_artifacts",
	} {
		if !strings.Contains(string(ag.calls[0].JSONSchema), field) {
			t.Errorf("review repair schema missing %q: %s", field, ag.calls[0].JSONSchema)
		}
	}
	rereviewPrompt := ag.calls[1].Prompt
	for _, want := range []string{
		"Semantic-repair proof (executor-owned targeted evidence)",
		"proof_status: verified",
		"fail_before: head=",
		"pass_after: head=",
		"integration_after: head=",
		"The pipeline executed these exact targeted checks outside the fixer session",
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

func TestReviewStep_RevertedRepairStillGetsColdRereviewAndIntentGate(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	gitCmd(t, dir, "checkout", "--detach", headSHA)

	call := 0
	ag := &mockAgent{
		name: "reverting-semantic-fixer",
		runFn: func(_ context.Context, opts agent.RunOpts) (*agent.Result, error) {
			call++
			if call == 1 {
				if err := os.Remove(filepath.Join(dir, "feature.txt")); err != nil {
					return nil, err
				}
				return &agent.Result{Output: completeSemanticRepairResult("remove required behavior")}, nil
			}
			if opts.Session != nil {
				t.Fatalf("independent rereview resumed a fixer session: %+v", opts.Session)
			}
			if !strings.Contains(opts.Prompt, "Intent conformance (required)") {
				t.Fatalf("reverted repair rereview lost intent conformance:\n%s", opts.Prompt)
			}
			return &agent.Result{Output: json.RawMessage(`{
				"findings":[{"id":"required-behavior-removed","severity":"error","file":"feature.txt","description":"the repair removed intent-required behavior","action":"ask-user","review_scope":"source","semantic_family":"product-behavior","semantic_root":"required-feature"}],
				"risk_level":"high","risk_rationale":"required behavior was removed","risk_scope":"source-or-external"
			}`)}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.SemanticProofRunner = nil
	sctx.Fixing = true
	sctx.UserIntent = "REQUIRED: preserve feature.txt behavior."
	sctx.IntentSource = db.RunIntentSourceAgent
	sctx.PreviousFindings = `{"findings":[{"id":"repair","severity":"error","file":"feature.txt","description":"repair behavior","action":"ask-user","review_scope":"source","semantic_family":"product-behavior","semantic_root":"required-feature"}]}`

	outcome, err := (&ReviewStep{}).Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if call != 2 {
		t.Fatalf("agent calls = %d, want fixer plus cold rereview after empty diff", call)
	}
	if !outcome.NeedsApproval || !hasAskUserFindings(t, outcome.Findings) {
		t.Fatalf("reverted repair bypassed intent gate: %+v", outcome)
	}
}

func TestReviewStep_SemanticProofRequiresExecutorEvidence(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	gitCmd(t, dir, "checkout", "--detach", headSHA)

	call := 0
	ag := &mockAgent{
		name: "unverified-semantic-fixer",
		runFn: func(context.Context, agent.RunOpts) (*agent.Result, error) {
			call++
			if call == 1 {
				if err := os.WriteFile(filepath.Join(dir, "semantic-repair.txt"), []byte("claimed\n"), 0o644); err != nil {
					return nil, err
				}
				// The shared fixture names commands that do not exist in this repo.
				// Agent-authored prose must not turn those claims into proof.
				return &agent.Result{Output: completeSemanticRepairResult("claim unexecuted proof")}, nil
			}
			return &agent.Result{Output: json.RawMessage(`{"findings":[],"risk_level":"low","risk_rationale":"agent claims look plausible","risk_scope":"source-or-external"}`)}, nil
		},
	}
	sctx := newTestContextWithDBRecords(t, ag, dir, baseSHA, headSHA, config.Commands{})
	sctx.SemanticProofRunner = nil
	sctx.Fixing = true
	sctx.PreviousFindings = `{"findings":[{"id":"repair","severity":"error","file":"feature.txt","description":"public behavior regressed","action":"ask-user","review_scope":"source","semantic_family":"product-behavior","semantic_root":"public-regression"}]}`

	outcome, err := (&ReviewStep{}).Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if call != 2 {
		t.Fatalf("agent calls = %d, want fixer plus independent rereview", call)
	}
	if !outcome.NeedsApproval || !hasAskUserFindings(t, outcome.Findings) {
		t.Fatalf("unexecuted semantic proof was accepted: %+v", outcome)
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
		Summary:                  "repair generated artifact",
		RepairComplete:           true,
		SemanticFamily:           "generated-artifact",
		SemanticRoot:             "generated-client-contract",
		PublicExecutableCheck:    "public test",
		IntegrationConsumerCheck: "consumer test",
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
		Summary:                  "claim an unproven repair",
		RepairComplete:           true,
		SemanticFamily:           "product-behavior",
		SemanticRoot:             "public-contract",
		PublicExecutableCheck:    "go test ./public -run TestContract",
		IntegrationConsumerCheck: "not run",
		GeneratedArtifacts: generatedArtifactDisposition{
			Disposition: "not applicable; no generated artifacts changed",
		},
	}
	if err := evidence.validate(); err == nil || !strings.Contains(err.Error(), "executable check") {
		t.Fatalf("placeholder proof validation error = %v, want executable-check rejection", err)
	}
}

func TestGeneratedArtifactDiffRejectsOutputOnlyChangeDespiteFixerClaim(t *testing.T) {
	t.Parallel()
	dir, _, startingHead := setupGitRepo(t)
	gitCmd(t, dir, "checkout", "--detach", startingHead)
	if err := os.WriteFile(filepath.Join(dir, "client.generated.go"), []byte("// Code generated by emitter. DO NOT EDIT.\npackage client\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, dir, "add", "client.generated.go")
	gitCmd(t, dir, "commit", "-m", "hand edit generated client")
	fixedHead := gitCmd(t, dir, "rev-parse", "HEAD")
	evidence := semanticRepairEvidence{GeneratedArtifacts: generatedArtifactDisposition{
		Touched: true, SourceUpdated: true, EmitterAvailable: true, EmitterRun: true,
		Disposition: "source and emitter updated",
	}}
	if err := validateGeneratedArtifactDiff(context.Background(), dir, startingHead, fixedHead, evidence); err == nil {
		t.Fatal("accepted generated-output-only diff based on fixer self-report")
	}
}

func TestGeneratedArtifactDiffHonorsAuthoritativeGitAttributes(t *testing.T) {
	t.Parallel()
	dir, _, startingHead := setupGitRepo(t)
	gitCmd(t, dir, "checkout", "--detach", startingHead)
	if err := os.MkdirAll(filepath.Join(dir, "contracts"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".gitattributes"), []byte("contracts/client.ts linguist-generated=true\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "contracts", "client.ts"), []byte("export const version = 1;\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, dir, "add", ".gitattributes", "contracts/client.ts")
	gitCmd(t, dir, "commit", "-m", "declare generated client")
	startingHead = gitCmd(t, dir, "rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(dir, "contracts", "client.ts"), []byte("export const version = 2;\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, dir, "add", "contracts/client.ts")
	gitCmd(t, dir, "commit", "-m", "hand edit ordinary named client")
	fixedHead := gitCmd(t, dir, "rev-parse", "HEAD")
	evidence := semanticRepairEvidence{GeneratedArtifacts: generatedArtifactDisposition{
		Touched: false, Disposition: "no generated artifacts changed",
	}}
	if err := validateGeneratedArtifactDiff(context.Background(), dir, startingHead, fixedHead, evidence); err == nil {
		t.Fatal("accepted an ordinary-named generated output hidden by fixer self-report")
	}
}

func TestNormalizeSemanticRiskActionsCatchesMechanicalLabelEvasion(t *testing.T) {
	findings := Findings{
		RiskLevel: "medium", RiskRationale: "authorization boundary is affected",
		Items: []Finding{{
			Severity: "warning", Action: types.ActionAutoFix, SemanticFamily: "local-mechanical",
			SemanticRoot: "role-check", Description: "configured role check is incomplete",
		}},
	}
	got := normalizeSemanticRiskReviewActions(findings)
	if got.Items[0].Action != types.ActionAskUser {
		t.Fatalf("mechanically mislabeled auth defect action = %q, want ask-user", got.Items[0].Action)
	}
}
