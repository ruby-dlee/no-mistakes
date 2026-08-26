package steps

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/agent"
	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

type generatedArtifactDisposition struct {
	Touched          bool   `json:"touched"`
	SourceUpdated    bool   `json:"source_updated"`
	EmitterAvailable bool   `json:"emitter_available"`
	EmitterRun       bool   `json:"emitter_run"`
	Disposition      string `json:"disposition"`
}

type semanticRepairEvidence struct {
	Summary                          string                       `json:"summary"`
	RepairComplete                   bool                         `json:"repair_complete"`
	SemanticFamily                   string                       `json:"semantic_family"`
	SemanticRoot                     string                       `json:"semantic_root"`
	PublicExecutableCheck            string                       `json:"public_executable_check"`
	FailBefore                       string                       `json:"fail_before"`
	PassAfter                        string                       `json:"pass_after"`
	IntegrationConsumerCompatibility string                       `json:"integration_consumer_compatibility"`
	GeneratedArtifacts               generatedArtifactDisposition `json:"generated_artifacts"`
}

var semanticRepairSchema = json.RawMessage(fmt.Sprintf(`{
	"type": "object",
	"properties": {
		"summary": {"type": "string", "maxLength": %d},
		"repair_complete": {"type": "boolean"},
		"semantic_family": {"type": "string", "enum": ["local-mechanical", "contract-schema", "parser-serialization", "auth-permission", "security-safety", "deploy-routing", "generated-artifact", "product-behavior", "integration-compatibility"]},
		"semantic_root": {"type": "string", "minLength": 1},
		"public_executable_check": {"type": "string", "minLength": 1},
		"fail_before": {"type": "string", "minLength": 1},
		"pass_after": {"type": "string", "minLength": 1},
		"integration_consumer_compatibility": {"type": "string", "minLength": 1},
		"generated_artifacts": {
			"type": "object",
			"properties": {
				"touched": {"type": "boolean"},
				"source_updated": {"type": "boolean"},
				"emitter_available": {"type": "boolean"},
				"emitter_run": {"type": "boolean"},
				"disposition": {"type": "string", "minLength": 1}
			},
			"required": ["touched", "source_updated", "emitter_available", "emitter_run", "disposition"]
		}
	},
	"required": ["summary", "repair_complete", "semantic_family", "semantic_root", "public_executable_check", "fail_before", "pass_after", "integration_consumer_compatibility", "generated_artifacts"]
}`, config.MaxFixMessageSummaryBytes))

const semanticRepairContract = `

## Semantic-repair contract

- Before editing, trace the affected public interface through contract owners, callers, consumers, and integration boundaries. Inspect authoritative intent, repository rules, prior rejected approaches, generated-code ownership, and the relevant call paths; do not treat the reported line as the whole system.
- Add or strengthen a public/executable regression that reproduces the reported behavior. It must fail against the pre-fix behavior and pass after the repair. Source-text matching is not proof.
- At the end, run one bounded verification set covering that regression plus the relevant integration or consumer compatibility boundary. Do not substitute a broad repository suite.
- Identify generated artifacts and their authoritative source and emitter before editing. Do not hand-edit generated output. Update the source and run the emitter. If its emitter is unavailable, leave the generated artifact untouched, set repair_complete=false, and hand the issue back rather than fabricating generated output.
- Return the required structured proof. Set repair_complete=false when any required public/executable fail-before/pass-after or integration/consumer proof is unavailable. Evidence strings must name the exact check and observed result, not a plan or a vague assertion.
`

const semanticRepairReviewerContract = `

Semantic-repair review obligation:
- Compare the fixer's proof claims against the current diff, the executable regression, relevant contract owners/call paths, and integration or consumer boundary. Claims are not evidence.
- A repair is not clear if it lacks a public/executable fail-before/pass-after regression or relevant integration/consumer compatibility evidence. Emit an ask-user finding rather than prescribing another same-context patch.
- Reject generated-output-only edits. Generated artifacts may change only with their authoritative source and an available emitter that was actually run; when the emitter is unavailable, generated output must remain untouched.
`

func parseSemanticRepairEvidence(result *agent.Result) (semanticRepairEvidence, error) {
	var evidence semanticRepairEvidence
	if result == nil || len(result.Output) == 0 {
		return evidence, fmt.Errorf("semantic repair returned no structured proof")
	}
	if err := json.Unmarshal(result.Output, &evidence); err != nil {
		return evidence, fmt.Errorf("parse semantic-repair proof: %w", err)
	}
	if err := evidence.validate(); err != nil {
		return evidence, err
	}
	return evidence, nil
}

func (e semanticRepairEvidence) validate() error {
	required := map[string]string{
		"summary":                            e.Summary,
		"semantic_family":                    e.SemanticFamily,
		"semantic_root":                      e.SemanticRoot,
		"public_executable_check":            e.PublicExecutableCheck,
		"fail_before":                        e.FailBefore,
		"pass_after":                         e.PassAfter,
		"integration_consumer_compatibility": e.IntegrationConsumerCompatibility,
		"generated_artifacts.disposition":    e.GeneratedArtifacts.Disposition,
	}
	for field, value := range required {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("semantic-repair proof field %s is required", field)
		}
	}
	family := normalizeSemanticIdentity(e.SemanticFamily)
	if family != "local-mechanical" && !semanticRiskFamilies[family] {
		return fmt.Errorf("semantic-repair proof field semantic_family has unsupported value %q", e.SemanticFamily)
	}
	if e.RepairComplete {
		for field, value := range map[string]string{
			"public_executable_check":            e.PublicExecutableCheck,
			"fail_before":                        e.FailBefore,
			"pass_after":                         e.PassAfter,
			"integration_consumer_compatibility": e.IntegrationConsumerCompatibility,
		} {
			if semanticProofPlaceholder(value) {
				return fmt.Errorf("semantic-repair proof field %s does not report an observed result", field)
			}
		}
		if strings.EqualFold(strings.TrimSpace(e.FailBefore), strings.TrimSpace(e.PassAfter)) {
			return fmt.Errorf("semantic-repair fail-before and pass-after evidence must report distinct observed results")
		}
	}
	generated := e.GeneratedArtifacts
	if generated.Touched && (!generated.SourceUpdated || !generated.EmitterAvailable || !generated.EmitterRun) {
		return fmt.Errorf("generated artifacts cannot be accepted unless their source was updated and an available emitter was run")
	}
	return nil
}

func semanticProofPlaceholder(value string) bool {
	normalized := strings.ToLower(strings.TrimSpace(value))
	for _, placeholder := range []string{"n/a", "na", "none", "unknown", "not established", "not run", "planned", "todo"} {
		if normalized == placeholder || strings.HasPrefix(normalized, placeholder+":") {
			return true
		}
	}
	return false
}

func semanticRepairEvidenceSection(e semanticRepairEvidence) string {
	generated := e.GeneratedArtifacts
	return fmt.Sprintf(`

Semantic-repair proof (fixer claims, not independent evidence):
- repair_complete: %t
- semantic_family: %s
- semantic_root: %s
- public_executable_check: %s
- fail_before: %s
- pass_after: %s
- integration_consumer_compatibility: %s
- generated_artifacts: touched=%t source_updated=%t emitter_available=%t emitter_run=%t disposition=%s
Compare these claims against the current diff and executable regression; do not accept the summary as proof.`,
		e.RepairComplete,
		sanitizePromptText(e.SemanticFamily),
		sanitizePromptText(e.SemanticRoot),
		sanitizePromptText(e.PublicExecutableCheck),
		sanitizePromptMultilineText(e.FailBefore),
		sanitizePromptMultilineText(e.PassAfter),
		sanitizePromptMultilineText(e.IntegrationConsumerCompatibility),
		generated.Touched,
		generated.SourceUpdated,
		generated.EmitterAvailable,
		generated.EmitterRun,
		sanitizePromptMultilineText(generated.Disposition),
	)
}

func reviewRepairAttempt(sctx *pipeline.StepContext) int {
	attempt := 1
	if sctx == nil || sctx.DB == nil || sctx.StepResultID == "" {
		return attempt
	}
	rounds, err := sctx.DB.GetRoundsByStep(sctx.StepResultID)
	if err != nil {
		return attempt
	}
	for _, round := range rounds {
		if round != nil && round.FixSummary != nil && strings.TrimSpace(*round.FixSummary) != "" {
			attempt++
		}
	}
	return attempt
}

func semanticRepairEscalationSection(attempt int) string {
	if attempt <= 1 {
		return ""
	}
	return fmt.Sprintf(`

Fresh semantic-repair escalation:
- This is semantic repair attempt %d. A prior repair did not clear independent rereview, so do not continue its agent session or assume its diagnosis was correct.
- Start from the current code, full round history, public behavior, and contract boundaries. Reconstruct the failure independently and either produce the required proof or set repair_complete=false for primary-agent handoff.
`, attempt)
}

func normalizeSemanticRiskReviewActions(findings Findings) Findings {
	highRisk := strings.EqualFold(strings.TrimSpace(findings.RiskLevel), "high")
	for i := range findings.Items {
		if findings.Items[i].ActionOrDefault() == types.ActionAutoFix && (highRisk || findingHasSemanticRisk(findings.Items[i])) {
			findings.Items[i].Action = types.ActionAskUser
		}
	}
	return findings
}

var semanticRiskFamilies = map[string]bool{
	"contract-schema":           true,
	"parser-serialization":      true,
	"auth-permission":           true,
	"security-safety":           true,
	"deploy-routing":            true,
	"generated-artifact":        true,
	"product-behavior":          true,
	"integration-compatibility": true,
}

func findingHasSemanticRisk(item Finding) bool {
	family := normalizeSemanticIdentity(item.SemanticFamily)
	if semanticRiskFamilies[family] {
		return true
	}
	// Structured family metadata is the primary policy input. These conservative
	// fallbacks cover legacy/non-schema outputs and a reviewer that incorrectly
	// labels an obviously semantic concern local-mechanical; they are not the sole
	// classifier.
	path := strings.ToLower(strings.TrimSpace(item.File))
	if strings.Contains(path, "generated") || strings.Contains(path, "/gen/") || strings.HasPrefix(path, "gen/") {
		return true
	}
	text := strings.ToLower(strings.Join([]string{item.SemanticRoot, item.Description}, " "))
	for _, marker := range []string{
		"contract", "schema", "serializ", "deserializ", "parser", "parsing",
		"authentication", "authorization", "permission", "tenant boundary",
		"security", "unsafe", "safety", "deploy", "rollout", "routing",
		"generated artifact", "generated output", "emitter", "product behavior",
		"public behavior", "compatibility", "consumer boundary", "integration boundary",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func enforceSemanticRepairHandoff(findings Findings, previousRaw string, attempt int, evidence semanticRepairEvidence, repairExecuted bool) Findings {
	if repairExecuted && !evidence.RepairComplete {
		file := firstFindingFile(previousRaw)
		findings.Items = append(findings.Items, Finding{
			ID:          "review-semantic-repair-proof",
			Severity:    types.FindingSeverityWarning,
			File:        file,
			Description: "The fixer could not prove the semantic repair with a public/executable fail-before/pass-after regression and relevant integration or consumer compatibility evidence; primary-agent handoff is required.",
			Action:      types.ActionAskUser,
			ReviewScope: types.FindingReviewScopeSource,
		})
	}
	if attempt < 2 {
		return findings
	}
	previous := findingSemanticIdentities(previousRaw)
	for i := range findings.Items {
		item := &findings.Items[i]
		if item.ActionOrDefault() != types.ActionAutoFix {
			continue
		}
		file := strings.TrimSpace(item.File)
		family := normalizeSemanticIdentity(item.SemanticFamily)
		root := normalizeSemanticIdentity(item.SemanticRoot)
		repeatedFile := file != "" && previous.files[file]
		repeatedFamily := semanticRiskFamilies[family] && previous.families[family]
		repeatedRoot := root != "" && previous.roots[root]
		if !repeatedFile && !repeatedFamily && !repeatedRoot {
			continue
		}
		item.Action = types.ActionAskUser
		item.Description = strings.TrimSpace(item.Description) + " The same file or behavioral family remains implicated after a fresh semantic-repair attempt; primary-agent handoff is required."
	}
	return findings
}

type semanticIdentitySet struct {
	files    map[string]bool
	families map[string]bool
	roots    map[string]bool
}

func findingSemanticIdentities(raw string) semanticIdentitySet {
	identities := semanticIdentitySet{files: map[string]bool{}, families: map[string]bool{}, roots: map[string]bool{}}
	parsed, err := types.ParseFindingsJSON(raw)
	if err != nil {
		return identities
	}
	for _, item := range parsed.Items {
		if file := strings.TrimSpace(item.File); file != "" {
			identities.files[file] = true
		}
		if family := normalizeSemanticIdentity(item.SemanticFamily); family != "" {
			identities.families[family] = true
		}
		if root := normalizeSemanticIdentity(item.SemanticRoot); root != "" {
			identities.roots[root] = true
		}
	}
	return identities
}

func normalizeSemanticIdentity(value string) string {
	parts := strings.FieldsFunc(strings.ToLower(strings.TrimSpace(value)), func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9')
	})
	return strings.Join(parts, "-")
}

func firstFindingFile(raw string) string {
	parsed, err := types.ParseFindingsJSON(raw)
	if err != nil {
		return ""
	}
	for _, item := range parsed.Items {
		if file := strings.TrimSpace(item.File); file != "" {
			return file
		}
	}
	return ""
}

func reviewChangedPaths(ctx context.Context, sctx *pipeline.StepContext, baseSHA string) ([]string, error) {
	var args []string
	if sctx.Fixing {
		args = []string{"diff", "--name-only", "-z", "--no-renames", baseSHA}
	} else {
		args = []string{"diff", "--name-only", "-z", "--no-renames", baseSHA + ".." + sctx.Run.HeadSHA}
	}
	changedFiles, err := git.Run(ctx, sctx.WorkDir, args...)
	if err != nil {
		return nil, fmt.Errorf("get changed files: %w", err)
	}
	return changedPathList(changedFiles), nil
}
