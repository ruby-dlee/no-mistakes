package steps

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

type semanticQualityObservation struct {
	PreviousFindings string
	Findings         Findings
	Evidence         semanticRepairEvidence
	RepairAttempt    int
	FixedHeadSHA     string
	ObservedHeadSHA  string
	StructuredReview bool
}

type semanticQualityIdentity struct {
	Files    []string `json:"files,omitempty"`
	Families []string `json:"families,omitempty"`
	Roots    []string `json:"roots,omitempty"`
}

func recordSemanticRepairQualityOutcome(sctx *pipeline.StepContext, observation semanticQualityObservation) error {
	if sctx == nil || sctx.QualityOutcomes == nil {
		return nil
	}
	if sctx.Run == nil || strings.TrimSpace(sctx.Run.ID) == "" || observation.RepairAttempt < 1 {
		return fmt.Errorf("record semantic repair quality outcome: run and positive repair attempt are required")
	}
	classification, rootID := classifySemanticRepairQuality(observation)
	fixAttemptID := fmt.Sprintf("review-fix-%d", observation.RepairAttempt)
	previousDigest, err := pipeline.QualityEvidenceDigest(observation.PreviousFindings)
	if err != nil {
		return fmt.Errorf("record semantic repair quality outcome: %w", err)
	}
	observedDigest, err := pipeline.QualityEvidenceDigest(observation.Findings)
	if err != nil {
		return fmt.Errorf("record semantic repair quality outcome: %w", err)
	}
	repairProofDigest, err := pipeline.QualityEvidenceDigest(observation.Evidence)
	if err != nil {
		return fmt.Errorf("record semantic repair quality outcome: %w", err)
	}
	digest, err := pipeline.QualityEvidenceDigest(struct {
		Version             string                   `json:"version"`
		Classification      db.QualityClassification `json:"classification"`
		FixedHeadSHA        string                   `json:"fixed_head_sha"`
		ObservedHeadSHA     string                   `json:"observed_head_sha"`
		FixAttemptID        string                   `json:"fix_attempt_id"`
		RootID              string                   `json:"root_id,omitempty"`
		FixPromptVersion    string                   `json:"fix_prompt_version"`
		ReviewPromptVersion string                   `json:"review_prompt_version"`
		StructuredReview    bool                     `json:"structured_review"`
		PreviousEvidence    string                   `json:"previous_evidence_digest"`
		ObservedEvidence    string                   `json:"observed_evidence_digest"`
		RepairProofEvidence string                   `json:"repair_proof_evidence_digest"`
		PreviousIdentity    semanticQualityIdentity  `json:"previous_identity"`
		ObservedIdentity    semanticQualityIdentity  `json:"observed_identity"`
	}{
		Version: "semantic-quality.v1", Classification: classification,
		FixedHeadSHA: observation.FixedHeadSHA, ObservedHeadSHA: observation.ObservedHeadSHA,
		FixAttemptID: fixAttemptID, RootID: rootID, FixPromptVersion: reviewFixPromptVersion,
		ReviewPromptVersion: reviewPromptVersion, StructuredReview: observation.StructuredReview,
		PreviousEvidence:    previousDigest,
		ObservedEvidence:    observedDigest,
		RepairProofEvidence: repairProofDigest,
		PreviousIdentity:    semanticQualityIdentityFromRaw(observation.PreviousFindings),
		ObservedIdentity:    semanticQualityIdentityFromFindings(observation.Findings),
	})
	if err != nil {
		return fmt.Errorf("record semantic repair quality outcome: %w", err)
	}
	var rootPtr *string
	if rootID != "" {
		root := rootID
		rootPtr = &root
	}
	_, err = sctx.QualityOutcomes.InsertQualityOutcome(db.QualityOutcome{
		RunID: sctx.Run.ID, FixAttemptID: &fixAttemptID, RootID: rootPtr,
		Classification: classification, FixedHeadSHA: observation.FixedHeadSHA,
		ObservedHeadSHA: observation.ObservedHeadSHA, EvidenceDigest: digest,
		EvidenceProvenance: "semantic_rereview",
	})
	if err != nil {
		return fmt.Errorf("record semantic repair quality outcome: %w", err)
	}
	return nil
}

func classifySemanticRepairQuality(observation semanticQualityObservation) (db.QualityClassification, string) {
	previous := findingSemanticIdentities(observation.PreviousFindings)
	if family := boundedSemanticRoot(observation.Evidence.SemanticFamily); family != "" {
		previous.families[family] = true
	}
	if root := boundedSemanticRoot(observation.Evidence.SemanticRoot); root != "" {
		previous.roots[root] = true
	}
	material := materialReviewFindings(observation.Findings.Items)
	repeated, repeatedRoot := repeatedSemanticIdentity(material, previous)
	evidenceRoot := boundedSemanticRoot(observation.Evidence.SemanticRoot)

	if !observation.StructuredReview || !observation.Evidence.RepairComplete {
		return db.QualityPrimaryHandoff, firstNonEmpty(evidenceRoot, repeatedRoot, firstPreviousRoot(previous))
	}
	if len(material) == 0 {
		return db.QualityCleanFix, firstNonEmpty(evidenceRoot, firstPreviousRoot(previous))
	}
	if observation.RepairAttempt > 1 && repeated {
		return db.QualityPrimaryHandoff, firstNonEmpty(repeatedRoot, evidenceRoot)
	}
	if repeated {
		return db.QualitySameRootFollowup, firstNonEmpty(repeatedRoot, evidenceRoot)
	}
	if root := firstNewMaterialRoot(material, previous); root != "" {
		return db.QualityIntroducedRegression, root
	}
	return db.QualityPrimaryHandoff, evidenceRoot
}

func materialReviewFindings(items []Finding) []Finding {
	material := make([]Finding, 0, len(items))
	for _, item := range items {
		if item.Severity == types.FindingSeverityError || item.Severity == types.FindingSeverityWarning {
			material = append(material, item)
		}
	}
	return material
}

func repeatedSemanticIdentity(items []Finding, previous semanticIdentitySet) (bool, string) {
	for _, item := range items {
		if root := boundedSemanticRoot(item.SemanticRoot); root != "" && previous.roots[root] {
			return true, root
		}
	}
	for _, item := range items {
		if family := boundedSemanticRoot(item.SemanticFamily); family != "" && previous.families[family] {
			return true, boundedSemanticRoot("family-" + family)
		}
	}
	for _, item := range items {
		if file := strings.TrimSpace(item.File); file != "" && previous.files[file] {
			return true, fileIdentity(file)
		}
	}
	return false, ""
}

func firstNewMaterialRoot(items []Finding, previous semanticIdentitySet) string {
	for _, item := range items {
		if root := boundedSemanticRoot(item.SemanticRoot); root != "" && !previous.roots[root] {
			return root
		}
	}
	for _, item := range items {
		if family := boundedSemanticRoot(item.SemanticFamily); family != "" && !previous.families[family] {
			return boundedSemanticRoot("family-" + family)
		}
	}
	for _, item := range items {
		if file := strings.TrimSpace(item.File); file != "" && !previous.files[file] {
			return fileIdentity(file)
		}
	}
	return ""
}

func firstPreviousRoot(previous semanticIdentitySet) string {
	roots := make([]string, 0, len(previous.roots))
	for root := range previous.roots {
		roots = append(roots, boundedSemanticRoot(root))
	}
	sort.Strings(roots)
	if len(roots) > 0 {
		return roots[0]
	}
	families := make([]string, 0, len(previous.families))
	for family := range previous.families {
		families = append(families, boundedSemanticRoot("family-"+family))
	}
	sort.Strings(families)
	if len(families) > 0 {
		return families[0]
	}
	files := make([]string, 0, len(previous.files))
	for file := range previous.files {
		files = append(files, fileIdentity(file))
	}
	sort.Strings(files)
	if len(files) > 0 {
		return files[0]
	}
	return ""
}

func semanticQualityIdentityFromRaw(raw string) semanticQualityIdentity {
	return semanticQualityIdentityFromSet(findingSemanticIdentities(raw))
}

func semanticQualityIdentityFromFindings(findings Findings) semanticQualityIdentity {
	set := semanticIdentitySet{files: map[string]bool{}, families: map[string]bool{}, roots: map[string]bool{}}
	for _, item := range findings.Items {
		if file := strings.TrimSpace(item.File); file != "" {
			set.files[fileIdentity(file)] = true
		}
		if family := boundedSemanticRoot(item.SemanticFamily); family != "" {
			set.families[family] = true
		}
		if root := boundedSemanticRoot(item.SemanticRoot); root != "" {
			set.roots[root] = true
		}
	}
	return semanticQualityIdentityFromSet(set)
}

func semanticQualityIdentityFromSet(set semanticIdentitySet) semanticQualityIdentity {
	identity := semanticQualityIdentity{}
	for file := range set.files {
		if strings.HasPrefix(file, "file-") {
			identity.Files = append(identity.Files, file)
		} else {
			identity.Files = append(identity.Files, fileIdentity(file))
		}
	}
	for family := range set.families {
		identity.Families = append(identity.Families, boundedSemanticRoot(family))
	}
	for root := range set.roots {
		identity.Roots = append(identity.Roots, boundedSemanticRoot(root))
	}
	sort.Strings(identity.Files)
	sort.Strings(identity.Families)
	sort.Strings(identity.Roots)
	return identity
}

func boundedSemanticRoot(value string) string {
	value = normalizeSemanticIdentity(value)
	if len(value) > 64 {
		value = value[:64]
	}
	return value
}

func fileIdentity(file string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(file)))
	return "file-" + hex.EncodeToString(sum[:8])
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
