package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/db"
)

// QualityLifecycleBinding is the content-free identity attached to an actual
// owner override or revert event. Merely constructing one records nothing.
type QualityLifecycleBinding struct {
	RunID           string
	JobID           *string
	FixAttemptID    *string
	RootID          string
	FixedHeadSHA    string
	ObservedHeadSHA string
	SupersedesID    *string
	// LifecycleEventID is a bounded owner-decision or revert event identity.
	// It participates in the digest and is not persisted as evidence content.
	LifecycleEventID string
}

// QualityEvidenceDigest returns a deterministic digest while retaining none
// of the source evidence in the quality row.
func QualityEvidenceDigest(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal quality evidence identity: %w", err)
	}
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// RecordQualityOverride appends an overridden outcome only when the future
// owner-decision lifecycle explicitly calls this seam.
func RecordQualityOverride(writer QualityOutcomeWriter, binding QualityLifecycleBinding) error {
	return recordQualityLifecycle(writer, db.QualityOverridden, "owner_lifecycle", binding)
}

// RecordQualityRevert appends a reverted outcome only when the future revert
// lifecycle explicitly calls this seam.
func RecordQualityRevert(writer QualityOutcomeWriter, binding QualityLifecycleBinding) error {
	return recordQualityLifecycle(writer, db.QualityReverted, "revert_lifecycle", binding)
}

func recordQualityLifecycle(writer QualityOutcomeWriter, classification db.QualityClassification, provenance string, binding QualityLifecycleBinding) error {
	if writer == nil {
		return fmt.Errorf("record quality lifecycle outcome: writer is required")
	}
	if eventID := strings.TrimSpace(binding.LifecycleEventID); eventID == "" || len(eventID) > 128 {
		return fmt.Errorf("record quality lifecycle outcome: bounded lifecycle event id is required")
	}
	digest, err := QualityEvidenceDigest(struct {
		Version          string                   `json:"version"`
		Classification   db.QualityClassification `json:"classification"`
		RunID            string                   `json:"run_id"`
		JobID            *string                  `json:"job_id,omitempty"`
		FixAttemptID     *string                  `json:"fix_attempt_id,omitempty"`
		RootID           string                   `json:"root_id,omitempty"`
		FixedHeadSHA     string                   `json:"fixed_head_sha"`
		ObservedHeadSHA  string                   `json:"observed_head_sha"`
		LifecycleEventID string                   `json:"lifecycle_event_id"`
		SupersedesID     *string                  `json:"supersedes_id,omitempty"`
	}{
		Version: "quality-lifecycle.v1", Classification: classification,
		RunID: binding.RunID, JobID: binding.JobID, FixAttemptID: binding.FixAttemptID,
		RootID: binding.RootID, FixedHeadSHA: binding.FixedHeadSHA,
		ObservedHeadSHA: binding.ObservedHeadSHA, LifecycleEventID: binding.LifecycleEventID,
		SupersedesID: binding.SupersedesID,
	})
	if err != nil {
		return err
	}
	var rootID *string
	if binding.RootID != "" {
		root := binding.RootID
		rootID = &root
	}
	_, err = writer.InsertQualityOutcome(db.QualityOutcome{
		RunID: binding.RunID, JobID: binding.JobID, FixAttemptID: binding.FixAttemptID,
		RootID: rootID, Classification: classification, FixedHeadSHA: binding.FixedHeadSHA,
		ObservedHeadSHA: binding.ObservedHeadSHA, EvidenceDigest: digest,
		EvidenceProvenance: provenance, SupersedesID: binding.SupersedesID,
	})
	if err != nil {
		return fmt.Errorf("record %s quality lifecycle outcome: %w", classification, err)
	}
	return nil
}
