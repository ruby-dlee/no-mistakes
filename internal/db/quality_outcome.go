package db

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

// QualityClassification is one immutable observation about a no-mistakes fix
// or handoff. The vocabulary is intentionally closed so longitudinal quality
// rates cannot fragment into spelling variants.
type QualityClassification string

const (
	QualityCleanFix             QualityClassification = "clean_fix"
	QualitySameRootFollowup     QualityClassification = "same_root_followup"
	QualityIntroducedRegression QualityClassification = "introduced_regression"
	QualityOverridden           QualityClassification = "overridden"
	QualityReverted             QualityClassification = "reverted"
	QualityPrimaryHandoff       QualityClassification = "primary_handoff"
)

var (
	qualityHeadPattern       = regexp.MustCompile(`^[0-9a-f]{40}([0-9a-f]{24})?$`)
	qualityDigestPattern     = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	qualityProvenancePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
)

func validQualityClassification(value QualityClassification) bool {
	switch value {
	case QualityCleanFix, QualitySameRootFollowup, QualityIntroducedRegression,
		QualityOverridden, QualityReverted, QualityPrimaryHandoff:
		return true
	default:
		return false
	}
}

// QualityOutcome is local-only append-only evidence. JobID, FixAttemptID, and
// RootID are optional correlation identifiers because not every observation
// originates in an Azure job, a fixer invocation, or a diagnosed root-cause
// chain. EvidenceProvenance is a bounded category, never a path or payload.
type QualityOutcome struct {
	ID                 string
	RunID              string
	JobID              *string
	FixAttemptID       *string
	RootID             *string
	Classification     QualityClassification
	FixedHeadSHA       string
	ObservedHeadSHA    string
	EvidenceDigest     string
	EvidenceProvenance string
	SupersedesID       *string
	CreatedAt          int64
}

func validateQualityOutcome(out QualityOutcome) error {
	if strings.TrimSpace(out.RunID) == "" {
		return fmt.Errorf("quality outcome: run id is required")
	}
	if !validQualityClassification(out.Classification) {
		return fmt.Errorf("quality outcome: unsupported classification %q", out.Classification)
	}
	if !qualityHeadPattern.MatchString(out.FixedHeadSHA) || !qualityHeadPattern.MatchString(out.ObservedHeadSHA) {
		return fmt.Errorf("quality outcome: fixed and observed heads must be full lowercase git object IDs")
	}
	if !qualityDigestPattern.MatchString(out.EvidenceDigest) {
		return fmt.Errorf("quality outcome: evidence digest must be sha256:<64 lowercase hex>")
	}
	if !qualityProvenancePattern.MatchString(out.EvidenceProvenance) {
		return fmt.Errorf("quality outcome: evidence provenance must be a bounded category")
	}
	return nil
}

// InsertQualityOutcome appends a classification. Supersession is constrained
// to the same run and, when present, the same root identifier. The prior row is
// never updated.
func (d *DB) InsertQualityOutcome(out QualityOutcome) (*QualityOutcome, error) {
	if err := validateQualityOutcome(out); err != nil {
		return nil, err
	}
	if out.SupersedesID != nil {
		var priorRun string
		var priorRoot sql.NullString
		if err := d.sql.QueryRow(`SELECT run_id, root_id FROM quality_outcomes WHERE id = ?`, *out.SupersedesID).Scan(&priorRun, &priorRoot); err != nil {
			return nil, fmt.Errorf("quality outcome: read superseded row: %w", err)
		}
		if priorRun != out.RunID {
			return nil, fmt.Errorf("quality outcome: supersession must stay within one run")
		}
		if (priorRoot.Valid != (out.RootID != nil)) || (priorRoot.Valid && priorRoot.String != *out.RootID) {
			return nil, fmt.Errorf("quality outcome: supersession must stay within one root")
		}
	}
	out.ID = newID()
	out.CreatedAt = now()
	_, err := d.sql.Exec(`INSERT INTO quality_outcomes
		(id, run_id, job_id, fix_attempt_id, root_id, classification, fixed_head_sha, observed_head_sha,
		 evidence_digest, evidence_provenance, supersedes_id, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		out.ID, out.RunID, out.JobID, out.FixAttemptID, out.RootID, out.Classification,
		out.FixedHeadSHA, out.ObservedHeadSHA, out.EvidenceDigest, out.EvidenceProvenance,
		out.SupersedesID, out.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("insert quality outcome: %w", err)
	}
	return &out, nil
}

// GetQualityOutcomesByRun returns the immutable classification history in
// append order.
func (d *DB) GetQualityOutcomesByRun(runID string) ([]QualityOutcome, error) {
	rows, err := d.sql.Query(`SELECT id, run_id, job_id, fix_attempt_id, root_id, classification,
		fixed_head_sha, observed_head_sha, evidence_digest, evidence_provenance, supersedes_id, created_at
		FROM quality_outcomes WHERE run_id = ? ORDER BY created_at, id`, runID)
	if err != nil {
		return nil, fmt.Errorf("get quality outcomes: %w", err)
	}
	defer rows.Close()
	var outcomes []QualityOutcome
	for rows.Next() {
		var out QualityOutcome
		if err := rows.Scan(&out.ID, &out.RunID, &out.JobID, &out.FixAttemptID, &out.RootID,
			&out.Classification, &out.FixedHeadSHA, &out.ObservedHeadSHA, &out.EvidenceDigest,
			&out.EvidenceProvenance, &out.SupersedesID, &out.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan quality outcome: %w", err)
		}
		outcomes = append(outcomes, out)
	}
	return outcomes, rows.Err()
}
