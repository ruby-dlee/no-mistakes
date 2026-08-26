package steps

import (
	"errors"
	"strings"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

type qualityOutcomeRecorderStub struct {
	outcomes []db.QualityOutcome
	err      error
}

func (r *qualityOutcomeRecorderStub) InsertQualityOutcome(out db.QualityOutcome) (*db.QualityOutcome, error) {
	if r.err != nil {
		return nil, r.err
	}
	r.outcomes = append(r.outcomes, out)
	return &out, nil
}

func semanticQualityContext(recorder pipeline.QualityOutcomeWriter) *pipeline.StepContext {
	return &pipeline.StepContext{
		Run: &db.Run{
			ID:      "run-quality",
			HeadSHA: strings.Repeat("2", 40),
		},
		QualityOutcomes: recorder,
	}
}

func materialSemanticFinding(file, family, root string) Finding {
	return Finding{
		ID:             "finding-1",
		Severity:       types.FindingSeverityError,
		File:           file,
		Action:         types.ActionAskUser,
		ReviewScope:    types.FindingReviewScopeSource,
		SemanticFamily: family,
		SemanticRoot:   root,
	}
}

func TestSemanticQualityOutcomeClassificationsAndBindings(t *testing.T) {
	previous := `{"findings":[{"id":"old","severity":"error","file":"parser/input.go","semantic_family":"parser-serialization","semantic_root":"effective-audience"}]}`
	evidence := semanticRepairEvidence{
		RepairComplete: true,
		SemanticFamily: "parser-serialization",
		SemanticRoot:   "effective-audience",
	}
	tests := []struct {
		name       string
		previous   string
		attempt    int
		evidence   semanticRepairEvidence
		findings   Findings
		structured bool
		changed    []string
		want       db.QualityClassification
		wantRoot   string
	}{
		{name: "clean rereview", attempt: 1, evidence: evidence, findings: Findings{}, structured: true, want: db.QualityCleanFix, wantRoot: "effective-audience"},
		{name: "same root different file", attempt: 1, evidence: evidence, findings: Findings{Items: []Finding{materialSemanticFinding("api/audience.go", "parser-serialization", "effective-audience")}}, structured: true, want: db.QualitySameRootFollowup, wantRoot: "effective-audience"},
		{name: "fix proof supplies missing prior identity", previous: `{"findings":[{"id":"old","severity":"error","description":"legacy finding without semantic fields"}]}`, attempt: 1, evidence: evidence, findings: Findings{Items: []Finding{materialSemanticFinding("api/audience.go", "parser-serialization", "effective-audience")}}, structured: true, want: db.QualitySameRootFollowup, wantRoot: "effective-audience"},
		{name: "new material root without fixer path proof", attempt: 1, evidence: evidence, findings: Findings{Items: []Finding{materialSemanticFinding("api/auth.go", "auth-permission", "tenant-isolation")}}, structured: true, want: db.QualityPrimaryHandoff, wantRoot: "tenant-isolation"},
		{name: "new material root on fixer changed path", attempt: 1, evidence: evidence, findings: Findings{Items: []Finding{materialSemanticFinding("api/auth.go", "auth-permission", "tenant-isolation")}}, structured: true, changed: []string{"api/auth.go"}, want: db.QualityIntroducedRegression, wantRoot: "tenant-isolation"},
		{name: "incomplete proof", attempt: 1, evidence: semanticRepairEvidence{RepairComplete: false, SemanticRoot: "effective-audience"}, findings: Findings{}, structured: true, want: db.QualityPrimaryHandoff, wantRoot: "effective-audience"},
		{name: "second same-root escalation", attempt: 2, evidence: evidence, findings: Findings{Items: []Finding{materialSemanticFinding("api/audience.go", "parser-serialization", "effective-audience")}}, structured: true, want: db.QualityPrimaryHandoff, wantRoot: "effective-audience"},
		{name: "unstructured rereview", attempt: 1, evidence: evidence, findings: Findings{}, structured: false, want: db.QualityPrimaryHandoff, wantRoot: "effective-audience"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := &qualityOutcomeRecorderStub{}
			sctx := semanticQualityContext(recorder)
			previousFindings := previous
			if tt.previous != "" {
				previousFindings = tt.previous
			}
			err := recordSemanticRepairQualityOutcome(sctx, semanticQualityObservation{
				PreviousFindings:  previousFindings,
				Findings:          tt.findings,
				Evidence:          tt.evidence,
				RepairAttempt:     tt.attempt,
				FixedHeadSHA:      strings.Repeat("1", 40),
				ObservedHeadSHA:   strings.Repeat("2", 40),
				StructuredReview:  tt.structured,
				FixerChangedPaths: tt.changed,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(recorder.outcomes) != 1 {
				t.Fatalf("outcomes = %d, want 1", len(recorder.outcomes))
			}
			got := recorder.outcomes[0]
			if got.Classification != tt.want || got.FixedHeadSHA != strings.Repeat("1", 40) || got.ObservedHeadSHA != strings.Repeat("2", 40) {
				t.Fatalf("outcome = %+v, want classification %s and exact heads", got, tt.want)
			}
			if got.FixAttemptID == nil || *got.FixAttemptID != "review-fix-1" && *got.FixAttemptID != "review-fix-2" {
				t.Fatalf("fix attempt binding = %v", got.FixAttemptID)
			}
			if got.RootID == nil || *got.RootID != tt.wantRoot {
				t.Fatalf("root binding = %v, want %q", got.RootID, tt.wantRoot)
			}
			if !strings.HasPrefix(got.EvidenceDigest, "sha256:") || len(got.EvidenceDigest) != 71 {
				t.Fatalf("evidence digest = %q", got.EvidenceDigest)
			}
			if got.EvidenceProvenance != "semantic_rereview" {
				t.Fatalf("provenance = %q", got.EvidenceProvenance)
			}
		})
	}
}

func TestSemanticQualityOutcomeRecordingFailureIsFailClosed(t *testing.T) {
	recorder := &qualityOutcomeRecorderStub{err: errors.New("disk full")}
	err := recordSemanticRepairQualityOutcome(semanticQualityContext(recorder), semanticQualityObservation{
		PreviousFindings: `{"findings":[{"file":"feature.go","semantic_root":"public-contract"}]}`,
		Findings:         Findings{},
		Evidence:         semanticRepairEvidence{RepairComplete: true, SemanticRoot: "public-contract"},
		RepairAttempt:    1,
		FixedHeadSHA:     strings.Repeat("1", 40),
		ObservedHeadSHA:  strings.Repeat("2", 40),
		StructuredReview: true,
	})
	if err == nil || !strings.Contains(err.Error(), "record semantic repair quality outcome") || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("recording error = %v, want visible fail-closed error", err)
	}
}

func TestSemanticQualityOutcomeDigestIsDeterministicAndContentFree(t *testing.T) {
	record := func() db.QualityOutcome {
		recorder := &qualityOutcomeRecorderStub{}
		err := recordSemanticRepairQualityOutcome(semanticQualityContext(recorder), semanticQualityObservation{
			PreviousFindings: `{"findings":[{"description":"private source evidence","semantic_root":"public-contract"}]}`,
			Findings:         Findings{},
			Evidence: semanticRepairEvidence{
				RepairComplete:        true,
				SemanticRoot:          "public-contract",
				PublicExecutableCheck: "private command identity",
			},
			RepairAttempt:    1,
			FixedHeadSHA:     strings.Repeat("1", 40),
			ObservedHeadSHA:  strings.Repeat("2", 40),
			StructuredReview: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		return recorder.outcomes[0]
	}
	first, second := record(), record()
	if first.EvidenceDigest != second.EvidenceDigest {
		t.Fatalf("digest is not deterministic: %q != %q", first.EvidenceDigest, second.EvidenceDigest)
	}
	serialized := first.EvidenceDigest + first.EvidenceProvenance
	for _, secret := range []string{"private source evidence", "private command identity"} {
		if strings.Contains(serialized, secret) {
			t.Fatalf("quality row retained evidence content %q", secret)
		}
	}
}
