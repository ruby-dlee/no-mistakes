package db

import (
	"path/filepath"
	"testing"
)

const (
	qualityFixedHead    = "1111111111111111111111111111111111111111"
	qualityObservedHead = "2222222222222222222222222222222222222222"
	qualityDigest       = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

func TestQualityOutcomesAppendAndSupersede(t *testing.T) {
	d, _, run := openSessionTestDB(t)
	first, err := d.InsertQualityOutcome(QualityOutcome{
		RunID: run.ID, JobID: strPtr("azure-job-1"), FixAttemptID: strPtr("fix-1"), RootID: strPtr("root-1"),
		Classification: QualitySameRootFollowup, FixedHeadSHA: qualityFixedHead, ObservedHeadSHA: qualityObservedHead,
		EvidenceDigest: qualityDigest, EvidenceProvenance: "crosscheck",
	})
	if err != nil {
		t.Fatalf("insert first: %v", err)
	}
	second, err := d.InsertQualityOutcome(QualityOutcome{
		RunID: run.ID, JobID: strPtr("azure-job-1"), FixAttemptID: strPtr("fix-1"), RootID: strPtr("root-1"),
		Classification: QualityCleanFix, FixedHeadSHA: qualityFixedHead, ObservedHeadSHA: qualityObservedHead,
		EvidenceDigest: qualityDigest, EvidenceProvenance: "operator", SupersedesID: &first.ID,
	})
	if err != nil {
		t.Fatalf("insert supersession: %v", err)
	}
	got, err := d.GetQualityOutcomesByRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Classification != QualitySameRootFollowup || got[1].Classification != QualityCleanFix ||
		got[0].JobID == nil || *got[0].JobID != "azure-job-1" ||
		got[0].FixAttemptID == nil || *got[0].FixAttemptID != "fix-1" ||
		got[0].RootID == nil || *got[0].RootID != "root-1" ||
		got[0].FixedHeadSHA != qualityFixedHead || got[0].ObservedHeadSHA != qualityObservedHead ||
		got[0].EvidenceDigest != qualityDigest || got[0].EvidenceProvenance != "crosscheck" ||
		got[1].SupersedesID == nil || *got[1].SupersedesID != first.ID || second.ID == first.ID {
		t.Fatalf("append-only history mismatch: %+v", got)
	}
	if _, err := d.sql.Exec(`UPDATE quality_outcomes SET classification = ? WHERE id = ?`, QualityReverted, first.ID); err == nil {
		t.Fatal("quality history update unexpectedly succeeded")
	}
	if _, err := d.sql.Exec(`DELETE FROM quality_outcomes WHERE id = ?`, first.ID); err == nil {
		t.Fatal("quality history delete unexpectedly succeeded")
	}
}

func TestQualityOutcomesAllowWholeRepoPrivacyDeletion(t *testing.T) {
	d, repo, run := openSessionTestDB(t)
	if _, err := d.InsertQualityOutcome(QualityOutcome{
		RunID: run.ID, Classification: QualityCleanFix,
		FixedHeadSHA: qualityFixedHead, ObservedHeadSHA: qualityObservedHead,
		EvidenceDigest: qualityDigest, EvidenceProvenance: "test",
	}); err != nil {
		t.Fatal(err)
	}
	if err := d.DeleteRepo(repo.ID); err != nil {
		t.Fatalf("whole-repo privacy deletion must remove local quality telemetry: %v", err)
	}
	var count int
	if err := d.sql.QueryRow(`SELECT COUNT(*) FROM quality_outcomes WHERE run_id = ?`, run.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("quality outcomes left after repo deletion: %d", count)
	}
}

func TestQualityOutcomesExactEnumsAndSupersessionBinding(t *testing.T) {
	d, repo, run := openSessionTestDB(t)
	for _, classification := range []QualityClassification{
		QualityCleanFix, QualitySameRootFollowup, QualityIntroducedRegression,
		QualityOverridden, QualityReverted, QualityPrimaryHandoff,
	} {
		if _, err := d.InsertQualityOutcome(QualityOutcome{
			RunID: run.ID, RootID: strPtr("root-" + string(classification)), Classification: classification,
			FixedHeadSHA: qualityFixedHead, ObservedHeadSHA: qualityObservedHead,
			EvidenceDigest: qualityDigest, EvidenceProvenance: "test",
		}); err != nil {
			t.Errorf("classification %q rejected: %v", classification, err)
		}
	}
	if _, err := d.InsertQualityOutcome(QualityOutcome{
		RunID: run.ID, Classification: "fixed-ish", FixedHeadSHA: qualityFixedHead,
		ObservedHeadSHA: qualityObservedHead, EvidenceDigest: qualityDigest, EvidenceProvenance: "test",
	}); err == nil {
		t.Fatal("unknown classification unexpectedly accepted")
	}
	if _, err := d.InsertQualityOutcome(QualityOutcome{
		RunID: run.ID, Classification: QualityCleanFix, FixedHeadSHA: "short",
		ObservedHeadSHA: qualityObservedHead, EvidenceDigest: qualityDigest,
		EvidenceProvenance: "test output bytes",
	}); err == nil {
		t.Fatal("unbound or content-shaped evidence unexpectedly accepted")
	}

	first, err := d.InsertQualityOutcome(QualityOutcome{
		RunID: run.ID, RootID: strPtr("root-bound"), Classification: QualityCleanFix,
		FixedHeadSHA: qualityFixedHead, ObservedHeadSHA: qualityObservedHead,
		EvidenceDigest: qualityDigest, EvidenceProvenance: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	other, err := d.InsertRun(repo.ID, "other", "head2", "base2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.InsertQualityOutcome(QualityOutcome{
		RunID: other.ID, RootID: strPtr("root-bound"), Classification: QualityReverted,
		FixedHeadSHA: qualityFixedHead, ObservedHeadSHA: qualityObservedHead,
		EvidenceDigest: qualityDigest, EvidenceProvenance: "test", SupersedesID: &first.ID,
	}); err == nil {
		t.Fatal("cross-run supersession unexpectedly accepted")
	}
}

func TestOpenMigratesQualityOutcomesTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite")
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.sql.Exec(`DROP TABLE quality_outcomes`); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d.Close()
	if !d.hasColumn("quality_outcomes", "classification") || !d.hasColumn("quality_outcomes", "supersedes_id") {
		t.Fatal("quality_outcomes table missing after migration")
	}
}
