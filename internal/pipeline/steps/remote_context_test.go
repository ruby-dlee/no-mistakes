package steps

import (
	"strings"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestReviewRemoteStepContextCarriesSanitizedHistoryRecurrenceAndQualityAuthority(t *testing.T) {
	t.Parallel()
	dir, baseSHA, headSHA := setupGitRepo(t)
	sctx := newTestContextWithDBRecords(t, &mockAgent{name: "remote-context"}, dir, baseSHA, headSHA, config.Commands{})
	sctx.Fixing = true
	stepResult, err := sctx.DB.InsertStepResult(sctx.Run.ID, types.StepReview)
	if err != nil {
		t.Fatal(err)
	}
	sctx.StepResultID = stepResult.ID
	prior := `{"findings":[{"id":"old","severity":"warning","description":"repeat auth defect ghp_12345678901234567890","action":"ask-user"}],"summary":"old"}`
	summary := "first repair"
	if _, err := sctx.DB.InsertStepRound(sctx.StepResultID, 1, "auto_fix", &prior, &summary, 1); err != nil {
		t.Fatal(err)
	}
	declined := db.DeclinedSelectionJSON
	source := db.RoundSelectionSourceUserDeclined
	sctx.PriorBranchDecisions = []*db.BranchDecisionRound{{
		RunID: "prior-run", StepName: types.StepReview,
		Round: &db.StepRound{Round: 2, FindingsJSON: &prior, SelectedFindingIDs: &declined, SelectionSource: &source},
	}}
	sctx.UncertifiedPriorRounds = []*db.StepRound{{Round: 3, Trigger: "auto_fix", FindingsJSON: &prior, FixSummary: &summary}}

	got := (&ReviewStep{}).RemoteStepContext(sctx)
	if got.RepairAttempt != 2 || got.QualityOutcomeAuthority != "semantic-rereview" {
		t.Fatalf("remote context = %+v", got)
	}
	for _, want := range []string{"repeat auth defect", "declined", "first repair"} {
		if !strings.Contains(got.PriorRoundHistory+got.UncertifiedRoundHistory, want) {
			t.Fatalf("remote history missing %q: %+v", want, got)
		}
	}
	if strings.Contains(got.PriorRoundHistory+got.UncertifiedRoundHistory, "ghp_12345678901234567890") {
		t.Fatalf("remote history was not sanitized: %q", got.PriorRoundHistory)
	}
}
