package steps

import (
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
)

func TestCIStepCoordinatorRegistersExactWaitAndDefers(t *testing.T) {
	dir, baseSHA, headSHA := setupGitRepo(t)
	sctx := newTestContextWithDBRecords(t, &mockAgent{name: "test"}, dir, baseSHA, headSHA, config.Commands{})
	prURL := "https://github.com/test/repo/pull/42"
	sctx.Run.PRURL = &prURL
	sctx.Env = fakeCIGH(t, "OPEN", `[{"name":"build","state":"PENDING","bucket":"pending"}]`)
	sctx.Config.Coordinator = config.Coordinator{Enabled: true, ReconcileInterval: time.Minute}

	outcome, err := (&CIStep{}).Execute(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if !outcome.Deferred {
		t.Fatalf("outcome = %+v, want deferred coordinator custody", outcome)
	}
	wait, err := sctx.DB.GetCIWaitForRun(sctx.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if wait == nil || wait.RepoID != sctx.Repo.ID || wait.Branch != sctx.Run.Branch ||
		wait.PRNumber != 42 || wait.HeadSHA != headSHA || wait.DesiredGeneration != 1 ||
		wait.Status != db.CIWaitWaiting || wait.IntervalSeconds != 60 {
		t.Fatalf("registered wait = %+v", wait)
	}
}
