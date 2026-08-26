package lifecycle

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/ipc"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestActiveWorkUsesDaemonExecutionsAndIgnoresStaleRunRows(t *testing.T) {
	root, err := os.MkdirTemp("", "nm-lc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	t.Setenv("NM_HOME", root)
	p := paths.WithRoot(root)
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	database, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	repo, err := database.InsertRepo(t.TempDir(), "https://example.invalid/liveness.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 56; i++ {
		run, err := database.InsertRun(repo.ID, "stale-"+string(rune(0x100+i)), strings.Repeat("a", 40), strings.Repeat("0", 40))
		if err != nil {
			t.Fatal(err)
		}
		if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
			t.Fatal(err)
		}
	}
	live, err := database.InsertRun(repo.ID, "live", strings.Repeat("b", 40), strings.Repeat("0", 40))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(live.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	srv := ipc.NewServer()
	srv.Handle(ipc.MethodGetExecutingRuns, func(context.Context, json.RawMessage) (interface{}, error) {
		return &ipc.GetExecutingRunsResult{RunIDs: []string{live.ID}}, nil
	})
	if err := srv.Listen(p.Socket()); err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ServeReady() }()
	t.Cleanup(func() {
		srv.Close()
		if err := <-errCh; err != nil {
			t.Errorf("serve: %v", err)
		}
	})

	work, err := ActiveWork(p, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if work.Count() != 1 || len(work.LocalRuns) != 1 || work.LocalRuns[0].ID != live.ID || len(work.WorkerLeases) != 0 {
		t.Fatalf("active work = %+v", work)
	}
}

func TestActiveWorkFailsClosedWhenDaemonIdentityExistsButIPCIsUnavailable(t *testing.T) {
	root := t.TempDir()
	t.Setenv("NM_HOME", root)
	p := paths.WithRoot(root)
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.PIDFile(), []byte("123\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ActiveWork(p, time.Now()); err == nil || !strings.Contains(err.Error(), "execution liveness") {
		t.Fatalf("error = %v", err)
	}
}
