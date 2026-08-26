package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestDaemonStopRefusesWithActiveRunsAndListsThem(t *testing.T) {
	nmHome := t.TempDir()
	t.Setenv("NM_HOME", nmHome)
	createLifecycleGuardRuns(t, paths.WithRoot(nmHome))

	stopCalled := false
	prevStop := daemonStopFn
	daemonStopFn = func(*paths.Paths) error {
		stopCalled = true
		return nil
	}
	t.Cleanup(func() { daemonStopFn = prevStop })

	out, err := executeCmd("daemon", "stop")
	if err == nil {
		t.Fatal("daemon stop should refuse while active runs exist")
	}
	if stopCalled {
		t.Fatal("daemon stop should not stop the daemon after refusing")
	}
	for _, want := range []string{
		"refusing daemon stop",
		"2 active pipeline executions",
		"head=aaaaaaaa",
		"head=bbbbbbbb",
		"owner=test-worker",
		"--force",
	} {
		if !strings.Contains(out+err.Error(), want) {
			t.Fatalf("daemon stop refusal should contain %q, got output %q error %v", want, out, err)
		}
	}
}

func TestDaemonStopForceOverridesActiveRunGuard(t *testing.T) {
	nmHome := t.TempDir()
	t.Setenv("NM_HOME", nmHome)
	createLifecycleGuardRuns(t, paths.WithRoot(nmHome))

	stopCalled := false
	prevStop := daemonStopFn
	daemonStopFn = func(*paths.Paths) error {
		stopCalled = true
		return nil
	}
	t.Cleanup(func() { daemonStopFn = prevStop })

	out, err := executeCmd("daemon", "stop", "--force")
	if err != nil {
		t.Fatalf("daemon stop --force failed: %v\n%s", err, out)
	}
	if !stopCalled {
		t.Fatal("daemon stop --force should stop the daemon")
	}
	if !strings.Contains(out, "FORCE: daemon stop") {
		t.Fatalf("force output should be loud, got %q", out)
	}
}

func TestDaemonRestartRefusesWithActiveRuns(t *testing.T) {
	nmHome := t.TempDir()
	t.Setenv("NM_HOME", nmHome)
	createLifecycleGuardRuns(t, paths.WithRoot(nmHome))

	stopCalled := false
	startCalled := false
	prevStop := daemonStopFn
	prevStart := daemonStartFn
	daemonStopFn = func(*paths.Paths) error {
		stopCalled = true
		return nil
	}
	daemonStartFn = func(*paths.Paths) error {
		startCalled = true
		return nil
	}
	t.Cleanup(func() {
		daemonStopFn = prevStop
		daemonStartFn = prevStart
	})

	out, err := executeCmd("daemon", "restart")
	if err == nil {
		t.Fatal("daemon restart should refuse while active runs exist")
	}
	if stopCalled || startCalled {
		t.Fatalf("daemon restart should not stop/start after refusing; stop=%t start=%t", stopCalled, startCalled)
	}
	if !strings.Contains(out+err.Error(), "refusing daemon restart") || !strings.Contains(out+err.Error(), "owner=test-worker") {
		t.Fatalf("daemon restart refusal should list active workers, got output %q error %v", out, err)
	}
}

func TestDaemonStopIgnoresLegacyRunningRowsWithoutWorkerLeases(t *testing.T) {
	nmHome := t.TempDir()
	t.Setenv("NM_HOME", nmHome)
	p := paths.WithRoot(nmHome)
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	database, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	repo, err := database.InsertRepo("/tmp/stale-project", "git@github.com:user/stale-project.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 56; i++ {
		run, err := database.InsertRun(repo.ID, "stale-"+string(rune(0x100+i)), strings.Repeat("c", 40), strings.Repeat("0", 40))
		if err != nil {
			t.Fatal(err)
		}
		if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
			t.Fatal(err)
		}
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	stopCalled := false
	prevStop := daemonStopFn
	daemonStopFn = func(*paths.Paths) error {
		stopCalled = true
		return nil
	}
	t.Cleanup(func() { daemonStopFn = prevStop })

	out, err := executeCmd("daemon", "stop")
	if err != nil {
		t.Fatalf("stale rows blocked daemon stop: %v\n%s", err, out)
	}
	if !stopCalled {
		t.Fatal("daemon stop was not called")
	}
}

func TestLifecycleCommandsWriteCallerAttributionToCLILog(t *testing.T) {
	nmHome := t.TempDir()
	t.Setenv("NM_HOME", nmHome)

	prevStop := daemonStopFn
	prevStart := daemonStartFn
	daemonStopFn = func(*paths.Paths) error { return nil }
	daemonStartFn = func(*paths.Paths) error { return nil }
	t.Cleanup(func() {
		daemonStopFn = prevStop
		daemonStartFn = prevStart
	})

	out, err := executeCmd("daemon", "stop", "--force")
	if err != nil {
		t.Fatalf("daemon stop --force failed: %v\n%s", err, out)
	}
	out, err = executeCmd("daemon", "restart", "--force")
	if err != nil {
		t.Fatalf("daemon restart --force failed: %v\n%s", err, out)
	}
	out, err = executeCmd("update", "--force")
	if err != nil {
		t.Fatalf("update --force failed: %v\n%s", err, out)
	}

	data, err := os.ReadFile(filepath.Join(nmHome, "logs", "cli.log"))
	if err != nil {
		t.Fatalf("read cli.log: %v", err)
	}
	log := string(data)
	for _, want := range []string{
		"lifecycle FORCE command=daemon.stop",
		"lifecycle FORCE command=daemon.restart",
		"lifecycle FORCE command=update",
		"force=true",
		"pid=",
		"ppid=",
		"parent_cmdline=",
	} {
		if !strings.Contains(log, want) {
			t.Fatalf("cli.log should contain %q, got %q", want, log)
		}
	}
}

func createLifecycleGuardRuns(t *testing.T, p *paths.Paths) {
	t.Helper()
	if err := p.EnsureDirs(); err != nil {
		t.Fatalf("ensure dirs: %v", err)
	}
	database, err := db.Open(p.DB())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer database.Close()
	repo, err := database.InsertRepo("/tmp/project", "git@github.com:user/project.git", "main")
	if err != nil {
		t.Fatalf("insert repo: %v", err)
	}
	pending, err := database.InsertRun(repo.ID, "feature-a", strings.Repeat("a", 40), strings.Repeat("0", 40))
	if err != nil {
		t.Fatalf("insert pending run: %v", err)
	}
	running, err := database.InsertRun(repo.ID, "feature-b", strings.Repeat("b", 40), strings.Repeat("0", 40))
	if err != nil {
		t.Fatalf("insert running run: %v", err)
	}
	if err := database.UpdateRunStatus(running.ID, types.RunRunning); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	for index, run := range []*db.Run{pending, running} {
		step, err := database.InsertStepResult(run.ID, types.StepReview)
		if err != nil {
			t.Fatalf("insert worker step: %v", err)
		}
		if _, replay, err := database.EnqueuePipelineJob(db.PipelineJobSpec{
			RunID:          run.ID,
			StepResultID:   step.ID,
			Kind:           db.PipelineJobReview,
			Round:          1,
			DesiredHeadSHA: run.HeadSHA,
			InputDigest:    strings.Repeat(string(rune('c'+index)), 64),
			MaxAttempts:    2,
		}); err != nil || replay {
			t.Fatalf("enqueue worker: replay=%v err=%v", replay, err)
		}
	}
	now := time.Now()
	for i := 0; i < 2; i++ {
		lease, err := database.ClaimPipelineJob(db.PipelineJobReview, "test-worker", now, time.Hour)
		if err != nil || lease == nil {
			t.Fatalf("claim worker %d: lease=%+v err=%v", i, lease, err)
		}
	}
}
