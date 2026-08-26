package daemon

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/coordinator"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/types"
	"github.com/kunchenguid/no-mistakes/internal/worktrees"
)

type daemonRepoMapper struct{ repoID string }

func (m daemonRepoMapper) ResolveGitHubRepository(context.Context, string) (string, error) {
	return m.repoID, nil
}

type daemonGitHubClient struct {
	repoID string
	pr     int64
	head   string
	calls  atomic.Int32
}

func (c *daemonGitHubClient) RefetchCIState(context.Context, string, int64) (db.AuthoritativeGitHubState, error) {
	state := "passed"
	if c.calls.Add(1) == 1 {
		state = "pending"
	}
	return db.AuthoritativeGitHubState{
		RepoID: c.repoID, PRNumber: c.pr, HeadSHA: c.head, CheckState: state,
		Mergeability: db.MergeabilityMergeable,
	}, nil
}

func TestCoordinatorRuntimeStaysDisabledWithoutReadingSecretOrBinding(t *testing.T) {
	getenvCalled := false
	listenCalled := false
	runtime, err := startCoordinatorRuntime(context.Background(), coordinatorRuntimeOptions{
		Config: config.Coordinator{Enabled: false},
		Getenv: func(string) (string, bool) {
			getenvCalled = true
			return "", false
		},
		Listen: func(string, string) (net.Listener, error) {
			listenCalled = true
			return nil, fmt.Errorf("must not bind")
		},
	})
	if err != nil || runtime != nil || getenvCalled || listenCalled {
		t.Fatalf("runtime=%v err=%v getenv=%v listen=%v", runtime, err, getenvCalled, listenCalled)
	}
}

func TestCoordinatorRuntimeFailsBeforeBindingWithoutSecret(t *testing.T) {
	listenCalled := false
	_, err := startCoordinatorRuntime(context.Background(), coordinatorRuntimeOptions{
		Config: config.Coordinator{
			Enabled: true, ListenAddress: "127.0.0.1:9783",
			GitHubWebhookSecretEnv: "NO_MISTAKES_GITHUB_WEBHOOK_SECRET",
			ReconcileInterval:      time.Minute, BatchSize: 100, MaxConcurrency: 4,
		},
		Getenv: func(string) (string, bool) { return "", false },
		Listen: func(string, string) (net.Listener, error) {
			listenCalled = true
			return nil, fmt.Errorf("must not bind")
		},
	})
	if err == nil || listenCalled || strings.Contains(err.Error(), "NO_MISTAKES_GITHUB_WEBHOOK_SECRET=") {
		t.Fatalf("err=%v listen=%v", err, listenCalled)
	}
}

func TestCoordinatorRuntimeServesSignedWebhookRecoversWaitAndStopsCleanly(t *testing.T) {
	root := t.TempDir()
	p := paths.WithRoot(root)
	database, err := db.Open(filepath.Join(root, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	repo, err := database.InsertRepo(t.TempDir(), "https://github.com/Ruby-Labs/Relvino.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	head := strings.Repeat("a", 40)
	run, err := database.InsertRun(repo.ID, "review/runtime", head, strings.Repeat("0", 40))
	if err != nil {
		t.Fatal(err)
	}
	step, err := database.InsertStepResult(run.ID, types.StepCI)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateStepStatus(step.ID, types.StepStatusRunning); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Second)
	input := strings.Repeat("b", 64)
	desired, _, _, err := database.AdvanceBranchDesiredState(db.BranchDesiredUpdate{
		RepoID: repo.ID, Branch: run.Branch, HeadSHA: head, InputDigest: input, UpdatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitID, err := database.RegisterCIWait(db.CIWaitSpec{
		RunID: run.ID, RepoID: repo.ID, Branch: run.Branch, PRNumber: 327, HeadSHA: head,
		InputDigest: input, DesiredGeneration: desired.Revision, RegisteredAt: now,
		ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	secret := "runtime-test-secret"
	client := &daemonGitHubClient{repoID: repo.ID, pr: 327, head: head}
	runtime, err := startCoordinatorRuntime(context.Background(), coordinatorRuntimeOptions{
		Config: config.Coordinator{
			Enabled: true, ListenAddress: "127.0.0.1:0",
			GitHubWebhookSecretEnv: "TEST_COORDINATOR_SECRET",
			ReconcileInterval:      time.Second, BatchSize: 100, MaxConcurrency: 4,
		},
		DB: database, Paths: p, Repositories: daemonRepoMapper{repoID: repo.ID},
		GitHub: client, Reducer: coordinator.ExactCIStateReducer{},
		Getenv: func(name string) (string, bool) {
			if name != "TEST_COORDINATOR_SECRET" {
				t.Fatalf("secret env name=%q", name)
			}
			return secret, true
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()

	body := []byte(fmt.Sprintf(`{"number":327,"repository":{"full_name":"Ruby-Labs/Relvino"},"pull_request":{"head":{"sha":"%s"}}}`, head))
	request, err := http.NewRequest(http.MethodPost, "http://"+runtime.Address()+"/github", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-GitHub-Delivery", "runtime-delivery")
	request.Header.Set("X-GitHub-Event", "pull_request")
	request.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("status=%d", response.StatusCode)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		wait, getErr := database.GetCIWait(waitID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if wait.Status == db.CIWaitReady {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("wait did not reconcile after webhook: %+v", wait)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-runtime.Errors():
		if err != nil {
			t.Fatalf("clean shutdown error=%v", err)
		}
	default:
	}
}

func TestCoordinatorRestartCustodySurvivesGenericDaemonRecovery(t *testing.T) {
	p := paths.WithRoot(t.TempDir())
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	database, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	repo, err := database.InsertRepo(t.TempDir(), "https://github.com/Ruby-Labs/Relvino.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	head := strings.Repeat("c", 40)
	run, err := database.InsertRun(repo.ID, "review/restart", head, strings.Repeat("0", 40))
	if err != nil {
		t.Fatal(err)
	}
	step, err := database.InsertStepResult(run.ID, types.StepCI)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateStepStatus(step.ID, types.StepStatusRunning); err != nil {
		t.Fatal(err)
	}
	start := time.Now().Truncate(time.Second)
	input := strings.Repeat("d", 64)
	desired, _, _, err := database.AdvanceBranchDesiredState(db.BranchDesiredUpdate{
		RepoID: repo.ID, Branch: run.Branch, HeadSHA: head, InputDigest: input, UpdatedAt: start,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitID, err := database.RegisterCIWait(db.CIWaitSpec{
		RunID: run.ID, RepoID: repo.ID, Branch: run.Branch, PRNumber: 328, HeadSHA: head,
		InputDigest: input, DesiredGeneration: desired.Revision, RegisteredAt: start,
		ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	stale, err := database.InsertRun(repo.ID, "review/stale", strings.Repeat("e", 40), strings.Repeat("0", 40))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(stale.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	recoverable, err := database.RecoverableCIWaitRunIDs()
	if err != nil || len(recoverable) != 1 || recoverable[0] != run.ID {
		t.Fatalf("recoverable=%v err=%v", recoverable, err)
	}

	recoverOnStartup(database, p, NewRunManager(database, p, nil), worktrees.New(p, nil), recoverable)
	preserved, err := database.GetRun(run.ID)
	if err != nil || preserved.Status != types.RunRunning {
		t.Fatalf("coordinator run=%+v err=%v", preserved, err)
	}
	recovered, err := database.GetRun(stale.ID)
	if err != nil || recovered.Status != types.RunFailed {
		t.Fatalf("ordinary stale run=%+v err=%v", recovered, err)
	}
	wait, err := database.GetCIWait(waitID)
	if err != nil || wait.Status != db.CIWaitWaiting {
		t.Fatalf("wait=%+v err=%v", wait, err)
	}
	liveness, err := database.UpdaterPipelineLiveness(time.Now())
	if err != nil || len(liveness.ActiveWorkerLeases) != 0 || liveness.LegacyActiveRowsIgnored != 1 {
		t.Fatalf("liveness=%+v err=%v", liveness, err)
	}
}

func TestAzureWorkerRestartCustodySurvivesGenericDaemonRecovery(t *testing.T) {
	p := paths.WithRoot(t.TempDir())
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	database, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	repo, err := database.InsertRepo(t.TempDir(), "https://github.com/Ruby-Labs/Relvino.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	head := strings.Repeat("a", 40)
	run, err := database.InsertRun(repo.ID, "review/worker-restart", head, strings.Repeat("0", 40))
	if err != nil {
		t.Fatal(err)
	}
	step, err := database.InsertStepResult(run.ID, types.StepReview)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateStepStatus(step.ID, types.StepStatusRunning); err != nil {
		t.Fatal(err)
	}
	input := strings.Repeat("b", 64)
	desired, _, _, err := database.AdvanceWorkerDesiredState(db.BranchDesiredUpdate{
		RepoID: repo.ID, Branch: run.Branch, HeadSHA: head, InputDigest: input, UpdatedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := database.EnqueuePipelineJob(db.PipelineJobSpec{
		RunID: run.ID, StepResultID: step.ID, Kind: db.PipelineJobReview, Round: 1,
		DesiredHeadSHA: head, InputDigest: input, DesiredGeneration: desired.Revision, MaxAttempts: 3,
	}); err != nil {
		t.Fatal(err)
	}
	recoverable, err := database.RecoverablePipelineJobRunIDs()
	if err != nil || len(recoverable) != 1 || recoverable[0] != run.ID {
		t.Fatalf("recoverable worker runs=%v err=%v", recoverable, err)
	}
	recoverOnStartup(database, p, NewRunManager(database, p, nil), worktrees.New(p, nil), recoverable)
	preserved, err := database.GetRun(run.ID)
	if err != nil || preserved.Status != types.RunRunning {
		t.Fatalf("worker run=%+v err=%v", preserved, err)
	}
}

func TestCoordinatorRestartAdoptsExistingRunningCIWithoutExecutionGoroutine(t *testing.T) {
	p := paths.WithRoot(t.TempDir())
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	database, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	repo, err := database.InsertRepo(t.TempDir(), "https://github.com/Ruby-Labs/relvino.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	head := strings.Repeat("a", 40)
	run, err := database.InsertRun(repo.ID, "review/adopt", head, strings.Repeat("0", 40))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunPRURL(run.ID, "https://github.com/Ruby-Labs/relvino/pull/327"); err != nil {
		t.Fatal(err)
	}
	step, err := database.InsertStepResult(run.ID, types.StepCI)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateStepStatus(step.ID, types.StepStatusRunning); err != nil {
		t.Fatal(err)
	}

	resolvedTrustedConfig := false
	adopted, err := adoptExistingCoordinatorCIWaits(database, config.Coordinator{ReconcileInterval: 15 * time.Second}, time.Now(),
		func(gotRun *db.Run, gotRepo *db.Repo) (*config.Config, error) {
			if gotRun.ID != run.ID || gotRepo.ID != repo.ID {
				t.Fatalf("resolver binding run=%s repo=%s", gotRun.ID, gotRepo.ID)
			}
			resolvedTrustedConfig = true
			return &config.Config{NoCI: true}, nil
		})
	if err != nil || adopted != 1 {
		t.Fatalf("adopted=%d err=%v", adopted, err)
	}
	wait, err := database.GetCIWaitForRun(run.ID)
	if err != nil || wait == nil || wait.HeadSHA != head || wait.PRNumber != 327 || wait.IntervalSeconds != 60 || !wait.DeclaredNoCI || !resolvedTrustedConfig {
		t.Fatalf("adopted wait=%+v err=%v", wait, err)
	}
	recoverable, err := database.RecoverableCIWaitRunIDs()
	if err != nil || len(recoverable) != 1 || recoverable[0] != run.ID {
		t.Fatalf("recoverable=%v err=%v", recoverable, err)
	}
	if active := NewRunManager(database, p, nil).ActiveExecutionRunIDs(); len(active) != 0 {
		t.Fatalf("durable wait spawned execution goroutine: %v", active)
	}

	// Existing waits from the pre-binding schema migrate with an explicit
	// unbound marker. Startup must resolve their trusted no_ci/evidence config
	// once, rather than preserving the migration defaults forever.
	legacyRun, err := database.InsertRun(repo.ID, "review/legacy", strings.Repeat("c", 40), strings.Repeat("0", 40))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunPRURL(legacyRun.ID, "https://github.com/Ruby-Labs/relvino/pull/328"); err != nil {
		t.Fatal(err)
	}
	legacyStep, err := database.InsertStepResult(legacyRun.ID, types.StepCI)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(legacyRun.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateStepStatus(legacyStep.ID, types.StepStatusRunning); err != nil {
		t.Fatal(err)
	}
	legacyInput := db.CIWaitInputDigest(repo.ID, legacyRun.Branch, 328, legacyRun.HeadSHA)
	legacyDesired, _, _, err := database.AdvanceBranchDesiredState(db.BranchDesiredUpdate{
		RepoID: repo.ID, Branch: legacyRun.Branch, HeadSHA: legacyRun.HeadSHA,
		InputDigest: legacyInput, UpdatedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	legacyWaitID, err := database.RegisterCIWait(db.CIWaitSpec{
		RunID: legacyRun.ID, RepoID: repo.ID, Branch: legacyRun.Branch, PRNumber: 328,
		HeadSHA: legacyRun.HeadSHA, InputDigest: legacyInput, DesiredGeneration: legacyDesired.Revision,
		RegisteredAt: time.Now(), ReconcileInterval: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	customEvidence := filepath.Join(t.TempDir(), "coordinator-evidence")
	resolvedTrustedConfig = false
	adopted, err = adoptExistingCoordinatorCIWaits(database, config.Coordinator{ReconcileInterval: time.Minute}, time.Now(),
		func(*db.Run, *db.Repo) (*config.Config, error) {
			resolvedTrustedConfig = true
			return &config.Config{NoCI: true, Test: config.Test{Evidence: config.Evidence{LocalRoot: customEvidence}}}, nil
		})
	if err != nil || adopted != 0 || !resolvedTrustedConfig {
		t.Fatalf("legacy binding adopted=%d resolved=%v err=%v", adopted, resolvedTrustedConfig, err)
	}
	wait, err = database.GetCIWait(legacyWaitID)
	if err != nil || !wait.TrustedConfigBound || !wait.DeclaredNoCI || wait.EvidenceLocalRoot != customEvidence {
		t.Fatalf("bound legacy wait=%+v err=%v", wait, err)
	}
}
