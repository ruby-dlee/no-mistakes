package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
	gitpkg "github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/pipeline/steps"
	"github.com/kunchenguid/no-mistakes/internal/types"
	"github.com/kunchenguid/no-mistakes/internal/workertransport"
	"github.com/kunchenguid/no-mistakes/internal/worktrees"
)

func TestAzureWorkerRuntimeExecutesOneExactReviewThroughWrapper(t *testing.T) {
	p := paths.WithRoot(t.TempDir())
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	database, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	repoDir := filepath.Join(t.TempDir(), "repo")
	azureWorkerGit(t, "", "init", "-q", "--initial-branch=main", repoDir)
	azureWorkerGit(t, repoDir, "config", "user.email", "worker@example.invalid")
	azureWorkerGit(t, repoDir, "config", "user.name", "Worker Test")
	if err := os.WriteFile(filepath.Join(repoDir, "source.txt"), []byte("source\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	azureWorkerGit(t, repoDir, "add", "source.txt")
	azureWorkerGit(t, repoDir, "commit", "-qm", "source")
	base := azureWorkerGit(t, repoDir, "rev-parse", "HEAD")
	azureWorkerGit(t, repoDir, "checkout", "-qb", "feature/runtime")
	if err := os.WriteFile(filepath.Join(repoDir, "source.txt"), []byte("source\nfeature\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	azureWorkerGit(t, repoDir, "add", "source.txt")
	azureWorkerGit(t, repoDir, "commit", "-qm", "feature")
	head := azureWorkerGit(t, repoDir, "rev-parse", "HEAD")
	repository, err := database.InsertRepo(repoDir, "https://example.invalid/runtime.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	run, err := database.InsertRun(repository.ID, "feature/runtime", head, strings.Repeat("0", 40))
	if err != nil {
		t.Fatal(err)
	}
	step, err := database.InsertStepResult(run.ID, types.StepReview)
	if err != nil {
		t.Fatal(err)
	}

	runner := filepath.Join(t.TempDir(), "fm-no-mistakes-worker")
	quotedSelf := "'" + strings.ReplaceAll(os.Args[0], "'", "'\\''") + "'"
	if err := os.WriteFile(runner, []byte("#!/bin/sh\nAZURE_WORKER_EXPECT_BASE="+base+" exec "+quotedSelf+" -test.run=^TestFakeAzureWorkerWrapperProcess$ -- \"$@\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	wrapperConfig := filepath.Join(t.TempDir(), "wrapper.json")
	if err := os.WriteFile(wrapperConfig, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runtime, err := newAzureWorkerRuntime(config.AzureWorkerConfig{
		Enabled: true, RunnerPath: runner, ConfigPath: wrapperConfig,
		LeaseDuration: 3 * time.Second, HeartbeatInterval: 200 * time.Millisecond,
		Timeout: 5 * time.Second,
	}, database, p)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runtime.Start(ctx)
	t.Cleanup(func() {
		cancel()
		runtime.Close()
	})

	done := make(chan *pipeline.RemoteStepExecution, 1)
	errCh := make(chan error, 1)
	go func() {
		result, err := runtime.ExecuteRemoteStep(context.Background(), pipeline.RemoteStepRequest{
			RunID: run.ID, RepoID: repository.ID, StepResultID: step.ID,
			Step: types.StepReview, Round: 1, DesiredHeadSHA: head,
			BaseSHA: run.BaseSHA, Branch: run.Branch, DefaultBranch: repository.DefaultBranch, WorkDir: repoDir,
		})
		if err != nil {
			errCh <- err
			return
		}
		done <- result
	}()
	deadline := time.Now().Add(4 * time.Second)
	observedOneLease := false
	for time.Now().Before(deadline) {
		leases, err := database.ActivePipelineWorkerLeases(time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if len(leases) > 1 {
			t.Fatalf("one remote review consumed %d worker leases", len(leases))
		}
		if len(leases) == 1 {
			observedOneLease = true
		}
		select {
		case result := <-done:
			if result.OutputHeadSHA != head || result.Outcome.ReviewApprovedHeadSHA != head {
				t.Fatalf("remote result = %+v", result)
			}
			if leases, err := database.ActivePipelineWorkerLeases(time.Now()); err != nil || len(leases) != 0 {
				t.Fatalf("post-completion leases = %+v, err %v", leases, err)
			}
			if !observedOneLease {
				t.Fatal("review completed without exposing its single fenced lease")
			}
			return
		case err := <-errCh:
			t.Fatal(err)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	t.Fatal("Azure worker review did not complete")
}

func TestAzureWorkerRestart_ReattachesQueuedJob(t *testing.T) {
	t.Setenv("NM_DEMO", "1")
	p := paths.WithRoot(t.TempDir())
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	database, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	repository, head := setupTestGitRepo(t, p, database, "azure-restart")
	run, err := database.InsertRun(repository.ID, "feature/restart", head, strings.Repeat("0", 40))
	if err != nil {
		t.Fatal(err)
	}
	workDir := p.WorktreeDir(repository.ID, run.ID)
	if err := gitpkg.WorktreeAdd(context.Background(), p.RepoDir(repository.ID), workDir, head); err != nil {
		t.Fatal(err)
	}
	if err := database.SetRunWorktreeDir(run.ID, workDir); err != nil {
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

	runner := filepath.Join(t.TempDir(), "fm-no-mistakes-worker")
	quotedSelf := "'" + strings.ReplaceAll(os.Args[0], "'", "'\\''") + "'"
	if err := os.WriteFile(runner, []byte("#!/bin/sh\nexec "+quotedSelf+" -test.run=^TestFakeAzureWorkerWrapperProcess$ -- \"$@\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	wrapperConfig := filepath.Join(t.TempDir(), "wrapper.json")
	if err := os.WriteFile(wrapperConfig, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runtime, err := newAzureWorkerRuntime(config.AzureWorkerConfig{
		Enabled: true, RunnerPath: runner, ConfigPath: wrapperConfig,
		LeaseDuration: 3 * time.Second, HeartbeatInterval: 200 * time.Millisecond, Timeout: 5 * time.Second,
	}, database, p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	request := pipeline.RemoteStepRequest{
		RunID: run.ID, RepoID: repository.ID, StepResultID: step.ID,
		Step: types.StepReview, Round: 1, DesiredHeadSHA: head,
		BaseSHA: run.BaseSHA, Branch: run.Branch, DefaultBranch: repository.DefaultBranch, WorkDir: workDir,
	}
	if _, err := runtime.enqueueRemoteStep(request, db.PipelineJobReview); err != nil {
		t.Fatal(err)
	}
	recoveries, err := runtime.recoverableRemoteSteps(context.Background())
	if err != nil || len(recoveries) != 1 {
		t.Fatalf("recoveries = %+v, err %v", recoveries, err)
	}
	manager := NewRunManager(database, p, func() []pipeline.Step { return []pipeline.Step{&steps.ReviewStep{}} })
	manager.SetRemoteStepRunner(runtime)
	manager.setRemoteRecoveries(recoveries)
	recoverOnStartup(database, p, manager, worktrees.New(p, nil), nil)
	ctx, cancel := context.WithCancel(context.Background())
	runtime.Start(ctx)
	t.Cleanup(func() {
		cancel()
		manager.Shutdown()
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		stored, err := database.GetRun(run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if stored.Status == types.RunCompleted {
			rounds, err := database.GetRoundsByStep(step.ID)
			if err != nil || len(rounds) != 1 {
				t.Fatalf("recovered rounds = %+v, err %v", rounds, err)
			}
			if stored.ReviewApprovedHeadSHA == nil || *stored.ReviewApprovedHeadSHA != head {
				t.Fatalf("recovered review head = %#v", stored.ReviewApprovedHeadSHA)
			}
			return
		}
		if stored.Status == types.RunFailed {
			t.Fatalf("recovered run failed: %v", stored.Error)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("recovered Azure job did not complete its pipeline run")
}

func TestAzureWorkerRecoveryConsumesCompletedAndFailedJobs(t *testing.T) {
	p := paths.WithRoot(t.TempDir())
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	database, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	repository, head := setupTestGitRepo(t, p, database, "azure-terminal-recovery")
	runner := filepath.Join(t.TempDir(), "fm-no-mistakes-worker")
	quotedSelf := "'" + strings.ReplaceAll(os.Args[0], "'", "'\\''") + "'"
	if err := os.WriteFile(runner, []byte("#!/bin/sh\nexec "+quotedSelf+" -test.run=^TestFakeAzureWorkerWrapperProcess$ -- \"$@\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	wrapperConfig := filepath.Join(t.TempDir(), "wrapper.json")
	if err := os.WriteFile(wrapperConfig, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runtime, err := newAzureWorkerRuntime(config.AzureWorkerConfig{
		Enabled: true, RunnerPath: runner, ConfigPath: wrapperConfig,
		LeaseDuration: 3 * time.Second, HeartbeatInterval: 200 * time.Millisecond, Timeout: 5 * time.Second,
	}, database, p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)

	newRequest := func(branch string) (pipeline.RemoteStepRequest, *db.PipelineJob) {
		run, err := database.InsertRun(repository.ID, branch, head, strings.Repeat("0", 40))
		if err != nil {
			t.Fatal(err)
		}
		step, err := database.InsertStepResult(run.ID, types.StepReview)
		if err != nil {
			t.Fatal(err)
		}
		request := pipeline.RemoteStepRequest{
			RunID: run.ID, RepoID: repository.ID, StepResultID: step.ID,
			Step: types.StepReview, Round: 1, DesiredHeadSHA: head,
			BaseSHA: run.BaseSHA, Branch: run.Branch, DefaultBranch: repository.DefaultBranch,
		}
		job, err := runtime.enqueueRemoteStep(request, db.PipelineJobReview)
		if err != nil {
			t.Fatal(err)
		}
		return request, job
	}

	completedRequest, completedJob := newRequest("feature/completed")
	if execution, err := runtime.service.ProcessOne(context.Background(), db.PipelineJobReview); err != nil || execution == nil {
		t.Fatalf("complete seeded job: execution=%+v err=%v", execution, err)
	}
	completedRequest.RecoveryJobID = completedJob.ID
	result, err := runtime.ExecuteRemoteStep(context.Background(), completedRequest)
	if err != nil || result.JobID != completedJob.ID || result.OutputHeadSHA != head {
		t.Fatalf("completed recovery = %+v, err %v", result, err)
	}

	failedRequest, failedJob := newRequest("feature/failed")
	lease, err := database.ClaimPipelineJob(db.PipelineJobReview, "failed-worker", time.Now(), time.Minute)
	if err != nil || lease == nil || lease.ID != failedJob.ID {
		t.Fatalf("claim failed-job fixture = %+v, err %v", lease, err)
	}
	if _, err := database.FailPipelineJob(db.PipelineJobFailure{
		JobID: failedJob.ID, LeaseOwner: "failed-worker", LeaseFence: lease.LeaseFence,
		DesiredHeadSHA: failedJob.DesiredHeadSHA, InputDigest: failedJob.InputDigest,
		OwnerDecisionHead: failedJob.OwnerDecisionHead, DesiredGeneration: failedJob.DesiredGeneration,
		ErrorCategory: "wrapper_failure", Retryable: false, FailedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	failedRequest.RecoveryJobID = failedJob.ID
	if _, err := runtime.ExecuteRemoteStep(context.Background(), failedRequest); err == nil || !strings.Contains(err.Error(), "wrapper_failure") {
		t.Fatalf("failed recovery error = %v", err)
	}
}

func TestFakeAzureWorkerWrapperProcess(t *testing.T) {
	separator := -1
	for index, arg := range os.Args {
		if arg == "--" {
			separator = index
			break
		}
	}
	if separator < 0 {
		return
	}
	args := os.Args[separator+1:]
	value := func(flag string) string {
		for index := 0; index+1 < len(args); index++ {
			if args[index] == flag {
				return args[index+1]
			}
		}
		return ""
	}
	var request workertransport.Request
	azureWorkerReadJSON(t, value("--request"), &request)
	if expectedBase := os.Getenv("AZURE_WORKER_EXPECT_BASE"); expectedBase != "" {
		var input workertransport.StepInputEnvelope
		azureWorkerReadJSON(t, filepath.Join(value("--payload"), "brief.md"), &input)
		if input.BaseSHA != expectedBase {
			t.Fatalf("worker base = %q, want resolved branch base %q", input.BaseSHA, expectedBase)
		}
	}
	outcome := workertransport.StepOutcomeEnvelope{
		Schema: workertransport.StepOutcomeSchema, Step: workertransport.StepOutcomeReview,
		ReviewApprovedHeadSHA: request.DesiredHeadSHA,
	}
	outcomeBytes, err := json.Marshal(outcome)
	if err != nil {
		t.Fatal(err)
	}
	outcomeBytes = append(outcomeBytes, '\n')
	if err := os.WriteFile(value("--step-outcome"), outcomeBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	outcomeDigest := sha256.Sum256(outcomeBytes)
	result := workertransport.ResultEnvelope{
		Schema: workertransport.ResultSchema, JobID: request.JobID, RunID: request.RunID,
		StepResultID: request.StepResultID, Step: request.Step, Kind: request.Kind, Round: request.Round,
		DesiredHeadSHA: request.DesiredHeadSHA, InputDigest: request.InputDigest,
		RuntimeIdentity:   request.RuntimeIdentity,
		OwnerDecisionHead: request.OwnerDecisionHead, DesiredGeneration: request.DesiredGeneration,
		Attempt: request.Attempt, LeaseFence: request.LeaseFence, LeaseOwner: request.LeaseOwner,
		SourceBundleSHA256: request.SourceBundleSHA256, Outcome: "succeeded",
		OutputHeadSHA: request.DesiredHeadSHA, StepOutcomeSHA256: hex.EncodeToString(outcomeDigest[:]),
	}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(value("--result"), append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func azureWorkerReadJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, value); err != nil {
		t.Fatal(err)
	}
}

func azureWorkerGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = dir
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, output)
	}
	return strings.TrimSpace(string(output))
}
