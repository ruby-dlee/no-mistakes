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
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/types"
	"github.com/kunchenguid/no-mistakes/internal/workertransport"
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
	azureWorkerGit(t, "", "init", "-q", repoDir)
	azureWorkerGit(t, repoDir, "config", "user.email", "worker@example.invalid")
	azureWorkerGit(t, repoDir, "config", "user.name", "Worker Test")
	if err := os.WriteFile(filepath.Join(repoDir, "source.txt"), []byte("source\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	azureWorkerGit(t, repoDir, "add", "source.txt")
	azureWorkerGit(t, repoDir, "commit", "-qm", "source")
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
	if err := os.WriteFile(runner, []byte("#!/bin/sh\nexec "+quotedSelf+" -test.run=^TestFakeAzureWorkerWrapperProcess$ -- \"$@\"\n"), 0o700); err != nil {
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
			BaseSHA: run.BaseSHA, Branch: run.Branch, WorkDir: repoDir,
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
