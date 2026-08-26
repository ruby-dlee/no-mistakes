package workertransport

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

type fakeWrapperConfig struct {
	Mode          string `json:"mode"`
	Marker        string `json:"marker,omitempty"`
	OutcomeBundle string `json:"outcome_bundle,omitempty"`
	OutputHead    string `json:"output_head,omitempty"`
	ReturnRef     string `json:"return_ref,omitempty"`
}

type transportFixture struct {
	database *db.DB
	job      *db.PipelineJob
	repo     string
	input    []byte
	runner   string
	config   string
	workRoot string
}

func newTransportFixture(t *testing.T, kind db.PipelineJobKind, maxAttempts int, wrapper fakeWrapperConfig) transportFixture {
	t.Helper()
	repo := filepath.Join(t.TempDir(), "repo")
	runGitTest(t, "", "init", "-q", repo)
	runGitTest(t, repo, "config", "user.email", "worker@example.invalid")
	runGitTest(t, repo, "config", "user.name", "Worker Test")
	if err := os.WriteFile(filepath.Join(repo, "source.txt"), []byte("source\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGitTest(t, repo, "add", "source.txt")
	runGitTest(t, repo, "commit", "-qm", "source")
	head := runGitTest(t, repo, "rev-parse", "HEAD")

	database, err := db.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	repository, err := database.InsertRepo(repo, "https://example.invalid/worker.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	run, err := database.InsertRun(repository.ID, "feature/worker", head, strings.Repeat("0", 40))
	if err != nil {
		t.Fatal(err)
	}
	step, err := database.InsertStepResult(run.ID, types.StepReview)
	if err != nil {
		t.Fatal(err)
	}
	input := []byte("bounded worker instructions\n")
	inputSum := sha256.Sum256(input)
	job, _, err := database.EnqueuePipelineJob(db.PipelineJobSpec{
		RunID: run.ID, StepResultID: step.ID, Kind: kind, Round: 1,
		DesiredHeadSHA: head, InputDigest: hex.EncodeToString(inputSum[:]), MaxAttempts: maxAttempts,
	})
	if err != nil {
		t.Fatal(err)
	}

	control := filepath.Join(t.TempDir(), "wrapper.json")
	writeJSONTest(t, control, wrapper, 0o600)
	runner := filepath.Join(t.TempDir(), "fm-no-mistakes-worker")
	quotedSelf := "'" + strings.ReplaceAll(os.Args[0], "'", "'\\''") + "'"
	script := "#!/bin/sh\nexec " + quotedSelf + " -test.run=^TestFakeFirstmateWrapperProcess$ -- \"$@\"\n"
	if err := os.WriteFile(runner, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return transportFixture{database: database, job: job, repo: repo, input: input, runner: runner, config: control, workRoot: t.TempDir()}
}

func (f transportFixture) service(t *testing.T, timeout, heartbeat, lease time.Duration) *Service {
	t.Helper()
	service, err := New(f.database, config.AzureWorkerConfig{
		Enabled: true, RunnerPath: f.runner, ConfigPath: f.config,
		Timeout: timeout, HeartbeatInterval: heartbeat, LeaseDuration: lease,
	}, f.workRoot, "transport-test", InputProviderFunc(func(_ context.Context, job *db.PipelineJob) ([]byte, error) {
		if job.ID != f.job.ID {
			return nil, fmt.Errorf("unexpected job %s", job.ID)
		}
		return append([]byte(nil), f.input...), nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestAzureWorkerTransportReviewSuccess(t *testing.T) {
	fixture := newTransportFixture(t, db.PipelineJobReview, 2, fakeWrapperConfig{Mode: "success"})
	result, err := fixture.service(t, 5*time.Second, 200*time.Millisecond, 3*time.Second).ProcessOne(context.Background(), db.PipelineJobReview)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.JobID != fixture.job.ID || result.OutputHeadSHA != fixture.job.DesiredHeadSHA || result.ReturnedBranch != "" {
		t.Fatalf("result = %+v", result)
	}
	if result.StepOutcome.Schema != StepOutcomeSchema || result.StepOutcome.Step != StepOutcomeReview ||
		result.StepOutcome.ReviewApprovedHeadSHA != fixture.job.DesiredHeadSHA {
		t.Fatalf("step outcome = %+v", result.StepOutcome)
	}
	stored, err := fixture.database.GetPipelineJob(fixture.job.ID)
	if err != nil || stored.Status != db.PipelineJobCompleted || stored.ResultDigest == nil {
		t.Fatalf("stored job = %+v, err %v", stored, err)
	}
}

func TestAzureWorkerTransportPreservesBlockingReviewFindings(t *testing.T) {
	fixture := newTransportFixture(t, db.PipelineJobReview, 2, fakeWrapperConfig{Mode: "findings"})
	result, err := fixture.service(t, 5*time.Second, 200*time.Millisecond, 3*time.Second).ProcessOne(context.Background(), db.PipelineJobReview)
	if err != nil {
		t.Fatal(err)
	}
	if !result.StepOutcome.NeedsApproval || !result.StepOutcome.AutoFixable ||
		!strings.Contains(result.StepOutcome.FindingsJSON, "remote blocker") {
		t.Fatalf("remote blocking finding was collapsed: %+v", result.StepOutcome)
	}
}

func TestAzureWorkerTransportRejectsMalformedAndStaleResults(t *testing.T) {
	for _, mode := range []string{"malformed", "stale"} {
		t.Run(mode, func(t *testing.T) {
			fixture := newTransportFixture(t, db.PipelineJobReview, 2, fakeWrapperConfig{Mode: mode})
			if _, err := fixture.service(t, 5*time.Second, 200*time.Millisecond, 3*time.Second).ProcessOne(context.Background(), db.PipelineJobReview); err == nil {
				t.Fatal("invalid result was admitted")
			}
			stored, err := fixture.database.GetPipelineJob(fixture.job.ID)
			if err != nil || stored.Status != db.PipelineJobFailed {
				t.Fatalf("stored job = %+v, err %v", stored, err)
			}
		})
	}
}

func TestAzureWorkerTransportTimeoutRequeuesWithinBudget(t *testing.T) {
	fixture := newTransportFixture(t, db.PipelineJobReview, 2, fakeWrapperConfig{Mode: "sleep"})
	// Leave enough time for the exact-source bundle on a loaded test host; the
	// fake wrapper itself sleeps five seconds, so the two-second bound still
	// deterministically exercises wrapper timeout rather than prep timeout.
	if _, err := fixture.service(t, 2*time.Second, 100*time.Millisecond, 3*time.Second).ProcessOne(context.Background(), db.PipelineJobReview); err == nil {
		t.Fatal("timed-out wrapper succeeded")
	}
	stored, err := fixture.database.GetPipelineJob(fixture.job.ID)
	if err != nil || stored.Status != db.PipelineJobQueued || stored.ErrorCategory == nil || *stored.ErrorCategory != "wrapper_timeout" {
		t.Fatalf("stored job = %+v, err %v", stored, err)
	}
}

func TestAzureWorkerTransportHeartbeatLossStopsWithoutAdmitting(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "started")
	fixture := newTransportFixture(t, db.PipelineJobReview, 2, fakeWrapperConfig{Mode: "sleep", Marker: marker})
	service := fixture.service(t, 5*time.Second, 50*time.Millisecond, 2*time.Second)
	done := make(chan error, 1)
	go func() {
		_, err := service.ProcessOne(context.Background(), db.PipelineJobReview)
		done <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("wrapper did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	leased, err := fixture.database.GetPipelineJob(fixture.job.ID)
	if err != nil || leased.Status != db.PipelineJobLeased {
		t.Fatalf("leased job = %+v, err %v", leased, err)
	}
	if _, err := fixture.database.SupersedePipelineJob(db.PipelineJobSupersession{
		JobID: leased.ID, DesiredHeadSHA: leased.DesiredHeadSHA, InputDigest: leased.InputDigest,
		OwnerDecisionHead: leased.OwnerDecisionHead, DesiredGeneration: leased.DesiredGeneration,
		SupersededAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil || !strings.Contains(err.Error(), "heartbeat") {
		t.Fatalf("heartbeat loss error = %v", err)
	}
	stored, _ := fixture.database.GetPipelineJob(fixture.job.ID)
	if stored.Status != db.PipelineJobSuperseded || stored.ResultDigest != nil {
		t.Fatalf("stored job = %+v", stored)
	}
}

func TestAzureWorkerTransportMaterializesRepairBranch(t *testing.T) {
	fixture := newTransportFixture(t, db.PipelineJobRepair, 2, fakeWrapperConfig{Mode: "pending"})
	clone := filepath.Join(t.TempDir(), "repair")
	runGitTest(t, "", "clone", "-q", fixture.repo, clone)
	runGitTest(t, clone, "config", "user.email", "worker@example.invalid")
	runGitTest(t, clone, "config", "user.name", "Worker Test")
	if err := os.WriteFile(filepath.Join(clone, "repair.txt"), []byte("repair\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGitTest(t, clone, "add", "repair.txt")
	runGitTest(t, clone, "commit", "-qm", "repair")
	outputHead := runGitTest(t, clone, "rev-parse", "HEAD")
	returnRef := "refs/heads/fm-repair-result"
	runGitTest(t, clone, "branch", "fm-repair-result", outputHead)
	bundle := filepath.Join(t.TempDir(), "repair.bundle")
	runGitTest(t, clone, "bundle", "create", bundle, returnRef)
	writeJSONTest(t, fixture.config, fakeWrapperConfig{
		Mode: "repair", OutcomeBundle: bundle, OutputHead: outputHead, ReturnRef: returnRef,
	}, 0o600)

	result, err := fixture.service(t, 5*time.Second, 200*time.Millisecond, 3*time.Second).ProcessOne(context.Background(), db.PipelineJobRepair)
	if err != nil {
		t.Fatal(err)
	}
	if result.ReturnedBranch == "" || result.OutputHeadSHA != outputHead {
		t.Fatalf("repair result = %+v", result)
	}
	if got := runGitTest(t, fixture.repo, "rev-parse", result.ReturnedBranch); got != outputHead {
		t.Fatalf("returned branch head = %s, want %s", got, outputHead)
	}
	if got := runGitTest(t, fixture.repo, "rev-parse", "HEAD"); got != fixture.job.DesiredHeadSHA {
		t.Fatalf("worktree HEAD moved to %s", got)
	}
	if got := runGitTest(t, fixture.repo, "status", "--porcelain=v1", "--untracked-files=all"); got != "" {
		t.Fatalf("worktree dirtied: %q", got)
	}
}

func TestFakeFirstmateWrapperProcess(t *testing.T) {
	separator := -1
	for i, arg := range os.Args {
		if arg == "--" {
			separator = i
			break
		}
	}
	if separator < 0 {
		return
	}
	args := os.Args[separator+1:]
	value := func(flag string) string {
		for i := 0; i+1 < len(args); i++ {
			if args[i] == flag {
				return args[i+1]
			}
		}
		return ""
	}
	var control fakeWrapperConfig
	readJSONTest(t, value("--config"), &control)
	if control.Marker != "" {
		if err := os.WriteFile(control.Marker, []byte("started\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if control.Mode == "sleep" {
		time.Sleep(5 * time.Second)
		return
	}
	resultPath := value("--result")
	if control.Mode == "malformed" {
		if err := os.WriteFile(resultPath, []byte("{\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return
	}
	var request Request
	readJSONTest(t, value("--request"), &request)
	result := ResultEnvelope{
		Schema: ResultSchema, JobID: request.JobID, RunID: request.RunID,
		StepResultID: request.StepResultID, Kind: request.Kind, Round: request.Round,
		DesiredHeadSHA: request.DesiredHeadSHA, InputDigest: request.InputDigest,
		OwnerDecisionHead: request.OwnerDecisionHead, DesiredGeneration: request.DesiredGeneration,
		Attempt: request.Attempt, LeaseFence: request.LeaseFence, LeaseOwner: request.LeaseOwner,
		SourceBundleSHA256: request.SourceBundleSHA256,
		Outcome:            "succeeded", OutputHeadSHA: request.DesiredHeadSHA,
	}
	if control.Mode == "stale" {
		result.JobID += "-stale"
	}
	if control.Mode == "repair" {
		contents, err := os.ReadFile(control.OutcomeBundle)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(value("--outcome"), contents, 0o600); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(contents)
		result.OutputHeadSHA = control.OutputHead
		result.ReturnRef = control.ReturnRef
		result.ReturnBundleSHA256 = hex.EncodeToString(sum[:])
	}
	step := StepOutcomeReview
	if request.Kind == db.PipelineJobTest {
		step = StepOutcomeTest
	}
	stepOutcome := StepOutcomeEnvelope{
		Schema: StepOutcomeSchema, Step: step,
		ReviewApprovedHeadSHA: result.OutputHeadSHA,
	}
	if step == StepOutcomeTest {
		stepOutcome.ReviewApprovedHeadSHA = ""
	}
	if control.Mode == "findings" {
		stepOutcome.NeedsApproval = true
		stepOutcome.AutoFixable = true
		stepOutcome.FindingsJSON = `{"findings":[{"severity":"error","description":"remote blocker","action":"auto-fix"}],"summary":"1 issue"}`
	}
	stepOutcomeBytes, err := json.Marshal(stepOutcome)
	if err != nil {
		t.Fatal(err)
	}
	stepOutcomeBytes = append(stepOutcomeBytes, '\n')
	if err := os.WriteFile(value("--step-outcome"), stepOutcomeBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	stepOutcomeDigest := sha256.Sum256(stepOutcomeBytes)
	result.StepOutcomeSHA256 = hex.EncodeToString(stepOutcomeDigest[:])
	writeJSONTest(t, resultPath, result, 0o600)
}

func runGitTest(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func writeJSONTest(t *testing.T, path string, value any, mode os.FileMode) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), mode); err != nil {
		t.Fatal(err)
	}
}

func readJSONTest(t *testing.T, path string, value any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, value); err != nil {
		t.Fatal(err)
	}
}
