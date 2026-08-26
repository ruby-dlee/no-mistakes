package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/agent"
	"github.com/kunchenguid/no-mistakes/internal/types"
	"github.com/kunchenguid/no-mistakes/internal/workertransport"
)

type workerScriptAgent struct {
	runs func(context.Context, agent.RunOpts) (*agent.Result, error)
}

func (a *workerScriptAgent) Name() string { return "pi" }
func (a *workerScriptAgent) Run(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
	return a.runs(ctx, opts)
}
func (a *workerScriptAgent) Close() error { return nil }

type workerHarness struct {
	repo, brief, result, base, head string
}

func newWorkerHarness(t *testing.T, configuredTest bool, timeout string) workerHarness {
	t.Helper()
	repo := t.TempDir()
	workerGit(t, repo, "init", "-b", "main")
	workerGit(t, repo, "config", "user.name", "worker-test")
	workerGit(t, repo, "config", "user.email", "worker@test.invalid")
	if err := os.WriteFile(filepath.Join(repo, "app.txt"), []byte("bad\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if configuredTest {
		if err := os.WriteFile(filepath.Join(repo, ".no-mistakes.yaml"), []byte("allow_repo_commands: true\ncommands:\n  test: grep -q good app.txt\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(repo, "app.sh"), []byte("#!/bin/sh\ncat app.txt\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "check-app.sh"), []byte("#!/bin/sh\n[ \"$(./app.sh)\" = good ]\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	workerGit(t, repo, "add", ".")
	workerGit(t, repo, "commit", "-m", "base")
	base := workerGit(t, repo, "rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(repo, "app.txt"), []byte("bad\ntarget\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	workerGit(t, repo, "add", "app.txt")
	workerGit(t, repo, "commit", "-m", "target")
	head := workerGit(t, repo, "rev-parse", "HEAD")

	home := t.TempDir()
	configYAML := "agent: pi\nagent_path_override:\n  pi: /usr/bin/true\n"
	if timeout != "" {
		configYAML += "review_agent_timeout: " + timeout + "\ntest_agent_timeout: " + timeout + "\n"
	}
	if err := os.WriteFile(filepath.Join(home, "config.yaml"), []byte(configYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("NM_HOME", home)
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(repo); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
	out := t.TempDir()
	return workerHarness{repo: repo, brief: filepath.Join(out, "brief.json"), result: filepath.Join(out, "result.json"), base: base, head: head}
}

func (h workerHarness) writeBrief(t *testing.T, step types.StepName, fixing bool, previous string) {
	t.Helper()
	input := workertransport.StepInputEnvelope{
		Schema: workertransport.StepInputSchema, RunID: "run-1", RepoID: "repo-1", StepResultID: "step-1",
		Step: step, Round: 1, DesiredHeadSHA: h.head, BaseSHA: h.base, Branch: "feature", DefaultBranch: "trunk",
		Fixing: fixing, PreviousFindings: previous,
	}
	if fixing && step == types.StepReview {
		input.RepairAttempt = 1
		input.QualityOutcomeAuthority = "semantic-rereview"
	}
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(h.brief, append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func runWorkerCommand(t *testing.T, h workerHarness, role string, scripted *workerScriptAgent) error {
	t.Helper()
	deps := workerRunDeps{newAgent: func(types.AgentName, string, []string, agent.Options) (agent.Agent, error) { return scripted, nil }}
	cmd := newWorkerRunCmd(deps)
	cmd.SetArgs([]string{"--role", role, "--brief", h.brief, "--result", h.result})
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	return cmd.ExecuteContext(context.Background())
}

func readWorkerOutcome(t *testing.T, path string) workertransport.StepOutcomeEnvelope {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var outcome workertransport.StepOutcomeEnvelope
	if err := json.Unmarshal(data, &outcome); err != nil {
		t.Fatal(err)
	}
	return outcome
}

func TestWorkerRunReviewFindingSurvivesClosedOutcome(t *testing.T) {
	h := newWorkerHarness(t, false, "")
	h.writeBrief(t, types.StepReview, false, "")
	ag := &workerScriptAgent{runs: func(context.Context, agent.RunOpts) (*agent.Result, error) {
		return &agent.Result{Output: []byte(`{"findings":[{"severity":"warning","description":"real bug","action":"auto-fix","review_scope":"source","semantic_family":"local-mechanical","semantic_root":"input validation"}],"risk_level":"medium","risk_rationale":"bug","risk_scope":"source-or-external"}`)}, nil
	}}
	if err := runWorkerCommand(t, h, "review", ag); err != nil {
		t.Fatal(err)
	}
	out := readWorkerOutcome(t, h.result)
	if !out.NeedsApproval || !strings.Contains(out.FindingsJSON, "real bug") || out.ReviewApprovedHeadSHA != h.head {
		t.Fatalf("outcome = %+v", out)
	}
}

func TestWorkerRunReviewClearBindsExactHead(t *testing.T) {
	h := newWorkerHarness(t, false, "")
	h.writeBrief(t, types.StepReview, false, "")
	ag := &workerScriptAgent{runs: func(context.Context, agent.RunOpts) (*agent.Result, error) {
		return &agent.Result{Output: []byte(`{"findings":[],"risk_level":"low","risk_rationale":"clear","risk_scope":"source-or-external"}`)}, nil
	}}
	if err := runWorkerCommand(t, h, "review", ag); err != nil {
		t.Fatal(err)
	}
	out := readWorkerOutcome(t, h.result)
	if out.NeedsApproval || out.ReviewApprovedHeadSHA != h.head {
		t.Fatalf("outcome = %+v", out)
	}
}

func TestWorkerRunTestFindingSurvivesClosedOutcome(t *testing.T) {
	h := newWorkerHarness(t, false, "")
	h.writeBrief(t, types.StepTest, false, "")
	ag := &workerScriptAgent{runs: func(context.Context, agent.RunOpts) (*agent.Result, error) {
		return &agent.Result{Output: []byte(`{"findings":[{"severity":"error","description":"focused test failed","action":"auto-fix"}],"summary":"failed","tested":["focused"],"testing_summary":"failed","artifacts":[]}`)}, nil
	}}
	if err := runWorkerCommand(t, h, "test", ag); err != nil {
		t.Fatal(err)
	}
	out := readWorkerOutcome(t, h.result)
	if !out.NeedsApproval || !strings.Contains(out.FindingsJSON, "focused test failed") || out.ReviewApprovedHeadSHA != "" {
		t.Fatalf("outcome = %+v", out)
	}
}

func TestWorkerRunReviewRepairCommitsDescendantAndRereviews(t *testing.T) {
	h := newWorkerHarness(t, true, "")
	previous := `{"findings":[{"severity":"warning","description":"bad value","action":"auto-fix","review_scope":"source","semantic_family":"local-mechanical","semantic_root":"value"}],"risk_level":"medium","risk_rationale":"bad","risk_scope":"source-or-external"}`
	h.writeBrief(t, types.StepReview, true, previous)
	calls := 0
	ag := &workerScriptAgent{runs: func(_ context.Context, opts agent.RunOpts) (*agent.Result, error) {
		calls++
		if calls == 1 {
			if err := os.WriteFile(filepath.Join(opts.CWD, "app.txt"), []byte("good\n"), 0o644); err != nil {
				return nil, err
			}
			return &agent.Result{Output: []byte(`{"summary":"fix value","repair_complete":true,"semantic_family":"local-mechanical","semantic_root":"value","public_executable_check":"./check-app.sh","integration_consumer_check":"./check-app.sh","generated_artifacts":{"touched":false,"source_updated":false,"emitter_available":false,"emitter_run":false,"disposition":"none"}}`)}, nil
		}
		return &agent.Result{Output: []byte(`{"findings":[],"risk_level":"low","risk_rationale":"clear","risk_scope":"source-or-external"}`)}, nil
	}}
	if err := runWorkerCommand(t, h, "repair", ag); err != nil {
		t.Fatal(err)
	}
	out := readWorkerOutcome(t, h.result)
	newHead := workerGit(t, h.repo, "rev-parse", "HEAD")
	if newHead == h.head || out.ReviewApprovedHeadSHA != newHead || calls != 2 || out.QualityOutcome == nil || out.QualityOutcome.Classification != "clean_fix" {
		t.Fatalf("head=%s outcome=%+v calls=%d", newHead, out, calls)
	}
	workerGit(t, h.repo, "merge-base", "--is-ancestor", h.head, newHead)
}

func TestWorkerRunTestRepairCommitsDescendant(t *testing.T) {
	h := newWorkerHarness(t, true, "")
	h.writeBrief(t, types.StepTest, true, `{"findings":[{"severity":"error","description":"test failed","action":"auto-fix"}],"summary":"failed"}`)
	ag := &workerScriptAgent{runs: func(_ context.Context, opts agent.RunOpts) (*agent.Result, error) {
		if err := os.WriteFile(filepath.Join(opts.CWD, "app.txt"), []byte("good\n"), 0o644); err != nil {
			return nil, err
		}
		return &agent.Result{Output: []byte(`{"summary":"fix test"}`)}, nil
	}}
	if err := runWorkerCommand(t, h, "repair", ag); err != nil {
		t.Fatal(err)
	}
	out := readWorkerOutcome(t, h.result)
	newHead := workerGit(t, h.repo, "rev-parse", "HEAD")
	if newHead == h.head || out.Step != workertransport.StepOutcomeTest || out.ReviewApprovedHeadSHA != "" {
		t.Fatalf("head=%s outcome=%+v", newHead, out)
	}
	workerGit(t, h.repo, "merge-base", "--is-ancestor", h.head, newHead)
}

func TestWorkerRunRejectsMalformedInputWithoutOutcome(t *testing.T) {
	h := newWorkerHarness(t, false, "")
	if err := os.WriteFile(h.brief, []byte(`{"schema":"wrong"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	ag := &workerScriptAgent{runs: func(context.Context, agent.RunOpts) (*agent.Result, error) { return nil, errors.New("must not run") }}
	if err := runWorkerCommand(t, h, "review", ag); err == nil {
		t.Fatal("expected malformed input rejection")
	}
	if _, err := os.Stat(h.result); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("result exists after refusal: %v", err)
	}
}

func TestWorkerRunTimeoutWritesNoFabricatedClear(t *testing.T) {
	h := newWorkerHarness(t, false, "20ms")
	h.writeBrief(t, types.StepReview, false, "")
	ag := &workerScriptAgent{runs: func(ctx context.Context, _ agent.RunOpts) (*agent.Result, error) { <-ctx.Done(); return nil, ctx.Err() }}
	err := runWorkerCommand(t, h, "review", ag)
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("error = %v", err)
	}
	if _, err := os.Stat(h.result); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("result exists after timeout: %v", err)
	}
}

func workerGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}
