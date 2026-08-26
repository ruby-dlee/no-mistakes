package pipeline

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

type remoteStepRunnerFunc func(context.Context, RemoteStepRequest) (*RemoteStepExecution, error)

func (f remoteStepRunnerFunc) ExecuteRemoteStep(ctx context.Context, request RemoteStepRequest) (*RemoteStepExecution, error) {
	return f(ctx, request)
}

func TestExecutorUsesRemoteReviewAndPreservesSemanticFinding(t *testing.T) {
	database, p, run, repo := setupTest(t)
	local := newFailStep(types.StepReview, nil)
	executor := NewExecutor(database, p, nil, nil, []Step{local}, nil)
	executor.SetRemoteStepRunner(remoteStepRunnerFunc(func(_ context.Context, request RemoteStepRequest) (*RemoteStepExecution, error) {
		if request.Step != types.StepReview || request.RunID != run.ID || request.Round != 1 || request.Fixing {
			t.Fatalf("remote request = %+v", request)
		}
		return &RemoteStepExecution{
			Outcome: StepOutcome{
				Findings:              `{"findings":[{"severity":"info","description":"remote evidence","action":"none"}],"summary":"1 note"}`,
				ReviewApprovedHeadSHA: run.HeadSHA,
			},
			OutputHeadSHA: run.HeadSHA,
		}, nil
	}))
	if err := executor.Execute(context.Background(), run, repo, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if local.callCount() != 0 {
		t.Fatalf("local review executed %d times", local.callCount())
	}
	steps, err := database.GetStepsByRun(run.ID)
	if err != nil || len(steps) != 1 || steps[0].FindingsJSON == nil || !strings.Contains(*steps[0].FindingsJSON, "remote evidence") {
		t.Fatalf("stored remote finding = %+v, err %v", steps, err)
	}
}

func TestAdoptRemoteRepairFastForwardsExactHeadAndRejectsStaleRunCAS(t *testing.T) {
	for _, stale := range []bool{false, true} {
		name := "success"
		if stale {
			name = "stale-cas-retains-adopted-custody"
		}
		t.Run(name, func(t *testing.T) {
			repoDir := filepath.Join(t.TempDir(), "repo")
			pipelineGit(t, "", "init", "-q", repoDir)
			pipelineGit(t, repoDir, "config", "user.email", "worker@example.invalid")
			pipelineGit(t, repoDir, "config", "user.name", "Worker Test")
			if err := os.WriteFile(filepath.Join(repoDir, "source.txt"), []byte("source\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			pipelineGit(t, repoDir, "add", "source.txt")
			pipelineGit(t, repoDir, "commit", "-qm", "source")
			oldHead := pipelineGit(t, repoDir, "rev-parse", "HEAD")
			if err := os.WriteFile(filepath.Join(repoDir, "repair.txt"), []byte("repair\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			pipelineGit(t, repoDir, "add", "repair.txt")
			pipelineGit(t, repoDir, "commit", "-qm", "repair")
			newHead := pipelineGit(t, repoDir, "rev-parse", "HEAD")
			resultBranch := "no-mistakes/azure-results/test-1"
			pipelineGit(t, repoDir, "branch", resultBranch, newHead)
			pipelineGit(t, repoDir, "reset", "--hard", oldHead)

			database, p, run, _ := setupTest(t)
			if err := database.UpdateRunHeadSHA(run.ID, oldHead); err != nil {
				t.Fatal(err)
			}
			if stale {
				if err := database.UpdateRunHeadSHA(run.ID, strings.Repeat("f", 40)); err != nil {
					t.Fatal(err)
				}
			}
			executor := NewExecutor(database, p, nil, nil, nil, nil)
			runHead := oldHead
			err := executor.adoptRemoteRepair(context.Background(), RemoteStepRequest{
				RunID: run.ID, DesiredHeadSHA: oldHead, WorkDir: repoDir, Fixing: true,
			}, &RemoteStepExecution{OutputHeadSHA: newHead, ReturnedBranch: resultBranch}, &runHead)
			if stale {
				if err == nil || pipelineGit(t, repoDir, "rev-parse", "HEAD") != newHead || runHead != oldHead {
					t.Fatalf("stale adoption err=%v worktree=%s run=%s", err, pipelineGit(t, repoDir, "rev-parse", "HEAD"), runHead)
				}
				if got := pipelineGit(t, repoDir, "rev-parse", resultBranch); got != newHead {
					t.Fatalf("retained result branch = %s, want %s", got, newHead)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := pipelineGit(t, repoDir, "rev-parse", "HEAD"); got != newHead || runHead != newHead {
				t.Fatalf("adopted worktree=%s run=%s want=%s", got, runHead, newHead)
			}
			stored, _ := database.GetRun(run.ID)
			if stored.HeadSHA != newHead {
				t.Fatalf("stored head = %s", stored.HeadSHA)
			}
		})
	}
}

func TestExecuteRemoteReviewRepairRecordsAuthorizedQualityOutcome(t *testing.T) {
	for _, missingQuality := range []bool{false, true} {
		name := "records-quality"
		if missingQuality {
			name = "missing-quality-rolls-back"
		}
		t.Run(name, func(t *testing.T) {
			repoDir := filepath.Join(t.TempDir(), "repo")
			pipelineGit(t, "", "init", "-q", repoDir)
			pipelineGit(t, repoDir, "config", "user.email", "worker@example.invalid")
			pipelineGit(t, repoDir, "config", "user.name", "Worker Test")
			if err := os.WriteFile(filepath.Join(repoDir, "source.txt"), []byte("source\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			pipelineGit(t, repoDir, "add", "source.txt")
			pipelineGit(t, repoDir, "commit", "-qm", "source")
			oldHead := pipelineGit(t, repoDir, "rev-parse", "HEAD")
			if err := os.WriteFile(filepath.Join(repoDir, "source.txt"), []byte("fixed\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			pipelineGit(t, repoDir, "add", "source.txt")
			pipelineGit(t, repoDir, "commit", "-qm", "repair")
			newHead := pipelineGit(t, repoDir, "rev-parse", "HEAD")
			resultBranch := "no-mistakes/azure-results/quality-1"
			pipelineGit(t, repoDir, "branch", resultBranch, newHead)
			pipelineGit(t, repoDir, "reset", "--hard", oldHead)

			database, p, run, _ := setupTest(t)
			if err := database.UpdateRunHeadSHA(run.ID, oldHead); err != nil {
				t.Fatal(err)
			}
			attempt, root := "review-fix-1", "auth-root"
			var quality *db.QualityOutcome
			if !missingQuality {
				quality = &db.QualityOutcome{
					FixAttemptID: &attempt, RootID: &root, Classification: db.QualityCleanFix,
					FixedHeadSHA: newHead, ObservedHeadSHA: newHead,
					EvidenceDigest: "sha256:" + strings.Repeat("0", 64), EvidenceProvenance: "semantic_rereview",
				}
			}
			executor := NewExecutor(database, p, nil, nil, nil, nil)
			executor.SetRemoteStepRunner(remoteStepRunnerFunc(func(_ context.Context, request RemoteStepRequest) (*RemoteStepExecution, error) {
				return &RemoteStepExecution{
					Outcome: StepOutcome{ReviewApprovedHeadSHA: newHead}, OutputHeadSHA: newHead,
					ReturnedBranch: resultBranch, QualityOutcome: quality,
				}, nil
			}))
			runHead := oldHead
			_, err := executor.executeRemoteStep(context.Background(), RemoteStepRequest{
				RunID: run.ID, Step: types.StepReview, DesiredHeadSHA: oldHead, WorkDir: repoDir,
				Fixing: true, QualityOutcomeAuthority: "semantic-rereview",
			}, &runHead)
			outcomes, getErr := database.GetQualityOutcomesByRun(run.ID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if missingQuality {
				if err == nil || runHead != oldHead || pipelineGit(t, repoDir, "rev-parse", "HEAD") != oldHead || len(outcomes) != 0 {
					t.Fatalf("missing quality err=%v run=%s outcomes=%+v", err, runHead, outcomes)
				}
				return
			}
			if err != nil || runHead != newHead || len(outcomes) != 1 || outcomes[0].Classification != db.QualityCleanFix {
				t.Fatalf("recorded quality err=%v run=%s outcomes=%+v", err, runHead, outcomes)
			}
		})
	}
}

func pipelineGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = dir
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, output)
	}
	return strings.TrimSpace(string(output))
}
