package steps

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/pipeline"
)

func TestExecuteTargetedSemanticProofBindsFailBeforePassAfterAndIntegration(t *testing.T) {
	t.Parallel()
	dir, _, startingHead := setupGitRepo(t)
	gitCmd(t, dir, "checkout", "--detach", startingHead)
	if err := os.WriteFile(filepath.Join(dir, "semantic-repair.txt"), []byte("fixed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, dir, "add", "semantic-repair.txt")
	gitCmd(t, dir, "commit", "-m", "fix public regression")
	fixedHead := gitCmd(t, dir, "rev-parse", "HEAD")

	result := executeTargetedSemanticProof(context.Background(), pipeline.SemanticProofRequest{
		WorkDir:            dir,
		StartingHeadSHA:    startingHead,
		FixedHeadSHA:       fixedHead,
		PublicCommand:      "git cat-file -e HEAD:semantic-repair.txt",
		IntegrationCommand: "git cat-file -e HEAD:semantic-repair.txt",
	})

	if result.FailureCategory != "" {
		t.Fatalf("proof failed: %+v", result)
	}
	if result.BeforeExit == 0 || result.AfterExit != 0 || result.IntegrationExit != 0 {
		t.Fatalf("proof exits = before:%d after:%d integration:%d", result.BeforeExit, result.AfterExit, result.IntegrationExit)
	}
	for name, digest := range map[string]string{
		"before":      result.BeforeOutputDigest,
		"after":       result.AfterOutputDigest,
		"integration": result.IntegrationOutputDigest,
	} {
		if !validSemanticProofDigest(digest) {
			t.Fatalf("%s digest invalid: %q", name, digest)
		}
	}
}

func TestExecuteTargetedSemanticProofRefusesBroadSuite(t *testing.T) {
	t.Parallel()
	dir, _, head := setupGitRepo(t)

	result := executeTargetedSemanticProof(context.Background(), pipeline.SemanticProofRequest{
		WorkDir:            dir,
		StartingHeadSHA:    head,
		FixedHeadSHA:       head,
		PublicCommand:      "go test ./...",
		IntegrationCommand: "go test ./internal/pipeline -run TestFocused",
	})

	if result.FailureCategory != "public_command_not_targeted" {
		t.Fatalf("broad command failure category = %q", result.FailureCategory)
	}
}
