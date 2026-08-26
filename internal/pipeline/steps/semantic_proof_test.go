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
	if err := os.WriteFile(filepath.Join(dir, "semantic-app.sh"), []byte("#!/bin/sh\ncat semantic-repair.txt 2>/dev/null\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "check-semantic.sh"), []byte("#!/bin/sh\n[ \"$(./semantic-app.sh)\" = fixed ]\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, dir, "add", "check-semantic.sh", "semantic-app.sh")
	gitCmd(t, dir, "commit", "-m", "add public behavior check")
	startingHead = gitCmd(t, dir, "rev-parse", "HEAD")
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
		PublicCommand:      "./check-semantic.sh",
		IntegrationCommand: "./check-semantic.sh",
	})

	if result.FailureCategory != "" {
		t.Fatalf("proof failed: %+v status=%q", result, gitCmd(t, dir, "status", "--porcelain"))
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

func TestValidateSemanticProofCommandRejectsObjectAndFileExistenceChecks(t *testing.T) {
	t.Parallel()
	for _, command := range []string{
		"git cat-file -e HEAD:semantic-repair.txt",
		"test -f semantic-repair.txt",
		"grep -q expected semantic-repair.txt",
		"/usr/bin/grep -q expected semantic-repair.txt",
		"sh -c 'test -f semantic-repair.txt'",
	} {
		if err := validateSemanticProofCommand(command); err == nil {
			t.Fatalf("accepted non-behavior proof command %q", command)
		}
	}
	if err := validateSemanticProofCommand("go test ./internal/public -run TestBehavior"); err != nil {
		t.Fatalf("rejected behavior proof command: %v", err)
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
