package steps

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
)

const maxSemanticProofCommandBytes = 2048

type semanticExecutorProof struct {
	StartingHeadSHA         string
	FixedHeadSHA            string
	BeforeExit              int
	AfterExit               int
	IntegrationExit         int
	BeforeOutputDigest      string
	AfterOutputDigest       string
	IntegrationOutputDigest string
	FailureCategory         string
}

func (p semanticExecutorProof) verified() bool {
	return p.FailureCategory == "" && p.BeforeExit != 0 && p.AfterExit == 0 &&
		p.IntegrationExit == 0 && validSemanticProofDigest(p.BeforeOutputDigest) &&
		validSemanticProofDigest(p.AfterOutputDigest) && validSemanticProofDigest(p.IntegrationOutputDigest)
}

func bindSemanticExecutorProof(sctx *pipeline.StepContext, startingHead string, evidence semanticRepairEvidence, injected pipeline.SemanticProofRunner) semanticRepairEvidence {
	proof := semanticExecutorProof{StartingHeadSHA: startingHead, FixedHeadSHA: sctx.Run.HeadSHA}
	if !evidence.RepairComplete {
		proof.FailureCategory = "fixer_reported_incomplete"
		evidence.executorProof = &proof
		return evidence
	}
	request := pipeline.SemanticProofRequest{
		WorkDir: sctx.WorkDir, StartingHeadSHA: startingHead, FixedHeadSHA: sctx.Run.HeadSHA,
		PublicCommand: evidence.PublicExecutableCheck, IntegrationCommand: evidence.IntegrationConsumerCheck,
		Env: append([]string(nil), sctx.Env...),
	}
	runner := sctx.SemanticProofRunner
	if runner == nil {
		runner = injected
	}
	if runner == nil {
		runner = executeTargetedSemanticProof
	}
	result := runner(sctx.Ctx, request)
	proof.BeforeExit = result.BeforeExit
	proof.AfterExit = result.AfterExit
	proof.IntegrationExit = result.IntegrationExit
	proof.BeforeOutputDigest = result.BeforeOutputDigest
	proof.AfterOutputDigest = result.AfterOutputDigest
	proof.IntegrationOutputDigest = result.IntegrationOutputDigest
	proof.FailureCategory = result.FailureCategory
	if proof.FailureCategory == "" && proof.BeforeExit == 0 {
		proof.FailureCategory = "public_check_did_not_fail_before"
	}
	if proof.FailureCategory == "" && proof.AfterExit != 0 {
		proof.FailureCategory = "public_check_failed_after"
	}
	if proof.FailureCategory == "" && proof.IntegrationExit != 0 {
		proof.FailureCategory = "integration_check_failed"
	}
	if proof.FailureCategory == "" && (!validSemanticProofDigest(proof.BeforeOutputDigest) ||
		!validSemanticProofDigest(proof.AfterOutputDigest) || !validSemanticProofDigest(proof.IntegrationOutputDigest)) {
		proof.FailureCategory = "executor_evidence_incomplete"
	}
	evidence.executorProof = &proof
	evidence.RepairComplete = proof.verified()
	if !evidence.RepairComplete && sctx.Log != nil {
		sctx.Log("semantic repair proof incomplete; primary-agent handoff required (" + proof.FailureCategory + ")")
	}
	return evidence
}

func executeTargetedSemanticProof(ctx context.Context, request pipeline.SemanticProofRequest) pipeline.SemanticProofResult {
	result := pipeline.SemanticProofResult{BeforeExit: -1, AfterExit: -1, IntegrationExit: -1}
	if err := validateSemanticProofCommand(request.PublicCommand); err != nil {
		result.FailureCategory = "public_command_not_targeted"
		return result
	}
	if err := validateSemanticProofCommand(request.IntegrationCommand); err != nil {
		result.FailureCategory = "integration_command_not_targeted"
		return result
	}
	if strings.TrimSpace(request.WorkDir) == "" || strings.TrimSpace(request.StartingHeadSHA) == "" || strings.TrimSpace(request.FixedHeadSHA) == "" {
		result.FailureCategory = "head_binding_missing"
		return result
	}
	currentHead, err := git.HeadSHA(ctx, request.WorkDir)
	if err != nil || currentHead != request.FixedHeadSHA {
		result.FailureCategory = "fixed_head_changed"
		return result
	}

	proofRoot, err := os.MkdirTemp("", "no-mistakes-semantic-proof-")
	if err != nil {
		result.FailureCategory = "before_checkout_failed"
		return result
	}
	defer os.RemoveAll(proofRoot)
	beforeDir := filepath.Join(proofRoot, "before")
	if _, err := git.Run(ctx, request.WorkDir, "worktree", "add", "--detach", beforeDir, request.StartingHeadSHA); err != nil {
		result.FailureCategory = "before_checkout_failed"
		return result
	}
	cleanup := func() error {
		_, cleanupErr := git.Run(context.Background(), request.WorkDir, "worktree", "remove", "--force", beforeDir)
		return cleanupErr
	}

	beforeOutput, beforeExit, beforeErr := runShellCommandWithEnv(ctx, beforeDir, request.Env, request.PublicCommand)
	result.BeforeExit = beforeExit
	result.BeforeOutputDigest = semanticProofOutputDigest(beforeOutput)
	afterOutput, afterExit, afterErr := runShellCommandWithEnv(ctx, request.WorkDir, request.Env, request.PublicCommand)
	result.AfterExit = afterExit
	result.AfterOutputDigest = semanticProofOutputDigest(afterOutput)
	integrationOutput, integrationExit, integrationErr := runShellCommandWithEnv(ctx, request.WorkDir, request.Env, request.IntegrationCommand)
	result.IntegrationExit = integrationExit
	result.IntegrationOutputDigest = semanticProofOutputDigest(integrationOutput)
	cleanupErr := cleanup()
	if beforeErr != nil || afterErr != nil || integrationErr != nil {
		result.FailureCategory = "command_execution_failed"
		return result
	}
	if cleanupErr != nil {
		result.FailureCategory = "before_checkout_cleanup_failed"
		return result
	}
	finalHead, err := git.HeadSHA(ctx, request.WorkDir)
	if err != nil || finalHead != request.FixedHeadSHA {
		result.FailureCategory = "fixed_head_changed"
		return result
	}
	status, err := git.Run(ctx, request.WorkDir, "status", "--porcelain")
	if err != nil || strings.TrimSpace(status) != "" {
		result.FailureCategory = "proof_command_mutated_worktree"
	}
	return result
}

func validateSemanticProofCommand(command string) error {
	command = strings.TrimSpace(command)
	if command == "" || len(command) > maxSemanticProofCommandBytes || strings.ContainsAny(command, "\x00\r\n") {
		return fmt.Errorf("semantic proof command must be one bounded line")
	}
	for _, marker := range []string{";", "&&", "||", "`", "$(", "|", ">", "<"} {
		if strings.Contains(command, marker) {
			return fmt.Errorf("semantic proof command must contain one direct check")
		}
	}
	fields := strings.Fields(strings.ToLower(command))
	for _, field := range fields {
		if field == "./..." || strings.HasSuffix(field, "/...") {
			return fmt.Errorf("semantic proof command cannot run a recursive suite")
		}
	}
	joined := strings.Join(fields, " ")
	switch {
	case joined == "go test", joined == "pytest", joined == "pytest .", joined == "python -m pytest", joined == "python3 -m pytest":
		return fmt.Errorf("semantic proof command requires an exact package or selector")
	case joined == "npm test", joined == "npm run test", joined == "pnpm test", joined == "yarn test", joined == "bun test":
		return fmt.Errorf("semantic proof command requires a focused selector")
	case joined == "cargo test", joined == "make test", joined == "make test-unit":
		return fmt.Errorf("semantic proof command cannot run a repository suite")
	}
	return nil
}

func semanticProofOutputDigest(output string) string {
	sum := sha256.Sum256([]byte(output))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func validSemanticProofDigest(value string) bool {
	if len(value) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && value == strings.ToLower(value)
}
