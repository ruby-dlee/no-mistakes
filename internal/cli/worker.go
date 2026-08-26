package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/agent"
	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
	gitx "github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/pipeline/steps"
	"github.com/kunchenguid/no-mistakes/internal/types"
	"github.com/kunchenguid/no-mistakes/internal/workertransport"
	"github.com/spf13/cobra"
)

const maxWorkerBriefBytes = 1 << 20

type workerRunOptions struct {
	role, brief, result string
}

type workerRunDeps struct {
	newAgent func(types.AgentName, string, []string, agent.Options) (agent.Agent, error)
}

type workerQualityCollector struct {
	outcomes []db.QualityOutcome
}

func (c *workerQualityCollector) InsertQualityOutcome(outcome db.QualityOutcome) (*db.QualityOutcome, error) {
	if len(c.outcomes) != 0 {
		return nil, errors.New("isolated worker produced more than one semantic quality outcome")
	}
	c.outcomes = append(c.outcomes, outcome)
	copy := outcome
	return &copy, nil
}

func defaultWorkerRunDeps() workerRunDeps {
	return workerRunDeps{newAgent: agent.NewWithOptions}
}

func newWorkerCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "worker", Short: "Run an isolated external pipeline worker"}
	cmd.AddCommand(newWorkerRunCmd(defaultWorkerRunDeps()))
	return cmd
}

func newWorkerRunCmd(deps workerRunDeps) *cobra.Command {
	var opts workerRunOptions
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run one exact-bound review, repair, or test role",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runWorker(cmd.Context(), cmd.ErrOrStderr(), opts, deps)
		},
	}
	cmd.Flags().StringVar(&opts.role, "role", "", "worker role: review, repair, or test")
	cmd.Flags().StringVar(&opts.brief, "brief", "", "path to the exact-bound worker brief")
	cmd.Flags().StringVar(&opts.result, "result", "", "path for the atomic worker outcome")
	_ = cmd.MarkFlagRequired("role")
	_ = cmd.MarkFlagRequired("brief")
	_ = cmd.MarkFlagRequired("result")
	return cmd
}

func runWorker(ctx context.Context, logs io.Writer, opts workerRunOptions, deps workerRunDeps) error {
	role := strings.TrimSpace(opts.role)
	if role != "review" && role != "repair" && role != "test" {
		return fmt.Errorf("unsupported worker role %q", role)
	}
	briefPath, err := filepath.Abs(opts.brief)
	if err != nil {
		return fmt.Errorf("resolve worker brief: %w", err)
	}
	resultPath, err := filepath.Abs(opts.result)
	if err != nil {
		return fmt.Errorf("resolve worker result: %w", err)
	}
	if briefPath == resultPath {
		return errors.New("worker brief and result paths must differ")
	}
	if _, err := os.Lstat(resultPath); err == nil {
		return errors.New("worker result path already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect worker result path: %w", err)
	}
	brief, err := readBoundedRegularFile(briefPath, maxWorkerBriefBytes)
	if err != nil {
		return fmt.Errorf("read worker brief: %w", err)
	}
	input, err := workertransport.DecodeStepInput(brief)
	if err != nil {
		return fmt.Errorf("parse worker brief: %w", err)
	}
	if err := validateWorkerRole(role, input); err != nil {
		return err
	}
	workDir, err := gitx.FindGitRoot(".")
	if err != nil {
		return fmt.Errorf("worker requires a git worktree: %w", err)
	}
	if err := verifyWorkerSource(ctx, workDir, input); err != nil {
		return err
	}

	cfg, err := loadWorkerConfig(ctx, workDir, input.BaseSHA)
	if err != nil {
		return err
	}
	if cfg.Agent == types.AgentAuto && (len(cfg.Agents) == 0 || len(cfg.Agents) == 1 && cfg.Agents[0] == types.AgentAuto) {
		cfg.Agent = types.AgentPi
		cfg.Agents = []types.AgentName{types.AgentPi}
	}
	if err := cfg.ResolveAgent(ctx, exec.LookPath); err != nil {
		return fmt.Errorf("resolve worker agent: %w", err)
	}
	if cfg.Agent != types.AgentPi {
		return fmt.Errorf("isolated worker requires configured or detected pi agent, got %q", cfg.Agent)
	}
	ag, err := deps.newAgent(types.AgentPi, cfg.AgentPathFor(types.AgentPi), cfg.AgentArgsFor(types.AgentPi), agent.Options{
		ACPRegistryOverrides: cfg.ACPRegistryOverrides, DisableProjectSettings: cfg.DisableProjectSettings,
		Profile: cfg.AgentProfileFor(types.AgentPi),
	})
	if err != nil {
		return fmt.Errorf("create pi worker agent: %w", err)
	}
	evidenceDir, err := os.MkdirTemp("", "no-mistakes-worker-evidence-")
	if err != nil {
		_ = ag.Close()
		return fmt.Errorf("create worker evidence directory: %w", err)
	}
	defer os.RemoveAll(evidenceDir)
	ag = agent.WithSteering(ag, evidenceDir)
	defer ag.Close()
	if cfg.DisableProjectSettings {
		if err := agent.EnsureGateNeutralized(ag); err != nil {
			return err
		}
	}

	run := &db.Run{ID: input.RunID, RepoID: input.RepoID, Branch: input.Branch, HeadSHA: input.DesiredHeadSHA, BaseSHA: input.BaseSHA}
	repo := &db.Repo{ID: input.RepoID, WorkingPath: workDir, DefaultBranch: input.DefaultBranch}
	logf := func(line string) {
		if logs != nil {
			fmt.Fprintln(logs, line)
		}
	}
	sctx := &pipeline.StepContext{
		Ctx: ctx, Run: run, Repo: repo, WorkDir: workDir, Agent: ag, Config: cfg,
		Log: logf, LogChunk: func(chunk string) {
			if logs != nil {
				fmt.Fprint(logs, chunk)
			}
		}, LogFile: func(string) {},
		Fixing: input.Fixing, PreviousFindings: input.PreviousFindings, StepResultID: input.StepResultID,
		EvidenceDir: evidenceDir, UserIntent: input.UserIntent, IntentSource: input.UserIntentSource,
		ReviewStartingHeadSHA:         input.DesiredHeadSHA,
		RemotePriorRoundHistory:       input.PriorRoundHistory,
		RemoteUncertifiedRoundHistory: input.UncertifiedRoundHistory,
		RemoteRepairAttempt:           input.RepairAttempt,
	}
	var qualityCollector *workerQualityCollector
	if input.QualityOutcomeAuthority == "semantic-rereview" {
		qualityCollector = &workerQualityCollector{}
		sctx.QualityOutcomes = qualityCollector
	}
	sctx.CommitFixes = func(step types.StepName, summary, fallback string) error {
		return steps.CommitIsolatedWorkerFixes(sctx, step, summary, fallback)
	}
	var step pipeline.Step
	if input.Step == types.StepReview {
		step = &steps.ReviewStep{}
	} else {
		step = &steps.TestStep{}
	}
	outcome, err := step.Execute(sctx)
	if err != nil {
		return fmt.Errorf("worker %s failed: %w", role, err)
	}
	if outcome == nil {
		return errors.New("worker step returned no outcome")
	}
	outputHead, err := gitx.HeadSHA(ctx, workDir)
	if err != nil {
		return fmt.Errorf("resolve worker output head: %w", err)
	}
	if err := verifyWorkerOutput(ctx, workDir, input, outputHead); err != nil {
		return err
	}
	closed := workertransport.StepOutcomeEnvelope{
		Schema: workertransport.StepOutcomeSchema, Step: workertransport.StepOutcomeStep(input.Step),
		NeedsApproval: outcome.NeedsApproval, AutoFixable: outcome.AutoFixable,
		FindingsJSON: outcome.Findings, ExitCode: outcome.ExitCode, FixSummary: outcome.FixSummary,
		ReviewApprovedHeadSHA: outcome.ReviewApprovedHeadSHA, Skipped: outcome.Skipped, SkipRemaining: outcome.SkipRemaining,
	}
	if qualityCollector != nil {
		if len(qualityCollector.outcomes) != 1 {
			return errors.New("authorized review repair returned no semantic quality outcome")
		}
		quality := qualityCollector.outcomes[0]
		closed.QualityOutcome = &workertransport.QualityOutcomeEnvelope{
			FixAttemptID: stringValue(quality.FixAttemptID), RootID: stringValue(quality.RootID),
			Classification: string(quality.Classification), FixedHeadSHA: quality.FixedHeadSHA,
			ObservedHeadSHA: quality.ObservedHeadSHA, EvidenceDigest: quality.EvidenceDigest,
			EvidenceProvenance: quality.EvidenceProvenance,
		}
	}
	if err := workertransport.ValidateStepOutcome(closed, workertransport.StepOutcomeStep(input.Step), outputHead); err != nil {
		return fmt.Errorf("validate worker outcome: %w", err)
	}
	if err := writeAtomicWorkerOutcome(resultPath, closed); err != nil {
		return fmt.Errorf("write worker outcome: %w", err)
	}
	return nil
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func validateWorkerRole(role string, input workertransport.StepInputEnvelope) error {
	switch role {
	case "review":
		if input.Step != types.StepReview || input.Fixing {
			return errors.New("review role requires a non-fixing review brief")
		}
	case "test":
		if input.Step != types.StepTest || input.Fixing {
			return errors.New("test role requires a non-fixing test brief")
		}
	case "repair":
		if !input.Fixing || (input.Step != types.StepReview && input.Step != types.StepTest) {
			return errors.New("repair role requires a fixing review or test brief")
		}
	}
	return nil
}

func verifyWorkerSource(ctx context.Context, workDir string, input workertransport.StepInputEnvelope) error {
	for name, branch := range map[string]string{"branch": input.Branch, "default branch": input.DefaultBranch} {
		ref := branch
		if !strings.HasPrefix(ref, "refs/heads/") {
			ref = "refs/heads/" + ref
		}
		if _, err := gitx.Run(ctx, workDir, "check-ref-format", ref); err != nil {
			return fmt.Errorf("worker %s is not a valid branch ref", name)
		}
	}
	current, err := gitx.HeadSHA(ctx, workDir)
	if err != nil || current != input.DesiredHeadSHA {
		return fmt.Errorf("worker source HEAD is %q, want exact %q", current, input.DesiredHeadSHA)
	}
	if _, err := gitx.Run(ctx, workDir, "cat-file", "-e", input.BaseSHA+"^{commit}"); err != nil {
		return errors.New("worker base commit is unavailable")
	}
	if _, err := gitx.Run(ctx, workDir, "merge-base", "--is-ancestor", input.BaseSHA, input.DesiredHeadSHA); err != nil {
		return errors.New("worker base is not an ancestor of the exact head")
	}
	status, err := gitx.Run(ctx, workDir, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil || strings.TrimSpace(status) != "" {
		return errors.New("worker source worktree is not exactly clean")
	}
	return nil
}

func verifyWorkerOutput(ctx context.Context, workDir string, input workertransport.StepInputEnvelope, outputHead string) error {
	status, err := gitx.Run(ctx, workDir, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil || strings.TrimSpace(status) != "" {
		return errors.New("worker output worktree is not exactly clean")
	}
	if !input.Fixing {
		if outputHead != input.DesiredHeadSHA {
			return errors.New("read-only worker changed the exact head")
		}
		return nil
	}
	if outputHead == input.DesiredHeadSHA {
		return errors.New("worker repair produced no descendant commit")
	}
	if _, err := gitx.Run(ctx, workDir, "merge-base", "--is-ancestor", input.DesiredHeadSHA, outputHead); err != nil {
		return errors.New("worker repair output is not a descendant of its input head")
	}
	return nil
}

func loadWorkerConfig(ctx context.Context, workDir, base string) (*config.Config, error) {
	p, err := paths.New()
	if err != nil {
		return nil, fmt.Errorf("resolve worker config root: %w", err)
	}
	global, err := config.LoadGlobal(p.ConfigFile())
	if err != nil {
		return nil, err
	}
	pushed, err := config.LoadRepo(workDir)
	if err != nil {
		return nil, err
	}
	trusted := &config.RepoConfig{}
	if data, showErr := gitx.Run(ctx, workDir, "show", base+":.no-mistakes.yaml"); showErr == nil {
		trusted, err = config.LoadRepoFromBytes([]byte(data))
		if err != nil {
			return nil, fmt.Errorf("parse trusted worker config: %w", err)
		}
	}
	return config.Merge(global, config.EffectiveRepoConfig(pushed, trusted, trusted.AllowRepoCommands)), nil
}

func readBoundedRegularFile(path string, max int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > max {
		return nil, errors.New("file is not regular or exceeds limit")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > max {
		return nil, errors.New("file exceeds limit")
	}
	return data, nil
}

func writeAtomicWorkerOutcome(path string, outcome workertransport.StepOutcomeEnvelope) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".worker-outcome-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	encoder := json.NewEncoder(tmp)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(outcome); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if _, err := os.Lstat(path); err == nil {
		return errors.New("worker result path appeared during execution")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Rename(tmpPath, path)
}
