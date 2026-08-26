package coordinator

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/scm"
	"github.com/kunchenguid/no-mistakes/internal/scm/github"
	"github.com/kunchenguid/no-mistakes/internal/shellenv"
)

const defaultGitHubRefetchTimeout = 30 * time.Second

type RepositoryStore interface {
	GetRepos() ([]*db.Repo, error)
	GetRepo(string) (*db.Repo, error)
}

// RegisteredGitHubRepositories maps webhook repository identities only to an
// exact, unambiguous repository already registered in the daemon database.
type RegisteredGitHubRepositories struct {
	Store RepositoryStore
}

func (m RegisteredGitHubRepositories) ResolveGitHubRepository(ctx context.Context, canonicalFullName string) (string, error) {
	if m.Store == nil {
		return "", errors.New("repository mapper has no store")
	}
	canonicalFullName = strings.ToLower(strings.TrimSpace(canonicalFullName))
	if canonicalFullName == "" {
		return "", errors.New("repository identity is empty")
	}
	repos, err := m.Store.GetRepos()
	if err != nil {
		return "", fmt.Errorf("load registered repositories: %w", err)
	}
	var match string
	for _, repo := range repos {
		if repo == nil || scm.DetectProviderContext(ctx, repo.UpstreamURL) != scm.ProviderGitHub {
			continue
		}
		if strings.ToLower(github.RepoSlug(repo.UpstreamURL)) != canonicalFullName {
			continue
		}
		if match != "" {
			return "", errors.New("repository identity is ambiguous")
		}
		match = repo.ID
	}
	if match == "" {
		return "", errors.New("repository is not registered")
	}
	return match, nil
}

type GitHubHostFactory func(context.Context, *db.Repo) (scm.Host, error)

type GitHubAuthorityOptions struct {
	Store   RepositoryStore
	Host    GitHubHostFactory
	WorkDir string
	Timeout time.Duration
}

// GitHubAuthority refetches lifecycle and checks through the same scm.Host
// authority used by the CI pipeline. Webhook payloads never enter this path.
type GitHubAuthority struct {
	store   RepositoryStore
	host    GitHubHostFactory
	timeout time.Duration
}

func NewGitHubAuthority(options GitHubAuthorityOptions) (*GitHubAuthority, error) {
	if options.Store == nil {
		return nil, errors.New("GitHub authority requires repository store")
	}
	if options.Timeout == 0 {
		options.Timeout = defaultGitHubRefetchTimeout
	}
	if options.Timeout < time.Second || options.Timeout > 5*time.Minute {
		return nil, errors.New("GitHub authority timeout must be 1 second..5 minutes")
	}
	if options.Host == nil {
		options.Host = productionGitHubHost(options.WorkDir)
	}
	return &GitHubAuthority{store: options.Store, host: options.Host, timeout: options.Timeout}, nil
}

func (a *GitHubAuthority) RefetchCIState(ctx context.Context, repoID string, prNumber int64) (db.AuthoritativeGitHubState, error) {
	if strings.TrimSpace(repoID) == "" || prNumber <= 0 {
		return db.AuthoritativeGitHubState{}, errors.New("GitHub refetch requires exact repository and PR")
	}
	repo, err := a.store.GetRepo(repoID)
	if err != nil {
		return db.AuthoritativeGitHubState{}, fmt.Errorf("load registered repository: %w", err)
	}
	if repo == nil || scm.DetectProviderContext(ctx, repo.UpstreamURL) != scm.ProviderGitHub {
		return db.AuthoritativeGitHubState{}, errors.New("registered repository is not GitHub")
	}
	refetchCtx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	host, err := a.host(refetchCtx, repo)
	if err != nil || host == nil || host.Provider() != scm.ProviderGitHub {
		return db.AuthoritativeGitHubState{}, errors.New("construct GitHub authority failed")
	}
	if err := host.Available(refetchCtx); err != nil {
		return db.AuthoritativeGitHubState{}, errors.New("GitHub authority is unavailable")
	}
	// A nonempty HeadSHA tells the GitHub host to discover checks by the
	// authoritative current PR head and to verify it again after discovery.
	pr := &scm.PR{Number: strconv.FormatInt(prNumber, 10), HeadSHA: strings.Repeat("0", 40)}
	prState, err := host.GetPRState(refetchCtx, pr)
	if err != nil {
		return db.AuthoritativeGitHubState{}, errors.New("GitHub lifecycle refetch failed")
	}
	checks, err := host.GetChecks(refetchCtx, pr)
	if err != nil {
		return db.AuthoritativeGitHubState{}, errors.New("GitHub check refetch failed")
	}
	if !isGitHead(pr.HeadSHA) {
		return db.AuthoritativeGitHubState{}, errors.New("GitHub refetch returned no exact head")
	}
	checkState, err := classifyGitHubState(prState, checks)
	if err != nil {
		return db.AuthoritativeGitHubState{}, err
	}
	return db.AuthoritativeGitHubState{
		RepoID: repoID, PRNumber: prNumber, HeadSHA: pr.HeadSHA, CheckState: checkState,
	}, nil
}

func productionGitHubHost(workDir string) GitHubHostFactory {
	return func(ctx context.Context, repo *db.Repo) (scm.Host, error) {
		host := scm.ResolveHost(ctx, repo.UpstreamURL)
		slug := github.HostPrefixedSlugForHost(repo.UpstreamURL, host)
		if host == "" || slug == "" {
			return nil, errors.New("could not resolve GitHub repository authority")
		}
		cmdFactory := func(commandCtx context.Context, name string, args ...string) *exec.Cmd {
			cmd := exec.CommandContext(commandCtx, name, args...)
			cmd.Dir = workDir
			shellenv.ConfigureShellCommand(cmd)
			return cmd
		}
		return github.New(cmdFactory, func() bool {
			_, err := exec.LookPath("gh")
			return err == nil
		}, host, slug), nil
	}
}

func classifyGitHubState(prState scm.PRState, checks []scm.Check) (string, error) {
	switch prState {
	case scm.PRStateMerged, scm.PRStateClosed:
		return "closed", nil
	case scm.PRStateOpen:
	default:
		return "", errors.New("GitHub lifecycle state is unknown")
	}
	if len(checks) == 0 {
		return "unknown", nil
	}
	failed := false
	pending := false
	cancelled := false
	for _, check := range checks {
		switch check.Bucket {
		case scm.CheckBucketPass, scm.CheckBucketSkip:
		case scm.CheckBucketFail:
			failed = true
		case scm.CheckBucketPending:
			pending = true
		case scm.CheckBucketCancel:
			cancelled = true
		default:
			pending = true
		}
	}
	if pending {
		return "pending", nil
	}
	if failed || cancelled {
		return "failed", nil
	}
	return "passed", nil
}

type ExactCIStateReducer struct{}

func (ExactCIStateReducer) ReduceCI(_ context.Context, work db.CIReconciliationWork, state db.AuthoritativeGitHubState) (db.CIWaitStatus, error) {
	if state.RepoID != work.Wait.RepoID || state.PRNumber != work.Wait.PRNumber || state.HeadSHA != work.Wait.HeadSHA {
		return "", errors.New("authoritative state exact binding changed")
	}
	switch state.CheckState {
	case "unknown", "pending":
		return db.CIWaitWaiting, nil
	case "passed":
		return db.CIWaitReady, nil
	case "failed":
		return db.CIWaitFailed, nil
	case "closed":
		return db.CIWaitClosed, nil
	default:
		return "", errors.New("authoritative state is invalid")
	}
}
