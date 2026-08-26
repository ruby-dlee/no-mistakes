package coordinator

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/scm"
)

type repositoryStoreStub struct {
	repos []*db.Repo
}

func (s repositoryStoreStub) GetRepos() ([]*db.Repo, error) {
	return s.repos, nil
}

func (s repositoryStoreStub) GetRepo(id string) (*db.Repo, error) {
	for _, repo := range s.repos {
		if repo.ID == id {
			copy := *repo
			return &copy, nil
		}
	}
	return nil, nil
}

type authoritativeHostStub struct {
	head   string
	state  scm.PRState
	checks []scm.Check
	calls  []string
}

func (h *authoritativeHostStub) Provider() scm.Provider { return scm.ProviderGitHub }
func (h *authoritativeHostStub) Capabilities() scm.Capabilities {
	return scm.Capabilities{}
}
func (h *authoritativeHostStub) Available(context.Context) error { return nil }
func (h *authoritativeHostStub) FindPR(context.Context, string, string) (*scm.PR, error) {
	return nil, scm.ErrUnsupported
}
func (h *authoritativeHostStub) CreatePR(context.Context, string, string, scm.PRContent) (*scm.PR, error) {
	return nil, scm.ErrUnsupported
}
func (h *authoritativeHostStub) UpdatePR(context.Context, *scm.PR, scm.PRContent) (*scm.PR, error) {
	return nil, scm.ErrUnsupported
}
func (h *authoritativeHostStub) GetPRState(_ context.Context, _ *scm.PR) (scm.PRState, error) {
	h.calls = append(h.calls, "state")
	return h.state, nil
}
func (h *authoritativeHostStub) GetChecks(_ context.Context, pr *scm.PR) ([]scm.Check, error) {
	h.calls = append(h.calls, "checks")
	pr.HeadSHA = h.head
	return h.checks, nil
}
func (h *authoritativeHostStub) GetMergeableState(context.Context, *scm.PR) (scm.MergeableState, error) {
	return scm.MergeableUnknown, scm.ErrUnsupported
}
func (h *authoritativeHostStub) FetchFailedCheckLogs(context.Context, *scm.PR, string, string, []string) (string, error) {
	return "", scm.ErrUnsupported
}

func TestRegisteredGitHubRepositoriesResolveExactCanonicalSlug(t *testing.T) {
	repos := repositoryStoreStub{repos: []*db.Repo{
		{ID: "github", UpstreamURL: "git@github.com:Ruby-Labs/Relvino.git"},
		{ID: "other", UpstreamURL: "https://gitlab.com/Ruby-Labs/Relvino.git"},
	}}
	mapper := RegisteredGitHubRepositories{Store: repos}
	got, err := mapper.ResolveGitHubRepository(context.Background(), "ruby-labs/relvino")
	if err != nil || got != "github" {
		t.Fatalf("repo=%q err=%v", got, err)
	}

	repos.repos = append(repos.repos, &db.Repo{ID: "duplicate", UpstreamURL: "https://github.com/ruby-labs/relvino"})
	mapper.Store = repos
	if _, err := mapper.ResolveGitHubRepository(context.Background(), "ruby-labs/relvino"); err == nil {
		t.Fatal("ambiguous registered repository was admitted")
	}
	if _, err := mapper.ResolveGitHubRepository(context.Background(), "ruby-labs/missing"); err == nil {
		t.Fatal("unregistered repository was admitted")
	}
}

func TestGitHubAuthorityRefetchesThroughSCMHostAndClassifiesExactHead(t *testing.T) {
	head := strings.Repeat("a", 40)
	host := &authoritativeHostStub{
		head: head, state: scm.PRStateOpen,
		checks: []scm.Check{{Name: "unit", Bucket: scm.CheckBucketPass}, {Name: "lint", Bucket: scm.CheckBucketSkip}},
	}
	authority, err := NewGitHubAuthority(GitHubAuthorityOptions{
		Store: repositoryStoreStub{repos: []*db.Repo{{ID: "repo", UpstreamURL: "https://github.com/Ruby-Labs/Relvino.git"}}},
		Host:  func(context.Context, *db.Repo) (scm.Host, error) { return host, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	state, err := authority.RefetchCIState(context.Background(), "repo", 327)
	if err != nil {
		t.Fatal(err)
	}
	if state.RepoID != "repo" || state.PRNumber != 327 || state.HeadSHA != head || state.CheckState != "passed" {
		t.Fatalf("state=%+v", state)
	}
	if strings.Join(host.calls, ",") != "state,checks" {
		t.Fatalf("SCM calls=%v", host.calls)
	}

	host.checks = []scm.Check{{Name: "unit", Bucket: scm.CheckBucketFail}}
	state, err = authority.RefetchCIState(context.Background(), "repo", 327)
	if err != nil || state.CheckState != "failed" {
		t.Fatalf("failed state=%+v err=%v", state, err)
	}
	host.checks = []scm.Check{{Name: "unit", Bucket: scm.CheckBucketCancel}}
	state, err = authority.RefetchCIState(context.Background(), "repo", 327)
	if err != nil || state.CheckState != "failed" {
		t.Fatalf("cancelled state=%+v err=%v", state, err)
	}
	host.state = scm.PRStateClosed
	state, err = authority.RefetchCIState(context.Background(), "repo", 327)
	if err != nil || state.CheckState != "closed" {
		t.Fatalf("closed state=%+v err=%v", state, err)
	}
}

func TestGitHubAuthorityFailsClosedOutsideGitHubOrWithoutExactHead(t *testing.T) {
	for name, repoURL := range map[string]string{
		"non-github": "https://gitlab.com/Ruby-Labs/Relvino.git",
		"bad-url":    "not-a-provider-url",
	} {
		t.Run(name, func(t *testing.T) {
			authority, err := NewGitHubAuthority(GitHubAuthorityOptions{
				Store: repositoryStoreStub{repos: []*db.Repo{{ID: "repo", UpstreamURL: repoURL}}},
				Host: func(context.Context, *db.Repo) (scm.Host, error) {
					t.Fatal("host factory called for untrusted provider")
					return nil, errors.New("unreachable")
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := authority.RefetchCIState(context.Background(), "repo", 1); err == nil {
				t.Fatal("untrusted provider was admitted")
			}
		})
	}

	host := &authoritativeHostStub{head: "not-a-sha", state: scm.PRStateOpen}
	authority, err := NewGitHubAuthority(GitHubAuthorityOptions{
		Store: repositoryStoreStub{repos: []*db.Repo{{ID: "repo", UpstreamURL: "https://github.com/Ruby-Labs/Relvino.git"}}},
		Host:  func(context.Context, *db.Repo) (scm.Host, error) { return host, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.RefetchCIState(context.Background(), "repo", 1); err == nil {
		t.Fatal("non-exact authoritative head was admitted")
	}
}

func TestExactCIStateReducerRejectsChangedBindingsAndMapsState(t *testing.T) {
	work := db.CIReconciliationWork{Wait: db.CIWait{
		RepoID: "repo", PRNumber: 7, HeadSHA: strings.Repeat("a", 40),
	}}
	for state, want := range map[string]db.CIWaitStatus{
		"unknown": db.CIWaitWaiting,
		"pending": db.CIWaitWaiting,
		"passed":  db.CIWaitReady,
		"failed":  db.CIWaitFailed,
		"closed":  db.CIWaitClosed,
	} {
		got, err := (ExactCIStateReducer{}).ReduceCI(context.Background(), work, db.AuthoritativeGitHubState{
			RepoID: "repo", PRNumber: 7, HeadSHA: work.Wait.HeadSHA, CheckState: state,
		})
		if err != nil || got != want {
			t.Errorf("state %q: got=%q err=%v want=%q", state, got, err, want)
		}
	}
	if _, err := (ExactCIStateReducer{}).ReduceCI(context.Background(), work, db.AuthoritativeGitHubState{
		RepoID: "repo", PRNumber: 7, HeadSHA: strings.Repeat("b", 40), CheckState: "passed",
	}); err == nil {
		t.Fatal("changed head was admitted")
	}
}
