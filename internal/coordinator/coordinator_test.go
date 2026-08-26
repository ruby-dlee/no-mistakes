package coordinator

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

const webhookSecret = "test-secret-not-persisted"

type staticRepoMapper struct {
	repoID string
	calls  atomic.Int32
}

func (m *staticRepoMapper) ResolveGitHubRepository(_ context.Context, fullName string) (string, error) {
	m.calls.Add(1)
	if fullName != "ruby-labs/relvino" {
		return "", fmt.Errorf("unknown repository")
	}
	return m.repoID, nil
}

type stateKey struct {
	repoID string
	pr     int64
}

type staticGitHubClient struct {
	mu           sync.Mutex
	states       map[stateKey]db.AuthoritativeGitHubState
	fetchErr     error
	calls        int
	inFlight     int
	maxInFlight  int
	requestPause time.Duration
}

func (c *staticGitHubClient) RefetchCIState(_ context.Context, repoID string, prNumber int64) (db.AuthoritativeGitHubState, error) {
	c.mu.Lock()
	c.calls++
	c.inFlight++
	if c.inFlight > c.maxInFlight {
		c.maxInFlight = c.inFlight
	}
	state, ok := c.states[stateKey{repoID: repoID, pr: prNumber}]
	fetchErr := c.fetchErr
	c.mu.Unlock()
	if c.requestPause > 0 {
		time.Sleep(c.requestPause)
	}
	c.mu.Lock()
	c.inFlight--
	c.mu.Unlock()
	if fetchErr != nil {
		return db.AuthoritativeGitHubState{}, fetchErr
	}
	if !ok {
		return db.AuthoritativeGitHubState{}, fmt.Errorf("not found")
	}
	return state, nil
}

type stateReducer struct{}

func (stateReducer) ReduceCI(_ context.Context, _ db.CIReconciliationWork, state db.AuthoritativeGitHubState) (db.CIWaitStatus, error) {
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
		return "", fmt.Errorf("state is not terminal")
	}
}

type waitFixture struct {
	waitID, repoID, branch, head, input string
	pr, generation                      int64
}

func addWait(t *testing.T, database *db.DB, index int, registered time.Time) waitFixture {
	t.Helper()
	repoID := fmt.Sprintf("repo-%03d", index)
	_, err := database.InsertRepoWithID(repoID, t.TempDir(), fmt.Sprintf("https://example.invalid/%03d.git", index), "main")
	if err != nil {
		t.Fatal(err)
	}
	head := strings.Repeat(fmt.Sprintf("%x", index%15+1), 40)
	branch := fmt.Sprintf("review-%03d", index)
	run, err := database.InsertRun(repoID, branch, head, strings.Repeat("0", 40))
	if err != nil {
		t.Fatal(err)
	}
	step, err := database.InsertStepResult(run.ID, types.StepCI)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateStepStatus(step.ID, types.StepStatusRunning); err != nil {
		t.Fatal(err)
	}
	input := strings.Repeat(fmt.Sprintf("%x", (index+5)%15+1), 64)
	desired, _, _, err := database.AdvanceBranchDesiredState(db.BranchDesiredUpdate{
		RepoID: repoID, Branch: branch, HeadSHA: head, InputDigest: input, UpdatedAt: registered,
	})
	if err != nil {
		t.Fatal(err)
	}
	pr := int64(index + 1)
	waitID, err := database.RegisterCIWait(db.CIWaitSpec{
		RunID: run.ID, RepoID: repoID, Branch: branch, PRNumber: pr, HeadSHA: head,
		InputDigest: input, DesiredGeneration: desired.Revision, RegisteredAt: registered,
		ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	return waitFixture{waitID: waitID, repoID: repoID, branch: branch, head: head, input: input, pr: pr, generation: desired.Revision}
}

func webhookRequest(t *testing.T, deliveryID string, body []byte, secret string) *http.Request {
	t.Helper()
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	req := httptest.NewRequest(http.MethodPost, "/github", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Delivery", deliveryID)
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	return req
}

func TestWebhookRejectsInvalidSignatureAndOversizedBodyBeforeAdmission(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite")
	database, err := db.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	mapper := &staticRepoMapper{repoID: "unused"}
	client := &staticGitHubClient{states: map[stateKey]db.AuthoritativeGitHubState{}}
	handler, err := NewWebhookHandler(WebhookOptions{
		Secret: []byte(webhookSecret), Store: database, Repositories: mapper, GitHub: client,
		MaxBodyBytes: 128,
	})
	if err != nil {
		t.Fatal(err)
	}

	bad := webhookRequest(t, "bad-signature", []byte(`{"repository":{"full_name":"Ruby-Labs/Relvino"}}`), "wrong")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, bad)
	if recorder.Code != http.StatusUnauthorized || mapper.calls.Load() != 0 {
		t.Fatalf("invalid signature status=%d mapper calls=%d", recorder.Code, mapper.calls.Load())
	}

	large := webhookRequest(t, "oversized", bytes.Repeat([]byte("x"), 129), webhookSecret)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, large)
	if recorder.Code != http.StatusRequestEntityTooLarge || mapper.calls.Load() != 0 {
		t.Fatalf("oversized status=%d mapper calls=%d", recorder.Code, mapper.calls.Load())
	}
}

func TestWebhookRefetchesBeforeConfirmAndDeduplicatesExactReplay(t *testing.T) {
	database, err := db.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	start := time.Unix(1_800_000_000, 0)
	fixture := addWait(t, database, 0, start)
	mapper := &staticRepoMapper{repoID: fixture.repoID}
	client := &staticGitHubClient{states: map[stateKey]db.AuthoritativeGitHubState{
		{repoID: fixture.repoID, pr: fixture.pr}: {RepoID: fixture.repoID, PRNumber: fixture.pr, HeadSHA: fixture.head, CheckState: "passed"},
	}}
	handler, err := NewWebhookHandler(WebhookOptions{
		Secret: []byte(webhookSecret), Store: database, Repositories: mapper, GitHub: client,
		Now: func() time.Time { return start },
	})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(fmt.Sprintf(`{"action":"completed","number":%d,"repository":{"full_name":"Ruby-Labs/Relvino"},"pull_request":{"head":{"sha":"%s"}}}`, fixture.pr, fixture.head))

	for range 2 {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, webhookRequest(t, "delivery-1", body, webhookSecret))
		if recorder.Code != http.StatusAccepted {
			t.Fatalf("exact delivery status=%d body=%q", recorder.Code, recorder.Body.String())
		}
	}
	pending, err := database.PendingCIReconciliationWork(10)
	if err != nil || len(pending) != 1 || pending[0].Wait.ID != fixture.waitID {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	client.mu.Lock()
	calls := client.calls
	client.mu.Unlock()
	if calls != 2 {
		t.Fatalf("authoritative refetch calls=%d want=2", calls)
	}

	conflictBody := bytes.Replace(body, []byte(`"completed"`), []byte(`"rerequested"`), 1)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, webhookRequest(t, "delivery-1", conflictBody, webhookSecret))
	if recorder.Code != http.StatusConflict {
		t.Fatalf("conflicting replay status=%d", recorder.Code)
	}
}

func TestWebhookWrongAuthoritativeHeadFailsClosed(t *testing.T) {
	database, err := db.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	fixture := addWait(t, database, 1, time.Unix(1_800_000_000, 0))
	mapper := &staticRepoMapper{repoID: fixture.repoID}
	client := &staticGitHubClient{states: map[stateKey]db.AuthoritativeGitHubState{
		{repoID: fixture.repoID, pr: fixture.pr}: {RepoID: fixture.repoID, PRNumber: fixture.pr, HeadSHA: strings.Repeat("f", 40), CheckState: "passed"},
	}}
	handler, err := NewWebhookHandler(WebhookOptions{Secret: []byte(webhookSecret), Store: database, Repositories: mapper, GitHub: client})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(fmt.Sprintf(`{"number":%d,"repository":{"full_name":"Ruby-Labs/Relvino"},"pull_request":{"head":{"sha":"%s"}}}`, fixture.pr, fixture.head))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, webhookRequest(t, "wrong-head", body, webhookSecret))
	if recorder.Code != http.StatusConflict {
		t.Fatalf("wrong-head status=%d", recorder.Code)
	}
	pending, err := database.PendingCIReconciliationWork(10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("wrong-head event woke work=%d err=%v", len(pending), err)
	}
}

func TestServiceRecoversLostWebhooksAfterRestartWithBoundedConcurrencyAndNoLeases(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite")
	database, err := db.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(1_800_000_000, 0)
	states := make(map[stateKey]db.AuthoritativeGitHubState, 39)
	waits := make([]waitFixture, 0, 39)
	wantStatuses := make(map[string]db.CIWaitStatus, 39)
	for i := 0; i < 39; i++ {
		fixture := addWait(t, database, i+10, start)
		waits = append(waits, fixture)
		checkState := "passed"
		wantStatus := db.CIWaitReady
		switch i % 3 {
		case 1:
			checkState, wantStatus = "failed", db.CIWaitFailed
		case 2:
			checkState, wantStatus = "closed", db.CIWaitClosed
		}
		states[stateKey{repoID: fixture.repoID, pr: fixture.pr}] = db.AuthoritativeGitHubState{
			RepoID: fixture.repoID, PRNumber: fixture.pr, HeadSHA: fixture.head, CheckState: checkState,
		}
		wantStatuses[fixture.waitID] = wantStatus
	}
	for i := 0; i < 17; i++ {
		repoID := fmt.Sprintf("stale-repo-%03d", i)
		if _, err := database.InsertRepoWithID(repoID, t.TempDir(), fmt.Sprintf("https://example.invalid/stale/%03d.git", i), "main"); err != nil {
			t.Fatal(err)
		}
		run, err := database.InsertRun(repoID, fmt.Sprintf("stale-%03d", i), strings.Repeat("a", 40), strings.Repeat("0", 40))
		if err != nil {
			t.Fatal(err)
		}
		if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
			t.Fatal(err)
		}
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = db.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	before, err := database.UpdaterPipelineLiveness(start)
	if err != nil || len(before.ActiveWorkerLeases) != 0 || before.LegacyActiveRowsIgnored != 56 {
		t.Fatalf("restart liveness=%+v err=%v", before, err)
	}
	client := &staticGitHubClient{states: states, requestPause: 2 * time.Millisecond}
	service, err := NewService(ServiceOptions{
		Store: database, GitHub: client, Reducer: stateReducer{}, BatchSize: 100,
		MaxConcurrency: 4, Interval: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.ProcessOnce(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	maxInFlight, calls := client.maxInFlight, client.calls
	client.mu.Unlock()
	if calls != 39 || maxInFlight > 4 || maxInFlight < 2 {
		t.Fatalf("calls=%d max concurrency=%d", calls, maxInFlight)
	}
	for _, fixture := range waits {
		wait, err := database.GetCIWait(fixture.waitID)
		if err != nil || wait == nil || wait.Status != wantStatuses[fixture.waitID] {
			t.Fatalf("wait %s = %+v err=%v", fixture.waitID, wait, err)
		}
	}
	if pending, err := database.PendingCIReconciliationWork(100); err != nil || len(pending) != 0 {
		t.Fatalf("completed waits left pending work=%d err=%v", len(pending), err)
	}
	liveness, err := database.UpdaterPipelineLiveness(start)
	if err != nil || len(liveness.ActiveWorkerLeases) != 0 || liveness.LegacyActiveRowsIgnored != 43 {
		t.Fatalf("liveness=%+v err=%v", liveness, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("service did not shut down after cancellation")
	}
}

func TestServiceRetainsFailedRefetchWithoutLeakingClientError(t *testing.T) {
	database, err := db.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	start := time.Unix(1_800_000_000, 0)
	addWait(t, database, 99, start)
	client := &staticGitHubClient{
		states:   map[stateKey]db.AuthoritativeGitHubState{},
		fetchErr: fmt.Errorf("provider-secret-marker must not reach telemetry"),
	}
	service, err := NewService(ServiceOptions{
		Store: database, GitHub: client, Reducer: stateReducer{}, BatchSize: 10,
		MaxConcurrency: 2, Interval: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = service.ProcessOnce(context.Background(), start)
	if err == nil || strings.Contains(err.Error(), "provider-secret-marker") {
		t.Fatalf("privacy-safe refetch error=%q", err)
	}
	pending, pendingErr := database.PendingCIReconciliationWork(10)
	if pendingErr != nil || len(pending) != 1 {
		t.Fatalf("failed refetch consumed durable work=%d err=%v", len(pending), pendingErr)
	}
}
