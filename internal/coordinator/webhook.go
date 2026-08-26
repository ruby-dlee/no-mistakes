package coordinator

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
)

const defaultWebhookBodyLimit int64 = 1 << 20

type RepositoryMapper interface {
	ResolveGitHubRepository(ctx context.Context, canonicalFullName string) (string, error)
}

type GitHubStateClient interface {
	RefetchCIState(ctx context.Context, repoID string, prNumber int64) (db.AuthoritativeGitHubState, error)
}

type WebhookStore interface {
	AdmitGitHubDelivery(delivery db.GitHubDelivery) (bool, error)
	ConfirmGitHubDelivery(deliveryID string, state db.AuthoritativeGitHubState, at time.Time) (int, error)
}

type WebhookOptions struct {
	Secret       []byte
	Store        WebhookStore
	Repositories RepositoryMapper
	GitHub       GitHubStateClient
	Now          func() time.Time
	MaxBodyBytes int64
}

type webhookHandler struct {
	secret       []byte
	store        WebhookStore
	repositories RepositoryMapper
	github       GitHubStateClient
	now          func() time.Time
	maxBodyBytes int64
}

func NewWebhookHandler(options WebhookOptions) (http.Handler, error) {
	if len(options.Secret) == 0 || options.Store == nil || options.Repositories == nil || options.GitHub == nil {
		return nil, errors.New("GitHub webhook handler requires secret, store, repository mapper, and client")
	}
	if options.MaxBodyBytes == 0 {
		options.MaxBodyBytes = defaultWebhookBodyLimit
	}
	if options.MaxBodyBytes < 1 || options.MaxBodyBytes > 10<<20 {
		return nil, errors.New("GitHub webhook body limit must be 1 byte..10 MiB")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &webhookHandler{
		secret: append([]byte(nil), options.Secret...), store: options.Store,
		repositories: options.Repositories, github: options.GitHub,
		now: options.Now, maxBodyBytes: options.MaxBodyBytes,
	}, nil
}

func (h *webhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		http.Error(w, "unsupported content type", http.StatusUnsupportedMediaType)
		return
	}
	deliveryID := strings.TrimSpace(r.Header.Get("X-GitHub-Delivery"))
	eventType := strings.TrimSpace(r.Header.Get("X-GitHub-Event"))
	signature := strings.TrimSpace(r.Header.Get("X-Hub-Signature-256"))
	if deliveryID == "" || len(deliveryID) > 255 || eventType == "" || len(eventType) > 100 || signature == "" {
		http.Error(w, "missing or invalid GitHub headers", http.StatusBadRequest)
		return
	}
	if !supportedWebhookEvent(eventType) {
		http.Error(w, "unsupported GitHub event", http.StatusUnprocessableEntity)
		return
	}
	if r.ContentLength > h.maxBodyBytes {
		http.Error(w, "webhook body too large", http.StatusRequestEntityTooLarge)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, h.maxBodyBytes+1))
	if err != nil {
		http.Error(w, "could not read webhook body", http.StatusBadRequest)
		return
	}
	if int64(len(body)) > h.maxBodyBytes {
		http.Error(w, "webhook body too large", http.StatusRequestEntityTooLarge)
		return
	}
	if !validWebhookSignature(h.secret, signature, body) {
		http.Error(w, "invalid webhook signature", http.StatusUnauthorized)
		return
	}
	event, err := extractWebhookEvent(eventType, body)
	if err != nil {
		http.Error(w, "invalid webhook payload", http.StatusUnprocessableEntity)
		return
	}
	repoID, err := h.repositories.ResolveGitHubRepository(r.Context(), event.repository)
	if err != nil || strings.TrimSpace(repoID) == "" {
		http.Error(w, "repository is not registered", http.StatusNotFound)
		return
	}
	digest := sha256.Sum256(body)
	_, err = h.store.AdmitGitHubDelivery(db.GitHubDelivery{
		DeliveryID: deliveryID, PayloadDigest: hex.EncodeToString(digest[:]),
		RepoID: repoID, PRNumber: event.prNumber, HeadSHA: event.headSHA,
		EventType: eventType, ReceivedAt: h.now(),
	})
	if errors.Is(err, db.ErrGitHubDeliveryConflict) {
		http.Error(w, "delivery conflicts with prior binding", http.StatusConflict)
		return
	}
	if err != nil {
		http.Error(w, "delivery admission failed", http.StatusInternalServerError)
		return
	}
	state, err := h.github.RefetchCIState(r.Context(), repoID, event.prNumber)
	if err != nil {
		http.Error(w, "authoritative GitHub refetch failed", http.StatusServiceUnavailable)
		return
	}
	if _, err := h.store.ConfirmGitHubDelivery(deliveryID, state, h.now()); err != nil {
		if errors.Is(err, db.ErrGitHubStateMismatch) {
			http.Error(w, "authoritative GitHub state does not match delivery", http.StatusConflict)
			return
		}
		http.Error(w, "delivery confirmation failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func validWebhookSignature(secret []byte, header string, body []byte) bool {
	if !strings.HasPrefix(header, "sha256=") || len(header) != len("sha256=")+sha256.Size*2 {
		return false
	}
	provided, err := hex.DecodeString(strings.TrimPrefix(header, "sha256="))
	if err != nil || len(provided) != sha256.Size {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	return hmac.Equal(mac.Sum(nil), provided)
}

func supportedWebhookEvent(eventType string) bool {
	switch eventType {
	case "pull_request", "check_run", "check_suite", "workflow_run":
		return true
	default:
		return false
	}
}

type webhookBinding struct {
	repository string
	prNumber   int64
	headSHA    string
}

type webhookPayload struct {
	Number     int64 `json:"number"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	PullRequest *struct {
		Head struct {
			SHA string `json:"sha"`
		} `json:"head"`
	} `json:"pull_request"`
	CheckRun    *webhookCheckObject `json:"check_run"`
	CheckSuite  *webhookCheckObject `json:"check_suite"`
	WorkflowRun *webhookCheckObject `json:"workflow_run"`
}

type webhookCheckObject struct {
	HeadSHA      string `json:"head_sha"`
	PullRequests []struct {
		Number int64 `json:"number"`
		Head   struct {
			SHA string `json:"sha"`
		} `json:"head"`
	} `json:"pull_requests"`
}

func extractWebhookEvent(eventType string, body []byte) (webhookBinding, error) {
	var payload webhookPayload
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&payload); err != nil {
		return webhookBinding{}, err
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return webhookBinding{}, err
	}
	repository, err := canonicalGitHubRepository(payload.Repository.FullName)
	if err != nil {
		return webhookBinding{}, err
	}
	var prNumber int64
	var headSHA string
	switch eventType {
	case "pull_request":
		if payload.PullRequest == nil {
			return webhookBinding{}, errors.New("missing pull request")
		}
		prNumber, headSHA = payload.Number, payload.PullRequest.Head.SHA
	case "check_run":
		prNumber, headSHA, err = exactCheckBinding(payload.CheckRun)
	case "check_suite":
		prNumber, headSHA, err = exactCheckBinding(payload.CheckSuite)
	case "workflow_run":
		prNumber, headSHA, err = exactCheckBinding(payload.WorkflowRun)
	default:
		err = errors.New("unsupported event")
	}
	if err != nil || prNumber <= 0 || !isGitHead(headSHA) {
		return webhookBinding{}, errors.New("event has no exact PR/head binding")
	}
	return webhookBinding{repository: repository, prNumber: prNumber, headSHA: headSHA}, nil
}

func exactCheckBinding(object *webhookCheckObject) (int64, string, error) {
	if object == nil || len(object.PullRequests) != 1 {
		return 0, "", errors.New("check event must identify exactly one pull request")
	}
	head := object.PullRequests[0].Head.SHA
	if head == "" {
		head = object.HeadSHA
	}
	return object.PullRequests[0].Number, head, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func canonicalGitHubRepository(value string) (string, error) {
	if len(value) < 3 || len(value) > 200 || strings.Count(value, "/") != 1 {
		return "", errors.New("invalid repository name")
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." {
			return "", errors.New("invalid repository component")
		}
		for _, char := range part {
			if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
				(char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.') {
				return "", fmt.Errorf("invalid repository character")
			}
		}
	}
	return strings.ToLower(value), nil
}

func isGitHead(value string) bool {
	if len(value) != 40 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
