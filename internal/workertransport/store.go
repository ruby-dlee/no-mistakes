package workertransport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/db"
)

const durableResultSchema = "no-mistakes.azure-worker-durable-result/v1"

// DurableStore owns private controller-side worker inputs and admitted result
// records. Inputs are content-addressed so an idempotent enqueue can recover
// after daemon restart without regenerating a prompt from mutable state.
type DurableStore struct {
	root    string
	inputs  string
	results string
}

type DurableResult struct {
	Schema            string              `json:"schema"`
	JobID             string              `json:"job_id"`
	RunID             string              `json:"run_id"`
	StepResultID      string              `json:"step_result_id"`
	Kind              db.PipelineJobKind  `json:"kind"`
	Step              StepOutcomeStep     `json:"step"`
	Round             int                 `json:"round"`
	DesiredHeadSHA    string              `json:"desired_head_sha"`
	InputDigest       string              `json:"input_digest"`
	OwnerDecisionHead string              `json:"owner_decision_head"`
	DesiredGeneration int64               `json:"desired_generation"`
	LeaseFence        int64               `json:"lease_fence"`
	ResultDigest      string              `json:"result_digest"`
	OutputHeadSHA     string              `json:"output_head_sha"`
	ReturnedBranch    string              `json:"returned_branch,omitempty"`
	StepOutcome       StepOutcomeEnvelope `json:"step_outcome"`
}

func NewDurableStore(root string) (*DurableStore, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return nil, errors.New("Azure worker durable store root must be absolute and clean")
	}
	store := &DurableStore{
		root: root, inputs: filepath.Join(root, "inputs"), results: filepath.Join(root, "results"),
	}
	for _, dir := range []string{store.root, store.inputs, store.results} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("create Azure worker durable store: %w", err)
		}
		if err := os.Chmod(dir, 0o700); err != nil {
			return nil, fmt.Errorf("protect Azure worker durable store: %w", err)
		}
	}
	return store, nil
}

func (s *DurableStore) PutInput(data []byte) (string, error) {
	if s == nil || len(data) == 0 || len(data) > maxInputBytes || bytes.IndexByte(data, 0) >= 0 {
		return "", errors.New("Azure worker durable input is empty, oversized, or binary")
	}
	digest := digestBytes(data)
	path := filepath.Join(s.inputs, digest+".input")
	if existing, err := readRegularBounded(path, maxInputBytes); err == nil {
		if digestBytes(existing) != digest || !bytes.Equal(existing, data) {
			return "", errors.New("Azure worker durable input conflicts with existing digest")
		}
		return digest, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err := writePrivateAtomic(path, data); err != nil {
		return "", err
	}
	return digest, nil
}

func (s *DurableStore) InputFor(_ context.Context, job *db.PipelineJob) ([]byte, error) {
	if s == nil || job == nil || !validDigestString(job.InputDigest) {
		return nil, errors.New("Azure worker durable input binding is invalid")
	}
	data, err := readRegularBounded(filepath.Join(s.inputs, job.InputDigest+".input"), maxInputBytes)
	if err != nil {
		return nil, err
	}
	if digestBytes(data) != job.InputDigest {
		return nil, errors.New("Azure worker durable input digest mismatch")
	}
	return data, nil
}

func (s *DurableStore) StoreResult(_ context.Context, job *db.PipelineJob, execution Execution) (func(), error) {
	if s == nil || job == nil || !safeJobID(job.ID) {
		return nil, errors.New("Azure worker durable result binding is invalid")
	}
	record := DurableResult{
		Schema: durableResultSchema, JobID: job.ID, RunID: job.RunID, StepResultID: job.StepResultID,
		Kind: job.Kind, Step: execution.StepOutcome.Step, Round: job.Round, DesiredHeadSHA: job.DesiredHeadSHA,
		InputDigest: job.InputDigest, OwnerDecisionHead: job.OwnerDecisionHead,
		DesiredGeneration: job.DesiredGeneration, LeaseFence: job.LeaseFence,
		ResultDigest: execution.ResultDigest, OutputHeadSHA: execution.OutputHeadSHA,
		ReturnedBranch: execution.ReturnedBranch, StepOutcome: execution.StepOutcome,
	}
	data, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	data = append(data, '\n')
	path := filepath.Join(s.results, job.ID+".json")
	if err := writePrivateAtomic(path, data); err != nil {
		return nil, err
	}
	writtenDigest := digestBytes(data)
	rollback := func() {
		current, err := readRegularBounded(path, maxResultBytes)
		if err == nil && digestBytes(current) == writtenDigest {
			_ = os.Remove(path)
		}
	}
	return rollback, nil
}

func (s *DurableStore) ReadResult(job *db.PipelineJob) (*DurableResult, error) {
	if s == nil || job == nil || !safeJobID(job.ID) {
		return nil, errors.New("Azure worker durable result binding is invalid")
	}
	data, err := readRegularBounded(filepath.Join(s.results, job.ID+".json"), maxResultBytes)
	if err != nil {
		return nil, err
	}
	var result DurableResult
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errors.New("Azure worker durable result has trailing data")
	}
	if result.Schema != durableResultSchema || result.JobID != job.ID || result.RunID != job.RunID ||
		result.StepResultID != job.StepResultID || result.Kind != job.Kind || result.Round != job.Round ||
		result.DesiredHeadSHA != job.DesiredHeadSHA || result.InputDigest != job.InputDigest ||
		result.OwnerDecisionHead != job.OwnerDecisionHead || result.DesiredGeneration != job.DesiredGeneration ||
		result.ResultDigest == "" || job.ResultDigest == nil || result.ResultDigest != *job.ResultDigest ||
		result.OutputHeadSHA == "" || job.OutputHeadSHA == nil || result.OutputHeadSHA != *job.OutputHeadSHA {
		return nil, errors.New("Azure worker durable result exact binding mismatch")
	}
	if result.Step != result.StepOutcome.Step ||
		(job.Kind == db.PipelineJobReview && result.Step != StepOutcomeReview) ||
		(job.Kind == db.PipelineJobTest && result.Step != StepOutcomeTest) ||
		(result.Step != StepOutcomeReview && result.Step != StepOutcomeTest) {
		return nil, errors.New("Azure worker durable result step binding mismatch")
	}
	if _, err := decodeStepOutcomeMustMatch(result.StepOutcome, result.Step, result.OutputHeadSHA); err != nil {
		return nil, err
	}
	return &result, nil
}

func decodeStepOutcomeMustMatch(outcome StepOutcomeEnvelope, step StepOutcomeStep, outputHead string) (StepOutcomeEnvelope, error) {
	data, err := json.Marshal(outcome)
	if err != nil {
		return outcome, err
	}
	return decodeStepOutcome(data, step, outputHead)
}

func writePrivateAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".worker-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func safeJobID(value string) bool {
	if value == "" || len(value) > 128 || strings.Contains(value, "..") {
		return false
	}
	for _, char := range value {
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') &&
			(char < '0' || char > '9') && char != '-' && char != '_' {
			return false
		}
	}
	return true
}

func validDigestString(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}
