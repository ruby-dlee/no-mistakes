package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
)

type CoordinatorStore interface {
	ScheduleDueCIReconciliations(at time.Time, limit int) (int, error)
	PendingCIReconciliationWork(limit int) ([]db.CIReconciliationWork, error)
	ApplyCIReconciliation(result db.CIReconciliationResult) (bool, error)
}

type CIStateReducer interface {
	ReduceCI(ctx context.Context, work db.CIReconciliationWork, state db.AuthoritativeGitHubState) (db.CIWaitStatus, error)
}

type ServiceOptions struct {
	Store          CoordinatorStore
	GitHub         GitHubStateClient
	Reducer        CIStateReducer
	BatchSize      int
	MaxConcurrency int
	Interval       time.Duration
	Now            func() time.Time
	OnError        func(error)
}

type Service struct {
	store          CoordinatorStore
	github         GitHubStateClient
	reducer        CIStateReducer
	batchSize      int
	maxConcurrency int
	interval       time.Duration
	now            func() time.Time
	onError        func(error)
}

func NewService(options ServiceOptions) (*Service, error) {
	if options.Store == nil || options.GitHub == nil || options.Reducer == nil {
		return nil, errors.New("coordinator service requires store, GitHub client, and reducer")
	}
	if options.BatchSize < 1 || options.BatchSize > 100 {
		return nil, errors.New("coordinator batch size must be 1..100")
	}
	if options.MaxConcurrency < 1 || options.MaxConcurrency > 16 {
		return nil, errors.New("coordinator concurrency must be 1..16")
	}
	if options.Interval < time.Second || options.Interval > 24*time.Hour {
		return nil, errors.New("coordinator interval must be 1 second..24 hours")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &Service{
		store: options.Store, github: options.GitHub, reducer: options.Reducer,
		batchSize: options.BatchSize, maxConcurrency: options.MaxConcurrency,
		interval: options.Interval, now: options.Now, onError: options.OnError,
	}, nil
}

// Run owns one process-wide ticker. Transient reconciliation failures are
// reported and retried durably on a later tick; cancellation is a clean stop.
func (s *Service) Run(ctx context.Context) error {
	if ctx.Err() != nil {
		return nil
	}
	s.processAndReport(ctx, s.now())
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case at := <-ticker.C:
			s.processAndReport(ctx, at)
		}
	}
}

func (s *Service) processAndReport(ctx context.Context, at time.Time) {
	if err := s.ProcessOnce(ctx, at); err != nil && s.onError != nil {
		s.onError(err)
	}
}

func (s *Service) ProcessOnce(ctx context.Context, at time.Time) error {
	if _, err := s.store.ScheduleDueCIReconciliations(at, s.batchSize); err != nil {
		return fmt.Errorf("schedule CI reconciliations: %w", err)
	}
	work, err := s.store.PendingCIReconciliationWork(s.batchSize)
	if err != nil {
		return fmt.Errorf("load CI reconciliations: %w", err)
	}
	if len(work) == 0 {
		return nil
	}
	workerCount := min(s.maxConcurrency, len(work))
	queue := make(chan db.CIReconciliationWork)
	var workers sync.WaitGroup
	var errorMu sync.Mutex
	var workErrors []error
	worker := func() {
		defer workers.Done()
		for item := range queue {
			if err := s.processOne(ctx, item, at); err != nil {
				errorMu.Lock()
				workErrors = append(workErrors, err)
				errorMu.Unlock()
			}
		}
	}
	workers.Add(workerCount)
	for range workerCount {
		go worker()
	}
	for _, item := range work {
		select {
		case queue <- item:
		case <-ctx.Done():
			close(queue)
			workers.Wait()
			return ctx.Err()
		}
	}
	close(queue)
	workers.Wait()
	return errors.Join(workErrors...)
}

func (s *Service) processOne(ctx context.Context, work db.CIReconciliationWork, at time.Time) error {
	state, err := s.github.RefetchCIState(ctx, work.Wait.RepoID, work.Wait.PRNumber)
	if err != nil {
		return fmt.Errorf("refetch CI state for wait %s failed", work.Wait.ID)
	}
	if state.RepoID != work.Wait.RepoID || state.PRNumber != work.Wait.PRNumber || state.HeadSHA != work.Wait.HeadSHA {
		return fmt.Errorf("refetch CI state for wait %s: exact binding changed", work.Wait.ID)
	}
	status, err := s.reducer.ReduceCI(ctx, work, state)
	if err != nil {
		return fmt.Errorf("reduce CI state for wait %s failed", work.Wait.ID)
	}
	applied, err := s.store.ApplyCIReconciliation(db.CIReconciliationResult{
		WaitID: work.Wait.ID, RepoID: work.Wait.RepoID, Branch: work.Wait.Branch,
		PRNumber: work.Wait.PRNumber, HeadSHA: work.Wait.HeadSHA,
		InputDigest: work.Wait.InputDigest, DesiredGeneration: work.Wait.DesiredGeneration,
		Status: status, CheckState: state.CheckState, AppliedAt: at,
	})
	if err != nil {
		return fmt.Errorf("apply CI state for wait %s: %w", work.Wait.ID, err)
	}
	if !applied {
		return fmt.Errorf("apply CI state for wait %s: result not admitted", work.Wait.ID)
	}
	return nil
}
