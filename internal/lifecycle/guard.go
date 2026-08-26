package lifecycle

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/ipc"
	"github.com/kunchenguid/no-mistakes/internal/paths"
)

type ActiveLifecycleWork struct {
	LocalRuns    []*db.Run
	WorkerLeases []*db.PipelineJob
}

func (work ActiveLifecycleWork) Count() int {
	return len(work.LocalRuns) + len(work.WorkerLeases)
}

// ActiveRuns returns all pending/running pipeline runs from the local state DB.
func ActiveRuns(p *paths.Paths) ([]*db.Run, error) {
	if p == nil {
		return nil, nil
	}
	dbPath := p.DB()
	if _, err := os.Stat(dbPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat database: %w", err)
	}

	database, err := db.Open(dbPath)
	if err != nil {
		return nil, err
	}
	defer database.Close()
	return database.GetActiveRuns()
}

// ActiveWorkerLeases returns only exact, unexpired, fenced review, repair, or
// test leases. Historical pending/running run rows and durable CI waits do not
// represent executing workers and therefore cannot block an update.
func ActiveWorkerLeases(p *paths.Paths, at time.Time) ([]*db.PipelineJob, error) {
	if p == nil {
		return nil, nil
	}
	dbPath := p.DB()
	if _, err := os.Stat(dbPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat database: %w", err)
	}

	database, err := db.Open(dbPath)
	if err != nil {
		return nil, err
	}
	defer database.Close()
	liveness, err := database.UpdaterPipelineLiveness(at)
	if err != nil {
		return nil, err
	}
	return liveness.ActiveWorkerLeases, nil
}

// ActiveWork reports actual cancellable work: run goroutines owned by the
// current daemon process plus exact unexpired external worker leases. Durable
// run rows and CI waits remain recovery state, never process liveness.
func ActiveWork(p *paths.Paths, at time.Time) (ActiveLifecycleWork, error) {
	if p == nil {
		return ActiveLifecycleWork{}, nil
	}
	leases, err := ActiveWorkerLeases(p, at)
	if err != nil {
		return ActiveLifecycleWork{}, err
	}
	ids, err := activeExecutionRunIDs(p)
	if err != nil {
		return ActiveLifecycleWork{}, err
	}
	if len(ids) == 0 {
		return ActiveLifecycleWork{WorkerLeases: leases}, nil
	}
	database, err := db.Open(p.DB())
	if err != nil {
		return ActiveLifecycleWork{}, fmt.Errorf("open executing-run database: %w", err)
	}
	defer database.Close()
	runs := make([]*db.Run, 0, len(ids))
	for _, id := range ids {
		run, err := database.GetRun(id)
		if err != nil {
			return ActiveLifecycleWork{}, fmt.Errorf("read daemon executing run %s: %w", id, err)
		}
		if run == nil {
			return ActiveLifecycleWork{}, fmt.Errorf("daemon reports unknown executing run %s", id)
		}
		runs = append(runs, run)
	}
	return ActiveLifecycleWork{LocalRuns: runs, WorkerLeases: leases}, nil
}

func activeExecutionRunIDs(p *paths.Paths) ([]string, error) {
	socketExists, err := pathExists(p.Socket())
	if err != nil {
		return nil, fmt.Errorf("inspect daemon socket: %w", err)
	}
	pidExists, err := pathExists(p.PIDFile())
	if err != nil {
		return nil, fmt.Errorf("inspect daemon pid file: %w", err)
	}
	if !socketExists && !pidExists {
		return nil, nil
	}
	client, err := ipc.Dial(p.Socket())
	if err != nil {
		return nil, fmt.Errorf("inspect daemon execution liveness: %w", err)
	}
	defer client.Close()
	var result ipc.GetExecutingRunsResult
	if err := client.CallWithTimeout(ipc.MethodGetExecutingRuns, nil, &result, 5*time.Second); err != nil {
		return nil, fmt.Errorf("inspect daemon execution liveness: %w", err)
	}
	return result.RunIDs, nil
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func RunList(runs []*db.Run) string {
	if len(runs) == 0 {
		return ""
	}
	out := "active pipeline runs:\n"
	for _, run := range runs {
		out += fmt.Sprintf("  %s  %s  %s  %s\n", run.ID, run.Status, run.Branch, ShortSHA(run.HeadSHA))
	}
	return out
}

func WorkerLeaseList(leases []*db.PipelineJob) string {
	if len(leases) == 0 {
		return ""
	}
	out := "active pipeline worker leases:\n"
	for _, lease := range leases {
		owner := "unknown"
		if lease.LeaseOwner != nil && *lease.LeaseOwner != "" {
			owner = *lease.LeaseOwner
		}
		out += fmt.Sprintf("  %s  %s  run=%s  head=%s  owner=%s\n", lease.ID, lease.Kind, lease.RunID, ShortSHA(lease.DesiredHeadSHA), owner)
	}
	return out
}

func WorkList(work ActiveLifecycleWork) string {
	return RunList(work.LocalRuns) + WorkerLeaseList(work.WorkerLeases)
}

func ShortSHA(sha string) string {
	if len(sha) <= 8 {
		return sha
	}
	return sha[:8]
}
