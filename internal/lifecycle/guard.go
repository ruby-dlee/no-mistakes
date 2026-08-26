package lifecycle

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/paths"
)

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

func ShortSHA(sha string) string {
	if len(sha) <= 8 {
		return sha
	}
	return sha[:8]
}
