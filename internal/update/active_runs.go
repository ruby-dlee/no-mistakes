package update

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/lifecycle"
)

func (u *updater) confirmActiveRunsBeforeUpdate() error {
	now := time.Now()
	if u.now != nil {
		now = u.now()
	}
	leases, err := lifecycle.ActiveWorkerLeases(u.paths, now)
	if err != nil {
		return fmt.Errorf("check active pipeline worker leases: %w", err)
	}
	if len(leases) == 0 {
		return nil
	}

	u.writeActiveWorkerWarning(leases)
	if u.force {
		fmt.Fprintln(u.stderrWriter(), "FORCE: continuing update and daemon restart despite active pipeline worker leases")
		return nil
	}

	return fmt.Errorf("refusing update because %d active pipeline worker leases are in progress; pass --force to stop/restart the daemon anyway", len(leases))
}

func (u *updater) writeActiveWorkerWarning(leases []*db.PipelineJob) {
	leaseWord := "leases"
	if len(leases) == 1 {
		leaseWord = "lease"
	}
	fmt.Fprintf(u.stderrWriter(), "warning: update will restart the daemon while %d active pipeline worker %s are in progress\n", len(leases), leaseWord)
	fmt.Fprint(u.stderrWriter(), lifecycle.WorkerLeaseList(leases))
	fmt.Fprintln(u.stderrWriter(), "continuing can cause these workers to fail")
}

func readYes(input io.Reader) bool {
	if input == nil {
		input = os.Stdin
	}
	response, err := bufio.NewReader(input).ReadString('\n')
	if err != nil && response == "" {
		return false
	}
	answer := strings.ToLower(strings.TrimSpace(response))
	return answer == "y" || answer == "yes"
}
