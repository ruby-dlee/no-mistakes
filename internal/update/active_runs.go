package update

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/lifecycle"
)

func (u *updater) confirmActiveRunsBeforeUpdate() error {
	now := time.Now()
	if u.now != nil {
		now = u.now()
	}
	work, err := lifecycle.ActiveWork(u.paths, now)
	if err != nil {
		return fmt.Errorf("check active pipeline execution: %w", err)
	}
	if work.Count() == 0 {
		return nil
	}

	u.writeActiveWorkWarning(work)
	if u.force {
		fmt.Fprintln(u.stderrWriter(), "FORCE: continuing update and daemon restart despite active pipeline execution")
		return nil
	}

	return fmt.Errorf("refusing update because %d active pipeline executions are in progress; pass --force to stop/restart the daemon anyway", work.Count())
}

func (u *updater) writeActiveWorkWarning(work lifecycle.ActiveLifecycleWork) {
	executionWord := "executions"
	verb := "are"
	if work.Count() == 1 {
		executionWord = "execution"
		verb = "is"
	}
	fmt.Fprintf(u.stderrWriter(), "warning: update will restart the daemon while %d active pipeline %s %s in progress\n", work.Count(), executionWord, verb)
	fmt.Fprint(u.stderrWriter(), lifecycle.WorkList(work))
	fmt.Fprintln(u.stderrWriter(), "continuing can cause this work to fail")
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
