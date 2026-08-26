# Azure worker transport owner contract

No-mistakes keeps its existing local execution path by default. Azure worker
execution is enabled only by the operator's global `azure_worker` block. A
repository configuration cannot enable it or select executable paths.

The configured `runner_path` is a trusted Firstmate-owned high-level wrapper,
not `fm-azure-runner.sh` and not a direct Azure command. No-mistakes invokes it
with this exact argv shape:

```text
<runner_path> --config <config_path> execute \
  --request <absolute-request.json> \
  --payload <absolute-payload-directory> \
  --result <absolute-result.json> \
  --outcome <absolute-outcome.bundle> \
  --step-outcome <absolute-step-outcome.json>
```

No job identity or artifact path is passed through environment variables. The
wrapper process receives only a bounded controller environment (`PATH`, a
private temporary `HOME`/`TMPDIR`, C locale, and disabled Git prompting). The
private payload directory contains exactly:

- `repo.bundle`: one ref, `HEAD`, at the requested exact commit
- `brief.md`: one `no-mistakes.azure-worker-step-input/v1` JSON object whose
  SHA-256 is in the request envelope. It binds `run_id`, `repo_id`,
  `step_result_id`, `step`, `round`, `desired_head_sha`, `base_sha`, `branch`,
  `fixing`, `previous_findings_json`, `user_intent`, and `user_intent_source`.

`request.json` uses `no-mistakes.firstmate-worker-request/v1`. It binds the job,
run, canonical step name, step-result ID, kind, round, desired head, input digest, owner-decision head, desired
generation, attempt, lease owner, and monotonic lease fence. It also binds the
source bundle digest and size, the required guest argv, the expected result
schema, and Firstmate's `fm.worker-return-contract/v1` result family.

The Firstmate wrapper owns task assignment, account selection, isolated Azure
worker lifecycle, heartbeat-independent VM cleanup, and the mapping onto
`fm-worker-lifecycle` semantics. It must stage a compatible no-mistakes runtime
and run the request's exact guest argv. It must not ask no-mistakes to construct
or impersonate a Firstmate assignment.

The wrapper writes one closed `no-mistakes.worker-step-outcome/v1` object to
`step-outcome.json` and binds its SHA-256 in one
`no-mistakes.firstmate-worker-result/v1` object at `result.json`, then exits
zero. The semantic object contains only `step`, `needs_approval`,
`auto_fixable`, `findings_json`, `exit_code`, `fix_summary`,
`review_approved_head_sha`, `skipped`, and `skip_remaining`. Review and repair
outcomes bind review approval to the exact output head; test outcomes cannot
assert review authority. This separate digest-bound object prevents a remote
finding from being collapsed into a clear/pass transport result.

Every request binding must be echoed exactly. Unknown fields, trailing data,
missing files, wrong identities, wrong heads, and stale fences fail closed.
Review and test jobs must return the input head and no bundle. A successful
repair returns a new descendant commit plus a single-ref `outcome.bundle`; its
declared SHA-256, ref, and commit must match. No-mistakes verifies and
materializes that commit on a new
`no-mistakes/azure-results/*` branch without checking it out or changing the
source worktree. The canonical pipeline adopts that branch only by a clean,
exact-head, fast-forward check and a run-head database CAS.

The wrapper must never put prompts, review prose, diffs, command output,
credentials, or raw model output in the result envelope. Its result is admitted
only by the live DB lease and completion CAS. Timeout and wrapper transport
failures consume the bounded infrastructure attempt budget; malformed, stale,
or invalid returned results fail terminally. Losing the heartbeat/fence cancels
the wrapper process tree and admits nothing.

When enabled, the daemon stores content-addressed inputs and exact-bound result
records privately below `NM_HOME/azure-worker`, starts one bounded loop for
each of review, repair, and test, and resumes queued/expired leased jobs after a
restart. Those three unexpired fenced leases are the only Azure capacity and
updater-liveness signal. CI waits, parked gates, raw run status, and daemon
touch timestamps consume no Azure worker capacity.
