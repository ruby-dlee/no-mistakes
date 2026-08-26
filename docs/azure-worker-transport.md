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
  --outcome <absolute-outcome.bundle>
```

No job identity or artifact path is passed through environment variables. The
wrapper process receives only a bounded controller environment (`PATH`, a
private temporary `HOME`/`TMPDIR`, C locale, and disabled Git prompting). The
private payload directory contains exactly:

- `repo.bundle`: one ref, `HEAD`, at the requested exact commit
- `brief.md`: the bounded input whose SHA-256 is in the request envelope

`request.json` uses `no-mistakes.firstmate-worker-request/v1`. It binds the job,
run, step, kind, round, desired head, input digest, owner-decision head, desired
generation, attempt, lease owner, and monotonic lease fence. It also binds the
source bundle digest and size, the required guest argv, the expected result
schema, and Firstmate's `fm.worker-return-contract/v1` result family.

The Firstmate wrapper owns task assignment, account selection, isolated Azure
worker lifecycle, heartbeat-independent VM cleanup, and the mapping onto
`fm-worker-lifecycle` semantics. It must stage a compatible no-mistakes runtime
and run the request's exact guest argv. It must not ask no-mistakes to construct
or impersonate a Firstmate assignment.

The wrapper writes one closed `no-mistakes.firstmate-worker-result/v1` JSON
object to `result.json`, then exits zero. Every request binding must be echoed
exactly. Unknown fields, trailing data, missing files, wrong identities, wrong
heads, and stale fences fail closed. Review and test jobs must return the input
head and no bundle. A successful repair returns a new descendant commit plus a
single-ref `outcome.bundle`; its declared SHA-256, ref, and commit must match.
No-mistakes verifies and materializes that commit on a new
`no-mistakes/azure-results/*` branch without checking it out or changing the
source worktree.

The wrapper must never put prompts, review prose, diffs, command output,
credentials, or raw model output in the result envelope. Its result is admitted
only by the live DB lease and completion CAS. Timeout and wrapper transport
failures consume the bounded infrastructure attempt budget; malformed, stale,
or invalid returned results fail terminally. Losing the heartbeat/fence cancels
the wrapper process tree and admits nothing.
