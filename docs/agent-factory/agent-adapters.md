# Agent Factory: agent adapters, runner, checks, routing

Binding: [architecture.md](architecture.md). Facts: [audit.md](audit.md). Workflow side: [temporal.md](temporal.md).
Spec sections are quoted as "spec 6.1" and so on. Anything not backed by the audit is marked "to be checked". "Call"
marks a design choice made here, open to veto.

## AgentAdapter: operations by meaning (spec 6.1)

An adapter lives on the worker (machine), never on the server. It is the only code that knows a provider. Every
operation takes an `executionId` created on the server; the adapter keeps its facts in
`~/.agent-rig/factory/executions/<execution_id>/` (intent, launch id, started marker, output, exit, result).

| Operation | Meaning |
|---|---|
| `getCapabilities` | What the adapter declares (roles, tools, kinds of work, resume, cancel, usage numbers) joined with what `validateEnvironment` verified. A declared but unchecked capability is reported as unverified and is not used for routing. |
| `validateEnvironment` | Checks tool paths from machine settings, the account allowlist, model gates (spec 6.5 inputs), network to the provider, free disk. Returns a list of named checks with pass, fail or `unknown`. Side-effect free. |
| `start` | Writes the intent file first, then launches through the runner, returns the launch id. A second call with the same `executionId` never starts a second agent. |
| `attach` | Looks the run up and returns one of five outcomes of the architecture's recovery table: `collect`, `attach`, `died` (new attempt), `start`, `unclear` (BLOCKED `execution`, D16, handled first by the chief engineer). |
| `getStatus` | State (starting, running, exited, lost), usage so far, time of last output. Read only. |
| `streamOutput` | Output chunks from a byte offset, so a restart or a retry resumes and never repeats. |
| `cancel` | Idempotent. Stops the run by process identity (pid plus creation time), returns whether the end is confirmed; "not confirmed" keeps the task in CANCELLING until the machine confirms or the owner uses `force_close` (D21). The same kill by identity (the whole process tree of the `execution_id`) executes the `stop_run` order of the control component: a stop of one run, a stop of all runs of a project or of the factory, the end of a right held by a run's sandbox profile, and the critical reaction (one order for each live run of the agent; personnel.md, Critical). |
| `collectResult` | Reads the execution folder, validates the report schema (below), computes usage, returns the typed run outcome (machine contract 2: `collected`, `schemaFinding`, `died`, `limit` with the reset time, `question`, `unclear`, `review`, `settled`; temporal.md "Run outcome"). |
| `cleanup` | Removes provider leftovers (temp, caches, supervisor job folder after upload). Never touches a worktree. |

**Rights at start (G7, G13-G17, D31, governance.md "Rights").** `start` receives a profile that the server computes from
the participant's role, its active `permission` records and its live grants; the adapter never decides a right. Claude
runs also get the permission hook (installed by the owner at the PC) that asks the rights check during the run. Codex has
no per-call hook, so its sandbox is a snapshot (the snapshot rule, governance.md, Rights): the profile holds only role
rights and grants that are not single-use and not narrower than the sandbox can express; a `once` record and any
narrower right never enter it, and such an action is done as a step of its own by the worker or the control component,
checked and consumed at that moment, or needs a new attempt of the same participant, which keeps its grants. The adapter
stores the rights the profile was built from on the run (`task_runs.profile_rights`). A right of a running Codex job
cannot be taken back by a message: when a grant, record, regulation version, role assignment or the agent's standing ends
a right that the profile holds, the control component kills that run by its identity (`stop_run`, database.md, Where a
right can live) in the same transaction, and the next attempt starts with the profile of that moment, a suspended agent
with none. A suspension stops every live run of the agent, whatever its configuration.

Factory runs (`claude -p`) have no interactive prompt: when the registry or the rights check cannot be reached, the hook
denies the action instead of asking the owner. The fail-open prompt of an ordinary session and the meaning of the owner's
press on it are in governance.md, Ordinary sessions.

What an adapter cannot do (fixed limits of the providers, from the audit):

| Adapter | Cannot do |
|---|---|
| Claude (`claude -p`, stream JSON, fixed session id, inside `rig exec`) | Pause a running turn; survive the 50-run limit of `rig exec` (the runner copies the result into the execution folder at once and the run record is not relied on after that); cancel is `rig exec stop` with 5 s grace, then kill of the job (Windows kill-on-close). Exact usage fields of the result event: to be checked (audit task S3). |
| Codex (supervisor, job folder `~/.claude/codex-jobs/`) | Report usage as a stable schema (events are unversioned); detect a usage limit before it hits (detection is reactive, status `limit`); `cancel` safely without pid-and-creation-time identity (this is added); the launcher's own restarts and orphan limit must be switched off, the factory owns attempts. Statuses mapped: done, question, blocked, malformed, cancelled, orphaned, limit. |
| OpenAI API | Run tools in a worktree (only text and file outputs through our code); resume a lost request: an unanswered request is recorded as `unclear`, its retry counts against the budget. Streaming of partial output is an option of the call, not a given. |
| ComfyUI (HTTP and WebSocket: queue, watch, cancel, fetch) | Edit code or files in a worktree; cancel a node that is already inside a long GPU kernel before it ends; give a model-specific quality verdict. |
| LM Studio | The audit confirms the `lms` command line only. Chat endpoint, model loading and unloading by API, token usage, cancel of a request: to be checked. Until then the adapter is marked `experimental` and routing never selects it (call; it is lifted when the interface is checked, phase 5). |

## Environment checks

Run by `validateEnvironment` at worker registration, at every machine heartbeat summary, and before each `start`.
Checks are named and stored with the machine record. Minimal set per adapter:
- Claude: executable path, `rig exec` available, logged-in account is in the allowlist, requested model is allowed by
  the profile.
- Codex: executable and supervisor path, `codex-jobs` folder writable, account and model gates.
- OpenAI: key present in the machine secret store, endpoint reachable, model in the allowed list, monthly budget left.
- ComfyUI: HTTP and WebSocket reachable, needed models and nodes present, output folder writable, GPU queue free.
- LM Studio: `lms` found, `lms` can list models; the rest to be checked.
- Common: Git version, free disk above `disk.minFreeGb`, project repository path exists, clock offset against the server
  below `clock.maxSkew`.
A failed check marks that capability unavailable on that machine; it never stops other adapters.

## The runner (`factory/runner/`)

Started by `rig exec`; it is the process that outlives the worker (scenarios A and B).
1. Writes the started marker (execution id, pid, creation time, time of start) before anything else. The marker and the
   exit file, not the run record of `rig exec` (which keeps only 50 runs), are what proves `died` against `unclear`
   after a long outage.
2. Starts the agent with the command in the intent file; the command is built by the adapter, never by the runner.
3. Streams stdout and stderr into `output` (append-only, offset-addressable) and keeps a last-output time.
4. Enforces the time limit (`run.maxSeconds` from the settings snapshot, spec 17): at the limit it stops the agent,
   then writes the exit file with reason `timeout`.
5. On normal end writes `exit` (code, reason, time) then, if present, `result` from the agent's own report.
6. Honours a stop request file from `cancel`; a missing exit file with a dead process is how `died` is recognised.
The runner never talks to the server and holds no credentials of the project.

## Report schema (spec 10.1) and checks

The agent's final report is one JSON document validated by `collectResult`:
`actions`, `changedFiles`, `commits` (hash, subject), `testResults` (each: check id, command, exit code, status
`passed|failed|not_run`, log reference), `artifacts` (kind, path or URL, hash), `problems`, `uncheckedScenarios`,
`requirementDeviations`, `finalStatus` (`done|partial|failed`). Rules:
- A report that claims `passed` for a check with no stored run is a schema finding, not a result (spec: reporting a test
  that did not run is forbidden). The stored automated-check record wins over the agent's text.
- A missing field or an unknown `finalStatus` is a schema finding returned as `collectResult` data; the workflow turns
  it into REWORK_REQUIRED (one round) or BLOCKED.
- Changed files and commits are recomputed from Git by the worker, not trusted from the report; a difference is stored
  as `deviation`.

Automated checks (`RunAutomatedChecks`) run on the worker, by the workflow's decision, with commands from the project
profile, not from the agent. For each: command, working directory, exit code, duration, log (kept as a file, large
ones as a reference), status. A mandatory check that cannot run (tool missing, timeout) is `not_run` with the reason;
agents first repair its cause, and if it still cannot run the task is BLOCKED `check_environment` with the reason (D26),
handled first by the chief engineer (an owner's credential, such as a login, makes it the owner's).
It is never counted as passed and never goes to the owner as complete. Graphics tasks: automatic checks cover format, size, count and naming only;
visual quality and story conformance are never recorded as automatically confirmed, they need the reviewer or the owner.

## Usage metrics per adapter

Stored per execution: `provider`, `model`, `startedAt`, `endedAt`, `wallSeconds`, tokens in and out where given,
`costEstimate` with its price-table version, `outputs`, and `source` (`reported` or `estimated`).
In `task_runs.usage_metrics` the result maps to three keys: tokens in to `tokens_in`, tokens out to `tokens_out`,
`costEstimate` to `cost_estimate` (a number in the currency of the policy's `max_cost`). An adapter that does not report
a number leaves its key out; the run is then unmeasured for budgets (database.md, `CreateChildTasks`), never zero.
- Claude: wall time and token counts from the result event of the stream (field names to be checked in S3); cost is an
  estimate from the price table setting.
- Codex: wall time from `state.json`; tokens from the events where present (unversioned, to be checked); cost estimate.
  Limit state is recorded when `limit` is returned, with the reset time if the output carries it.
- OpenAI: the `usage` object of each response; cost from the price-table setting; a lost request is recorded with
  `source: unknown` and counted at the largest plausible cost against the budget.
- ComfyUI: GPU seconds (queue start to end), wait time in the GPU queue, count and size of outputs, model names.
- LM Studio: wall time only; tokens and loaded-model data to be checked.
Budgets (spec 17: token, cost, time limits) are compared in `SelectAgent` before a start and in `MonitorExecution` on
every status read; exceeding stops the run and applies the owner's chosen action (pause or wait for the owner).

## Routing inputs (spec 6.5)

`SelectAgent` has two stages and stores the whole decision (inputs, filters, ranking, chosen agent, reason) with the
event.
1. Hard filters: the task's required capability is present and verified; the adapter is available on an online machine;
   the platform fits (OS, tools); permissions of the profile allow the agent, tools and network; project limits
   (concurrent tasks, per-agent tasks, budget left) are not exceeded; the account and model gates pass; for review, the
   agent is in the task's `allowedAgents` when the owner restricted it (`allow_agent`); for review, the provider differs
   from the providers of the implementer runs (D20, spec 9.1).
2. Ranking among the eligible by: owner preference for the project (`routing_rules`), then estimated cost, then current
   load of the machine and queue; past outcomes break a tie (call: preference is the owner's word, cost is what the
   owner pays, load only orders equals).
No eligible agent: the task stays ASSIGNED with the list of failed filters and one notice to the chief engineer (to the
owner only when no enabled agent of any provider can run, or when the fix is the owner's: a cost, a login). The handler
answers with `allow_agent` or fixes the cause; a review with no other provider waits for one (D20).

## Worktrees

- One task owns one worktree and one branch; reuse for a new attempt and for rework; a child task gets its own.
- Before each start the worker records a baseline: HEAD, branch, `git status --porcelain`, and the list of untracked
  files. After the run it records the same and the diff; changes are what differs from the baseline.
- Rig's `project-tmp` hook (SessionStart) changes the worktree in two places: it creates `<project>/.tmp/` and writes
  `.claude/settings.local.json` (env block with TEMP, TMP, TMPDIR). It appends both paths to Git's `info/exclude`, not to
  `.gitignore`, so they are untracked but hidden from `git status`, and in a linked worktree the exclude file is the
  shared one of the repository. It also deletes entries of `.tmp/` older than three days. Consequences: both paths are
  excluded from "changes of the agent" and from commits; a project `.gitignore` is never edited by the factory; nothing
  valuable is kept in `.tmp/`; results are copied into the execution folder, not left in `.tmp/`; the baseline records
  whether the two paths existed before the run.
- Never `reset --hard`, never delete a branch, never force-push. `CleanupWorkspace` refuses if the worktree has unpublished
  commits or uncommitted changes and says which; removal of such a worktree is an owner command.
- Push and pull request creation run on the machine by the owner's decision, after the remote branch and request are
  looked up by key (spec 5.5, `external`).

## GPU queue (D12)

- One queue per machine, `machine-<id>-gpu`, with one worker slot; ComfyUI and LM Studio jobs share it, so only one
  GPU job runs at a time on a machine.
- One `RunGpuJob` activity is one job from submit to fetched outputs, with a heartbeat carrying the generator's own job
  id. After a worker restart the retry first asks the generator's queue by that id: finished (collect), running (attach),
  absent (resubmit within `attempt.max`).
- A task waiting for the GPU is ASSIGNED or VERIFYING with the reason "waiting for the GPU" and the queue position.
  A job that exceeds `gpu.maxSeconds` is cancelled in the generator, then the activity returns `died`.
- Ordering is by task priority, then first come (call). Temporal task order cannot do that, so the slot is a row in
  `gpu_leases` (database.md): a task requests it, the grant goes to the open request with the highest priority and
  oldest request time, and `RunGpuJob` starts only with a grant. A running job is never preempted. A loaded LM Studio
  model counts as holding the slot: loading or unloading a model while a ComfyUI job holds it is not allowed, the adapter
  asks for the slot first.

## Retention of execution folders (D24)

Nothing is deleted by itself. Execution folders stay on the machine after upload until the owner's cleanup command
(api.md) removes them; the command lists what it would remove and its size first. There is no `executions.keepDays`.

## Open, decided in its phase

| Question | What it changes for the owner | Phase |
|---|---|---|
| Budgets of subscription agents (what counts as "budget left" when the provider shows no number) | When routing skips an agent and when the owner is told to wait | 5 |
| Network access of a profile (which hosts a profile may reach; default off) | What an agent can fetch and leak | 4 |
| Codex login: ChatGPT plan or API key. The documentation recommends the key for automation, keeps the plan login for trusted private automation and says one `auth.json` must not be shared by concurrent jobs or machines and not used for public repositories | What Codex runs cost, whether they may run in parallel, whether public repositories may use the plan | 4 |
| Claude subscription in unattended runs: `claude -p` and `claude setup-token` are documented for scripts, the consumer terms forbid automated access unless explicitly permitted, the help center says `claude -p` draws from the plan's limits; own scheduled runs, a server and several machines are not addressed; the edition for the EEA and Switzerland also bars commercial or business use, other editions were not read | Whether Claude runs stay on the subscription or need an API key | 4 |

Sources and quotes: [provider-terms.md](../reports/provider-terms.md). Whatever is chosen, the adapters start only the unmodified `claude`
and `codex` programs with the owner's own login or key, and never read, store or pass on a login of either.

Spec 6.1 lists `getStatus` and `streamOutput` as adapter operations, while the architecture has the worker read the
execution folder through activities. Both are kept; `getStatus` is derived from the folder, not from the provider.
