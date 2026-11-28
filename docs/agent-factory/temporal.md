# Agent Factory: Temporal design

Binding: [architecture.md](architecture.md) (D1-D34, the state table, state ownership); the registry and the regulation:
[governance.md](governance.md). Facts: [audit.md](audit.md).
Spec sections: "spec 5.2". Numbers are named settings; defaults are set in the measuring phase. "Call" marks a design
choice made here, open to veto. Table and column names are those of [database.md](database.md).

## Workflows

| Workflow | Queue | Id | Purpose |
|---|---|---|---|
| `TaskWorkflow` | `control` | `task-<task uuid>` | The only writer of task progress. One per task, child tasks included (they are ordinary tasks, not Temporal child workflows). |
| `MachineLivenessWorkflow` | `control` | `machine-liveness` | Started by a Temporal Schedule; reads last-seen times from the API, marks a silent machine OFFLINE, raises one notice per outage. |

| `PhysicalDevelopmentWorkflow` | `control` | `physical-dev-<program id>` | D34 (call: a separate workflow). The development programme of one platform or skill, beside `TaskWorkflow` with the same acceptance and audit: its life is months, so it is not one task. Stage, hypotheses, experiments, evidence, reviews and limits are rows in PostgreSQL ([database.md](database.md), Robotics tables); the workflow only sequences and waits. After a restart it resumes an unfinished experiment only after the real state of the robot is established, never from its history alone. Each physical task it makes is an ordinary task. Built when robotics starts; stages and gates in [robotics.md](robotics.md), Phases. |

No other workflow exists. A physical task is an ordinary task: a child task of a `TaskWorkflow` like any other (same
state table, same operation matrix), with dangerous operations and the physical BLOCKED kinds below; it has no workflow of
its own. The outbox relay is a loop in the Control Worker process, not a workflow. Priorities, queues of
tasks and concurrency limits (spec 17) are checked in `SelectAgent` against PostgreSQL, not by a second scheduler. The
relay starts a workflow only after it reads the task's state in PostgreSQL: a task in a final state, or one whose
workflow already ran, never starts again, so an expired history cannot restart it (architecture, state ownership). The rows
of one task are delivered in insertion order, a delivery has a limit of attempts, and a `sent` row is closed to `done` once
PostgreSQL shows its effect: see "Delivery order" below.

`TaskWorkflow` keeps in memory: status, `revision`, counters (`attempt`, `rework`, `autoFix`), `executionId` and machine of
the current run (empty when no run is alive), worktree reference, `pauseRequested`, `stopping`, `endUnconfirmed`,
`resumeTo`, `blockKind`, `blockHandler`, `blockReason`, `allowedAgents` (agents the owner allowed for this task), the map
`handled` (decision id to outcome), the settings snapshot, `regulationVersionId` (the version the current run
started with, stored as `task_runs.regulation_version_id`; rules are always read from the active version, G10 of
governance.md). Waiting for a decision or a dependency is `condition()` on this state: no worker is held and no timer
polls. Every state change is a call to `PersistTaskEvent` before the next step. Names in the database are the snake_case
forms (`block_kind`, `block_handler`, `resume_to`, `pause_requested`, `end_unconfirmed`). A standing task of an ordinary
session has no workflow (database.md) and none of this applies to it.

Main path by state (all moves follow the architecture's table; each arrow is one `PersistTaskEvent`):
1. CREATED: `PrepareTask` (loads task and requirements revision) then PLANNING.
2. PLANNING: planner run (same machine activities as any run); `CreateChildTasks` creates children that are inside the
   requirements (D19). The scope check is in PostgreSQL; out of scope goes to BLOCKED `scope`. Input that no attempt can
   make valid goes to FAILED. Then READY.
3. READY: `ResolveDependencies`; open dependencies go to WAITING_DEPENDENCIES, none go on to ASSIGNED.
4. WAITING_DEPENDENCIES: waits for the Signal `wake`; each wake re-runs `ResolveDependencies` (a read of PostgreSQL). A
   dependency that ended in a final state other than ACCEPTED goes to BLOCKED `dependency` (D15).
5. ASSIGNED: `SelectAgent`, `PrepareWorkspace`, `StartAgentExecution`. No machine or no eligible agent: stays ASSIGNED,
   `NotifyUser` once, as a notice to the chief engineer with the failed filters (the owner sees it only by the
   owner-load rule, BLOCKED kinds and exits below); the wait ends on `wake`, on `cancel` or on `allow_agent`.
   There is no timeout that moves the task (call: a silent wait is made visible by the notice, and `notice.repeatHours`
   repeats it, see Open, decided in its phase).
6. RUNNING: `MonitorExecution` until an outcome; then `CollectResults`, `UploadResults`. Outcome `finished` goes to
   VERIFYING (or PAUSED if `pauseRequested`); `died` starts a new attempt in the same worktree within `attempt.max` and
   budget limits (ASSIGNED, D16); `unclear` goes to BLOCKED `execution` after the read-only inspection settles nothing;
   `limit` to BLOCKED `usage_limit`; `question` to BLOCKED `agent_question` after the question gate (below); a
   non-retryable error to FAILED.
7. VERIFYING: `RunAutomatedChecks`, then `RequestReview` (picks a reviewer of another provider than the implementer, D20;
   the review run is the same activities). Pass goes to AWAITING_APPROVAL; confirmed findings to REWORK_REQUIRED;
   `rework.maxRounds` reached to BLOCKED `rework_limit`. Details below. Input no attempt can make valid goes to FAILED.
8. AWAITING_APPROVAL: waits for a decision Update. `PublishArtifacts` runs only after ACCEPT, on the machine, by key,
   with the second press of D23 already recorded where the publication is the owner's (G15).
9. After a final state the workflow completes. `CleanupWorkspace` runs only through the owner's cleanup command or the chief engineer's grant (D24, G15),
   delivered as an outbox row of kind `cleanup`; nothing runs by itself.

### Verification, review and mandatory checks (D20, D25, D26)

- **Checks (D26).** A mandatory check that ends `not_run` first starts an agent run to repair its cause (tool path,
  missing file, a broken command), up to `autoFix.max` times; the check then runs again. A check still not run after
  that goes to BLOCKED `check_environment` with the reason, handled first by the chief engineer; it is never counted as
  passed and never ignored.
- **Reviewer (D20).** `RequestReview` selects an enabled agent whose provider differs from the providers of the
  implementer runs, that is not itself an agent of an implementer run, and whose declared `capabilities.roles`
  (agent-adapters.md, `getCapabilities`) lists `reviewer`. No spec line settled whether a planner may review; the rule is
  that any agent, a planner included, reviews only when its `capabilities.roles` lists `reviewer`. The lowest agent id
  of the eligible ones is picked. When the reviewer's usage limit is spent, the task stays VERIFYING with the reason "waiting for the
  reviewer's limit" and one notice to the chief engineer, and the workflow sleeps until the known reset time (`limit.maxWait` caps one sleep;
  without a known reset time the task goes to BLOCKED `usage_limit`). When no such agent exists the task goes to BLOCKED
  `check_environment`.
- **Findings (D25).** Each review finding has a kind: `defect`, `requirement_change`, `suggestion`. The implementer's
  next run answers every `defect`: `accepted` (it is fixed in the rework, confirmed) or `disputed`. A disputed technical
  finding goes to one fresh settling reviewer run (not an implementer run, `disputes_review_id` set) whose outcome is
  `confirmed` or `dismissed`, and that is final. A dispute about requirements, scope or cost goes to the owner as BLOCKED
  `scope`. A `requirement_change` or `suggestion` never blocks acceptance: it becomes a proposal row for the owner.
  Confirmed findings send the task to REWORK_REQUIRED, bounded by `rework.maxRounds`.

## Retry classes (spec 5.5)

Classes follow the architecture's error table; settings `retry.<class>.maxAttempts|initialInterval|backoffCoefficient|
maxInterval`. Agent died, limit, question, unclear are typed return values, not exceptions: Temporal retries only
infrastructure faults. A new agent attempt is a workflow loop bounded by `attempt.max` and budget.

| Class | Temporal retry | Error types | Exhaustion |
|---|---|---|---|
| `infra` | yes, bounded by schedule-to-close | `DbUnavailable`, `ApiUnavailable`, `NetworkError` | BLOCKED `execution`, notice to its handler |
| `workerLost` | yes, same machine queue; first step is a lookup | heartbeat timeout | BLOCKED `execution` |
| `agentDied` | no; workflow loop | outcome `died` | BLOCKED `execution` after `attempt.max` |
| `unclear` | no | outcome `unclear` | BLOCKED `execution` at once, after the inspection |
| `usageLimit` | no; workflow sleeps until the reset time when known (`limit.maxWait`) | outcome `limit` | BLOCKED `usage_limit` |
| `invalid` | no | `ValidationError`, `PermissionDenied`, `BudgetExceeded`, `BadInput` (non-retryable) | FAILED when no attempt can make the input valid (only in PLANNING, RUNNING and VERIFYING); otherwise BLOCKED `execution` (permission, budget) or `scope` |
| `external` | only after checking the target by idempotency key | `PublishFailed` | BLOCKED `execution` |
| `physical` (D34) | **never**: maximum attempts 1, no activity or workflow retry | a command to a robot, a stop request, a lease change | an unknown outcome is BLOCKED `physical_unknown`; a refusal by the runtime is a typed result, not an error |

Every exhaustion into BLOCKED goes to the handler of its kind first (below): the chief engineer, unless the cause needs
the owner.

## BLOCKED kinds and exits

`blockKind` is set when the task enters BLOCKED and decides the exit (architecture). The decision is `resolve_block` with
a choice; a choice that does not belong to the kind is refused `block_kind_mismatch`. Before blocking, agents try to
settle a technical cause (D18).

**Who handles a block first.** The task also carries `blockHandler` (`chief_engineer` or `owner`, `tasks.block_handler`),
set with the kind by a rule in code when the task enters BLOCKED. `scope` and `agent_question` enter with `owner`. The
other five kinds (`dependency`, `execution`, `usage_limit`, `check_environment`, `rework_limit`) and the physical kinds
`safe_stop`, `physical_unknown` and `maintenance` enter with `chief_engineer`, and `quarantine` with `owner`, unless the
cause needs the owner: a requirement, the scope, a cost, a dangerous operation, or an
owner's own credential such as a login; then they enter with `owner`. A routine qualified physical action is not a
dangerous operation and asks no owner (robotics.md, Ordinary physical work needs no owner); a `physical_unknown` is the
chief engineer's even when a person might have to look: a person is asked only when no allowed observation remains, and
then as a dependency with the smallest human part, not as the owner's decision. A start that may have people in the
zone, or that has nobody attending and no valid unattended policy, is no new kind: the task stays ASSIGNED and waits for
the person's start press or the owner's two-press confirmation listing each open reason (robotics.md "People in the zone,
and attendance" and Table 2; `people_may_be_present`, `unattended_policy_missing`). The engineer's resolution is a `resolve_block`
decision row with its run (`engineer_run_id`) and channel `api`, applied by the same handler as the owner's. The choices
are those of the kind, except one that raises a cost, token or time budget or does a dangerous operation: that is the
owner's (`422 owner_only`). The engineer's extra choice `call_owner` leaves the status as it is, sets
`blockHandler = owner` once and raises the owner's notice. The owner may resolve any block and wins over a concurrent
engineer resolution: the row version decides and the loser is refused `block_handled`. A `usage_limit` block reaches the
owner only when no enabled agent of any provider can run. With the chief engineer unavailable the block waits and nobody
is told (G13). Leaving BLOCKED clears the handler. The owner hears of the five kinds only by `call_owner` or when the
block entered as `owner`; this is the whole of the owner-load rule for blocks, kept by the check `block-owner-load`.

**The question gate (G9, governance.md).** When the registry is on, `agent_question` and BLOCKED `scope` raised by an
agent's question go through the gate on the server before the owner is asked: the question needs an inquiry id of the run,
a verdict on every found record, and a second search by the real text. A failed check or a verdict
`applies` sends the found records to the agent in a new run of the same attempt budget (`attempt.max`); when that budget
is spent, the task goes to the owner with the question and the records. The other kinds (`dependency`, `execution`,
`usage_limit`, `check_environment`, `rework_limit` and the physical ones) are not questions of an agent: they never pass the gate and go to the
chief engineer first (above). After `registry.gateTimeoutMs` the question passes (fail open), so a down
registry never holds a task. The owner's answer to the poll is recorded as a registry record by the API from the press
(not by the workflow), and the next run's inquiry finds it.

| Kind | Entered when | Handled first by | Choices | Exit |
|---|---|---|---|---|
| `scope` | plan or finding outside the approved scope; a dispute about requirements, scope or cost | owner | `answer` | PLANNING |
| `dependency` | a dependency ended without ACCEPTED (D15) | chief engineer | `remove_dependency`, `replace_dependency` (a new dependency inside the project) | READY |
| `execution` | infra or worker faults exhausted; attempts or budget exhausted; unclear state the inspection settled nothing (D16); a failed publication | chief engineer | `retry`, `raise_limit` (when the cause is a limit; the owner's if it raises a cost) | ASSIGNED |
| `usage_limit` | agent limit with no known reset time, or beyond `limit.maxWait` | chief engineer; the owner only if no enabled agent of any provider can run | `retry` | ASSIGNED |
| `agent_question` | the agent asked the owner | owner | `answer` | ASSIGNED |
| `check_environment` | a mandatory check could not run after repair (D26); no reviewer of another provider (D20) | chief engineer; the owner when the fix is an owner's credential or a cost | `retry` (after the cause is fixed, for example an agent enabled) | VERIFYING |
| `rework_limit` | `rework.maxRounds` reached | chief engineer | `raise_limit` (the owner's if it raises a cost) | REWORK_REQUIRED |
| `safe_stop` (D34) | a safe stop of the platform: a request, a safety event or a lost link | chief engineer | `release_after_check` (a new hardware check, then a new permission), `call_owner` | ASSIGNED, never a retry; the platform is `stopped` until checked |
| `physical_unknown` (D34) | the real physical state is unknown after a command, a restart or a lost link | chief engineer; a person only when no allowed observation remains, then a dependency with the smallest human part, never the owner's decision | `observe` (read the real state first), `call_owner` | ASSIGNED with a new action formed from the observed state; the platform leaves `unknown` only by observation |
| `maintenance` (D34) | repair, calibration or a part change; power isolated | chief engineer (or the handler the record names) | `finish_maintenance` (calibration and check recorded) | ASSIGNED after a new check and a new permission |
| `quarantine` (D34) | after an incident: the platform cannot take a lease | owner (the notice is on the critical list) | `release_quarantine` (owner only) | ASSIGNED after a new check and a new permission |

**Physical kinds (D34, [robotics.md](robotics.md)).** They are new `blockKind` values, no new task state. Their exit is
always ASSIGNED and never the old attempt: no physical operation is retried (below), and a lease, a qualification and the
runtime's own checks are asked again. A release from them is refused only for a missing self check of this boot or a
pending re-enrolment (`release_requires_check`), never for an unconfirmed state (robotics.md Table 1). A platform in `reserved` (equipment busy, charging) does not block the task: it
stays ASSIGNED with the reason and its queue position.

A block whose handler is the chief engineer also has the choice `call_owner` for it (the status stays, the handler
becomes the owner), and the owner may resolve any row. A block entered as `owner` (a cause that needs the owner) shows
the owner as its handler from the start and the engineer has no choice on it.

**A right request (G7, G13-G17, D31, governance.md "Rights").** A run that needs a right it does not have asks the check
and waits for the answer without holding a machine; the task stays RUNNING or VERIFYING, and the right request adds no BLOCKED kind.
The wait for the chief engineer is a run of role `chief_engineer`, and its server steps are local activities (D28). Each
step of the answer is its own immutable `right_answers` row (the chief engineer's, the code's, the owner's), so
`call_owner` is not the final word: the owner's answer is recorded as a further row and the grant points at the row that
gave it. Only the answer `call_owner` (and a request on the owner's list, G15) goes through the gate above and reuses
BLOCKED `agent_question`. An unavailable chief engineer (limit, outage) leaves the request waiting: no BLOCKED, no notice
to the owner, nobody else answers; the client shows the waiting requests, and each is answered once when it is back. A
run of the chief engineer for an ordinary session belongs to the project's standing task (database.md), which has no
workflow: the request and its answer are rows only. A grant belongs to one participant of the task: the next attempt
keeps it, a finished or died run does not end it, and it ends with the task, by the owner's revocation, or at once by a
critical event (`ended_by = 'critical'`, written with the suspension in one transaction).

**Assessment, terms of limits and replacement (D32, [personnel.md](personnel.md)).** No workflow is added. Facts are
written by the Control Worker as local activities at the end of a run, a check or a review (`RecordPersonnelFact`, keyed
by the event key). The assessment cycle and the end of a limit's term are a loop in the Control Worker process, like the
outbox relay: it computes assessments, files incidents and lifts a limit whose term ran out, each as a local activity
with its own key, so a repeat after a crash returns the stored result. A run of the chief engineer or of the
independent evaluator is a run of its own role, and its server steps are local activities (D28). The critical reaction
(stop the run, close the rights, notify the owner), from phase 4, is no workflow step: it is one transaction of the
control component (`af_control`): the suspension, the grants ended `critical`, the tokens revoked, the audit row
`rights.suspend`, the outbox order `stop_run` (the run killed by its identity, whole tree) and the notice to the owner,
with the same `StopExecution` and `NotifyUser` activities carrying out the outbox orders. It never waits for a workflow
or the Temporal server. A measure or a replacement decided by the owner is an ordinary recorded decision read by
`ReadDecision`. Nothing is added to the
operation matrix and personnel adds no BLOCKED kind.

Cancel is open to every kind. The owner's feedback and revision are open too (matrix below).

## Activities (spec 5.2 and the architecture's own)

Timeouts per activity are settings `act.<Name>.<kind>`: S start-to-close, Q schedule-to-start (machine queues only,
bounds the wait for a machine), H heartbeat, C schedule-to-close. "Key" is the idempotency key.

**Server steps are local activities (D28).** Rows marked `local` run inside the Control Worker process that runs the
workflow task: no activity task is created, so nothing can be polled or answered by another certificate. The Control
Worker sets `enableNonLocalActivities: false` and workflow code reaches these steps only through
`proxyLocalActivities`. Consequences:
- A local activity may run again after a crash of the Control Worker, also with one attempt allowed, so each write is
  idempotent by its key. The call that repeats a recorded write returns the stored result.
- It is short: `act.<Name>.S` of a `local` row stays below the workflow task timeout (a check derives the bound from the
  setting). Anything long belongs to a machine activity.
- A wait longer than `localRetryThreshold` becomes a server timer, so a PostgreSQL outage holds no worker.
- `MachineLivenessWorkflow` follows the same rule.
- `RecordPrecedent` is a local activity for the same reason: the registry write is a server step, and no machine
  certificate can answer it. It exists from phase 3; the registry itself (phase 2) works without it.
- A machine activity's result is data about a run. No machine result is read as a decision of the owner.

| Activity | Queue / where | Key | Timeouts | Class | Non-retryable | Escalation |
|---|---|---|---|---|---|---|
| `PrepareTask` | local | task id + revision | S C | infra | unknown task | BLOCKED `execution` |
| `ResolveDependencies` | local | read only | S C | infra | cycle | BLOCKED `dependency` |
| `SelectAgent` | local | task id + attempt | S C | infra | no agent allowed by profile | stays ASSIGNED, notice |
| `ReadDecision` | local | decision id | S C | infra | actor, revision or single-use check fails | outcome `refused` with a reason, task unchanged |
| `RecordDecisionOutcome` | local | decision id | S C | infra | none | none; the relay re-reads an unresolved decision |
| `PersistTaskEvent` | local | event id, row version | S C | infra | version conflict is re-read, not failed | BLOCKED `execution` |
| `NotifyUser` | local, writes a notice via API; Telegram client delivers | notice key | S C | infra | none | none (a lost notice is shown in the client) |
| `CreateChildTasks` | local | origin task + proposal id | S C | infra | depth, count, budget, scope limit | the proposal stays `open` for the owner |
| `RequestReview` | local (selects reviewer, creates review record) | task + round | S C | infra | no agent of another provider | BLOCKED `check_environment` |
| `PrepareWorkspace` | `machine-<id>` | execution id | S Q C | workerLost | repo missing, dirty base | BLOCKED `execution` |
| `InspectWorkspace` | machine | read only | S Q | workerLost | none | BLOCKED `execution` |
| `StartAgentExecution` | machine | execution id (intent file first) | S Q C | workerLost | `invalid` | BLOCKED `execution` |
| `MonitorExecution` | machine | execution id | S Q H | workerLost | none; returns outcome | per outcome |
| `StopExecution` | machine | execution id | S Q C | workerLost | none | stays CANCELLING, notice |
| `CollectResults` | machine | execution id | S Q C | workerLost | report schema invalid becomes a finding, not an error | REWORK_REQUIRED or BLOCKED `execution` |
| `UploadResults` | machine, via API | execution id + file hash | S Q C | infra | none | BLOCKED `execution`; files stay on the machine |
| `RunAutomatedChecks` | machine (also H when a check is long) | execution id + check id | S Q H | workerLost | tool missing becomes `not_run` | repair run (D26), then BLOCKED `check_environment` |
| `PublishArtifacts` | machine (credentials stay with the worker) | branch + target, checked first | S Q C | external | `invalid` | BLOCKED `execution` |
| `CleanupWorkspace` | machine | worktree path | S Q | workerLost | unpublished changes | stays, notice |
| `RunGpuJob` | `machine-<id>-gpu`, one slot | job key in the generator's queue | S Q H | workerLost | `invalid` | BLOCKED `execution` |
| `ReconcileMachine` | machine, at worker start before polling | none, read mostly | S | infra | none | machine not registered |
| `LoadSettings` | local | read only | S | infra | none | none |
| `RecordPersonnelFact` | local; phase 5 | event key | S C | infra | none | none; a lost write is repeated by its key. Written by the server from a recorded run, check or review, never from an agent's report |
| `RunAssessment` | local; phase 8 | agent + period | S C | infra | below the minimum facts (result "not enough data") | none; the result is stored, a repeat returns it |
| `ApplyMeasureTerm` | local; phase 8 | measure id + term end | S C | infra | none | none; lifts a limit whose term ended, audited |
| `ObservePhysicalState` | local, reads the robot's confirmed state through the API; D34, when robotics starts | robot id + request id | S C | infra | none | read only; the state it returns is the confirmed state (also whether the robot confirmed the end of its last lease), never a request |
| `IssuePhysicalCommand` | local, through the API to the robot gateway; D34 | `command_id` (the runtime deduplicates by it) | S | physical | stale or ended lease, configuration or qualification; a lease that is not the active one; a gate-3 reference that is not exactly one of qualification and admission | no retry: an unknown outcome is BLOCKED `physical_unknown` and ends the lease in the same transaction, recalling the commands not yet fetched; the next action is formed after `ObservePhysicalState`, under a new lease issued only as Table 1 of robotics.md allows (`lease_ending` otherwise), and gets a new `command_id` |
| `RecordPrecedent` | local; writes through `RegistryService` | task id + run id + case (settled dispute, resolved block, answered question) | S C | infra | none | none; a lost write is repeated by its key. Writes only an agent `precedent` or a `draft`, never an active owner record |

Heartbeat details of `MonitorExecution` and `RunGpuJob` are the log offset and the execution id, so a retry attaches. The
SDK throttles heartbeats to the smaller of 0.8 x heartbeat timeout and 60 s; `act.*.H` is set above that. Cancellation of
an activity reaches it only on a heartbeat, so the workflow never relies on it for stopping an agent: it calls
`StopExecution`, which acts on the runner directly.

## Operations (spec 5.3)

All operations are Updates delivered by the outbox relay; the Update id is the decision id (one decision has exactly one `deliver_decision` outbox row, 1:1 by `decision_id`). Ids are `approve`, `reject`,
`request_changes`, `cancel`, `pause`, `resume`, `revise_requirements`, `add_feedback`, `change_priority`,
`resolve_block`, `force_close`, `allow_agent`. They are exactly the decision kinds of `task_decisions` (database.md) except `physical_decision` (D34): the chief
engineer's routine physical decision is applied at insert on the standing or physical task, is never an Update and has
no outbox row or handler.
Signals exist only for wake-ups carrying no decision: `wake` (dependency ended, machine back, agent or rule changed).

**Rule of recording.** A mutation is recorded, never applied, until the workflow answers. Every press is recorded,
refused ones included; the outcome is `applied` or `refused` with a reason code written by `RecordDecisionOutcome`. A
refusal changes no task state and is shown to the owner.

Order of checks: (1) the **validator**, deterministic, sees only the Update and workflow memory, and checks form only:
the operation id is known, the payload has the fields of its kind, the Update id is not in `handled`. It cannot run
activities and does not look at state or revision. (2) The **handler** takes a mutex so one decision applies at a time,
calls `ReadDecision` (the actor is an owner or a relay press that maps to an owner, or for `resolve_block` the chief
engineer's run when the block's handler is the chief engineer; the revision is the current one for kinds that carry it;
the decision is not already resolved), then looks up the cell of the matrix, applies the effect,
calls `PersistTaskEvent` and then `RecordDecisionOutcome`, and stores the outcome in `handled`. A repeated id returns the
stored outcome and has no effect. A refusal inside the handler returns the refusal code to the relay (no exception, no
workflow failure), so Update failures mean only faults.

Refusal codes: `not_allowed_in_state`, `revision_stale`, `actor_not_allowed`, `already_resolved`, `block_kind_mismatch`,
`block_handled`, `task_closed`, `invalid_payload`. `invalid_payload` is a `change_priority` whose `priority` is not a finite
number or an `allow_agent` whose `agent_id` is not a non-empty string; the validator sees only the decision id, so the
handler finds it after `ReadDecision`, as the last check, after the state, block kind and revision checks. The first four, `block_kind_mismatch` and `block_handled` (a `resolve_block` of the
chief engineer after the owner took the block, or after `call_owner` made the owner the handler; the loser of the race
by row version) come from the handler; an engineer's choice that raises a cost or does a dangerous operation is refused
`owner_only` by the API before any outbox row exists (HTTP 422, not a workflow refusal). `task_closed` is written by the relay when
the workflow is already closed or the task is final in PostgreSQL (the relay marks the outbox row and calls
`RecordDecisionOutcome` through the API path, not a workflow).

Rules that hold in the whole matrix:
- **L (live run).** A run is alive when `executionId` is set and the machine has not confirmed its end. Cancel goes to
  CANCELLING while a run is alive, else to CANCELLED. Pause is a flag while a run is alive, else it moves at once.
- **Cancel from CANCELLING.** A note: the task is already ending. `force_close` is open only here (D21): CANCELLED with
  `endUnconfirmed` set; on reconnect the worker stops such a run before anything else (`ReconcileMachine`).
- **PAUSED (D22).** Only `resume`, `cancel`, `add_feedback`, `revise_requirements` apply. Everything else is refused
  `not_allowed_in_state` and tells the owner to resume first.
- **Second press (D23).** `cancel`, `reject`, `force_close` and the owner's part of publications need a second press in the same channel. The
  API holds the first press as a `confirmations` row and records the decision on the second; the workflow sees only
  completed decisions.

Legend: `R` refused (`not_allowed_in_state`); `closed` refused `task_closed`; `note` recorded, no move; `->X` moves to
X; `flag` sets a flag, no move; `stop` stops a live run first (rule L). `*` see below the table.

| State | approve | reject | request_changes | cancel | pause | resume | revise_requirements | add_feedback | change_priority | resolve_block | force_close | allow_agent |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| CREATED | R | R | R | ->CANCELLED | ->PAUSED | R | ->PLANNING | note | applies | R | R | R |
| PLANNING | R | R | R | L | P | clears flag, else R | stop, ->PLANNING | note | applies | R | R | R |
| READY | R | R | R | ->CANCELLED | ->PAUSED | R | note, taken at start | note | applies | R | R | R |
| WAITING_DEPENDENCIES | R | R | R | ->CANCELLED | ->PAUSED | R | note, taken at start | note | applies | R | R | R |
| ASSIGNED | R | R | R | L | P | clears flag, else R | note, taken at start | note | applies | R | R | allows the agent, re-runs `SelectAgent` |
| RUNNING | R | R | R | ->CANCELLING | flag | clears flag, else R | stop, then ->ASSIGNED | note | note, next attempt | R | R | R |
| VERIFYING | R | R | R | L | P | clears flag, else R | stop, ->REWORK_REQUIRED | note | note | R | R | R |
| AWAITING_APPROVAL | ->ACCEPTED | ->REJECTED | ->REWORK_REQUIRED | ->CANCELLED | ->PAUSED | R | ->REWORK_REQUIRED | note | note | R | R | R |
| REWORK_REQUIRED | R | R | R | ->CANCELLED | ->PAUSED | R | note, taken by the next attempt | note | note | R | R | R |
| BLOCKED | R | R | R | L | ->PAUSED, kind kept | R | * | note | note | the exit of its kind | R | R |
| PAUSED | R | R | R | L | R | ->`resumeTo`, or the held move | note, `resume` goes to PLANNING * | note | R | R | R | R |
| CANCELLING | R | R | R | note, already ending | R | R | R | R | R | R | ->CANCELLED, `endUnconfirmed` | R |
| ACCEPTED, REJECTED, CANCELLED, FAILED | closed | closed | closed | closed | closed | closed | closed | closed | closed | closed | closed | closed |

`P` is rule L for pause: `flag` when a run is alive, else `->PAUSED`. `L` in a cancel cell is rule L for cancel.

`*` Revise in BLOCKED goes to PLANNING and clears the block, except in kind `execution` with an unsettled run
(inspection found nothing): the revision is stored, the task stays BLOCKED, and the owner's next `resolve_block` goes to
PLANNING instead of ASSIGNED (call: a new run must never start beside a run that may be alive). `resolve_block` of kind
`execution` is the handler's word that the old run ended (the chief engineer's, or the owner's after `call_owner`). The
same holds in PAUSED entered from such a BLOCKED: cancel follows
rule L, and `resume` returns to BLOCKED with the revision stored. The same holds for the physical kinds `safe_stop`,
`physical_unknown` and `quarantine` (D34): a revise is stored and the task stays BLOCKED, because the platform is not
confirmed safe; the `resolve_block` of such a kind goes to PLANNING instead of ASSIGNED when a revision is stored, and in
every case only after the platform's state is confirmed by the robot. **No retry of a physical operation:** `retry` is
not a choice of any physical kind, and the activity class `physical` never repeats; after a stop, a restart or an unknown
outcome the next step is `ObservePhysicalState`, then a new action with a new `command_id` under a new lease (issued
only as robotics.md Table 1 allows, else `lease_ending`) and a new check
of the qualification or admission. An unknown outcome, a safe stop, a stop request and a change of agent each end the
lease and recall its unfetched commands in one transaction, before any later step. `approve`, `reject`,
`request_changes` and `resolve_block` carry the revision the deciding actor saw; `revise_requirements` carries the revision it
builds on; a stale one is refused `revision_stale`. In ASSIGNED, if `StartAgentExecution` is already scheduled the intent
may exist: pause is a `flag`, cancel is CANCELLING, and a started run takes the RUNNING cell. RUNNING with `stopping` set
(a revision stop) shows RUNNING until the machine confirms the end, and then `stopping` clears. A `flag` is applied when
the run ends, before the next move: `pauseRequested` makes the workflow go to PAUSED with `resumeTo` set to the move it
held back. `change_priority` "applies" in states before RUNNING and is `note` after: it affects only work that has not
started.

### Every state has an exit

| State | Forward exit (no owner) | Owner exit | Closing exit |
|---|---|---|---|
| CREATED | PLANNING | pause, revise | cancel |
| PLANNING | READY, BLOCKED `scope`, FAILED | pause, revise | cancel |
| READY | WAITING_DEPENDENCIES, ASSIGNED | pause, revise (stored) | cancel |
| WAITING_DEPENDENCIES | ASSIGNED, BLOCKED `dependency` | pause, revise (stored) | cancel |
| ASSIGNED | RUNNING, BLOCKED (the run could not be prepared), FAILED (a non-retryable preparation error) | `allow_agent`, pause, revise (stored) | cancel |
| RUNNING | VERIFYING, ASSIGNED (died), BLOCKED, FAILED | pause (held), revise | cancel (CANCELLING) |
| VERIFYING | AWAITING_APPROVAL, REWORK_REQUIRED, BLOCKED, FAILED | pause (held), revise | cancel |
| AWAITING_APPROVAL | none; waits for the owner | approve, reject, request_changes, revise, pause | cancel |
| REWORK_REQUIRED | ASSIGNED | pause, revise | cancel |
| BLOCKED | `resolve_block` by the chief engineer when `blockHandler = chief_engineer`; otherwise waits for the owner (`quarantine`: the owner; `physical_unknown`: the chief engineer) | `resolve_block`, revise, pause | cancel |
| PAUSED | none; waits for the owner | resume, revise | cancel |
| CANCELLING | CANCELLED when the machine confirms | `force_close` | none needed |

A test walks this table and the matrix (testing.md): no cell is empty, every non-final state reaches a final state by at
least one path that needs no unavailable actor, and a state that waits for the owner has an owner operation to leave it.
AWAITING_APPROVAL and PAUSED wait for the owner by design; BLOCKED waits for its handler, the chief engineer first and
the owner when `blockHandler = owner`, and the owner can always resolve it; the owner's cancel is always possible, so
none is a dead end. CANCELLING waits for the machine and has `force_close` for a machine that never returns.

## Delivery order

The relay ([modules.md](modules.md), `control/outbox-relay.ts`) leases rows with `for update skip locked` on the database
clock and delivers at least once; the repeat is harmless because Temporal and `handled` deduplicate by the decision id.
Three rules sit on top of that, and the settings they use are chosen, not measured (no measurement run exists; they are
`settings` rows with defaults in code, read at relay start).

**Per-task order.** A row with a `task_id` is claimable only when no older row of the same task (lower `seq`) is `pending`
(leased or waiting for its retry time) or `failed`. The rule is in the claim query itself, so two relays on one database
cannot deliver two rows of one task out of order, and one claim takes one row per task. A `sent` or `done` older row does
not block: `sent` already reached Temporal, and what follows it is ordered by the workflow's own mutex. Rows without a
task id are not ordered.

**Retry limit.** A failed delivery releases the row with a delay and counts the claim in `attempts`. The delay is
`min(relay.retryMaxSeconds, relay.retrySeconds x 2^(attempts - 1))`: 30 s, 60 s, 120 s, 240 s, 480 s, then 600 s. When
`attempts` reaches `relay.maxAttempts` (8, about 35 minutes of trying) the row goes to `failed` and the relay raises one
notification `retry_limit_exceeded` for it (dedup key `outbox-failed:<outbox id>`, the row's `task_id`), so the owner sees
a stuck delivery once, not on every round; if the notice cannot be written the row is released instead, never failed
unseen. A row that no retry can fix (an unknown kind, a missing task or decision, a `deliver_decision` row without them) takes the same road at once, on its first claim. `failed` is not
retried by the relay, and it blocks the later rows of its task, because delivering them past a lost one would break the
order. The owner acts through administration: closes the row (`done`) when its effect is not needed, or puts it back to
`pending` to try again (one open notice per row, so a second failure does not raise another until the first is resolved).
Defaults: `relay.leaseSeconds` 60, `relay.retrySeconds` 30, `relay.maxAttempts` 8, `relay.retryMaxSeconds` 600.
A lease that expired (a relay that died) is taken over by the next claim and counts as an attempt as well.

**Sent and done.** `sent` is not final for the two kinds whose effect PostgreSQL can show: the index of database.md
(`next_attempt_at where state in ('pending','sent')`) keeps `sent` rows in the relay's working set, the cleanup rule keeps
only `done` rows, and the relay "re-reads an unresolved decision" (`RecordDecisionOutcome`, above). So `start_workflow`
and `deliver_decision` end `sent` when Temporal accepted them, and a sweep closes them to `done` when PostgreSQL shows the
effect: for `start_workflow` the task is no longer `CREATED`, has a `workflow_run_id`, or is gone; for `deliver_decision`
the decision is `applied` or `refused`, or is gone. An unconfirmed `sent` row is looked at again after `relay.retrySeconds`
(`next_attempt_at`), never delivered again by the sweep. `wake` (a Signal), `stop_run` and `cleanup` (an activity return)
have nothing further to confirm and end `done` directly, as does `notify`.

## Continue-As-New

Triggered when `continueAsNewSuggested` is true or `history.maxEvents` is passed, only at a quiet point: no activity in
flight and `allHandlersFinished()`. A long `MonitorExecution` does not force it (heartbeats add no events). It carries:
task id, status and flags (`pauseRequested`, `stopping`, `endUnconfirmed`, `resumeTo`, `blockKind`, `blockHandler`, `blockReason`),
`revision`, `attempt`, `rework`, `autoFix`, current `executionId` and machine, worktree reference, `allowedAgents`,
`handled` (decision ids and outcomes), dependency and child ids, the settings snapshot, `regulationVersionId`, budget used, the contract
version. The Update dedupe of Temporal does not cross a roll-over; `handled` and the unique key in PostgreSQL do.

## Failure scenarios (spec 8), step by step

**A. Local Rig closed.** (1) Server keeps `TaskWorkflow`; machine activities wait on `machine-<id>`. (2) A running
`MonitorExecution` fails by heartbeat timeout (`workerLost`); a not yet started one waits until `act.*.Q`. (3) The agent
lives on under `rig exec` or the Codex supervisor and stops by its runner time limit. (4) On return the worker runs
`ReconcileMachine` first (executions folder, process identity, worktrees, open cancels and force-closes from the API),
then polls. (5) The retried `StartAgentExecution` finds its intent and applies the architecture's lookup table (collect,
attach, new attempt, start, inspection, then BLOCKED); Q expiry with the machine silent goes to BLOCKED `execution`.

**B. Windows restart.** (1) Autostart (if enabled) starts the worker; without it the situation is A. (2) The worker
registers again with platform, contract version, tool paths. (3) `ReconcileMachine` checks leftover processes by pid and
creation time, worktrees by Git state, no cleanup. (4) Activities that retry hit the lookup table, so a run that
survived is attached and never started twice. (5) A run that did not survive and left a started marker is `died`.

**C. Temporal Server down.** (1) State is in persistence; workers and the relay get connection errors and back off. (2)
Decisions wait in the outbox. (3) Agents continue; their results stay in the execution folder. (4) After recovery
workflows resume; activity timers fire as recorded, so a completed attempt with its result already stored is
collected by the lookup, not repeated. External effects retry only after the target check (`external`).

**D. PostgreSQL down.** (1) API refuses mutations, nothing is confirmed. (2) Server steps fail as `infra` and retry;
`PersistTaskEvent` is idempotent by event id. (3) After recovery the relay re-sends outbox rows in `pending` (and closes `sent` rows once their effect is visible,
"Delivery order"); a row already applied is recognised by `handled` (or by the unique key), so no decision is lost or doubled. (4) The projection
is rebuilt from the workflow's own events if a row version lags: the workflow is the writer.

**E. Agent dies while changing code.** (1) `MonitorExecution` returns `died` or `unclear` from the lookup table. (2)
`InspectWorkspace` records Git state and file changes (see the adapters document for the two files of Rig's temp hook).
(3) `died`: new attempt in the same worktree, session resumed where the adapter allows, results of the earlier attempt
kept, within `attempt.max` and budget (D16). (4) `unclear`: never a second run; a read-only inspection of processes,
folder and Git; if it settles the state the matching row applies, else BLOCKED `execution` and the chief engineer
chooses `retry` or cancel (or `call_owner`).

**F. Network isolation.** (1) The worker starts nothing new without a connection; no decision needing the server is taken
locally. (2) Running processes end by their runner time limit or finish. (3) On reconnect `ReconcileMachine` uploads
what the folder holds and reports it; the workflow decides from the outcome. (4) Conflicts: an execution the server
considers cancelled or superseded (`stopping`, revision changed, new attempt, force-closed) is stopped before anything
else; a result for a stale revision is stored but marked `stale` and never advances the task.

**G. Duplicated events.** (1) Repeated Update id: server dedupe inside the run, `handled` across roll-overs, unique key
in PostgreSQL before. (2) Repeated Signal `wake`: harmless, it only re-reads. (3) Repeated activity: key per activity
(table above). (4) `PublishArtifacts`: first checks the remote branch and pull request by key, creates only if absent,
and stores the URL. (5) A duplicate `StartAgentExecution` finds the intent and never starts a second agent. (6) A
repeated hub press id is refused by the API's unique key before any outbox row exists.

## Limits of child tasks (D19, spec 9.1, 17)

Checked in `CreateChildTasks` inside one PostgreSQL transaction, not in workflow code: `child.maxDepth`,
`child.maxPerParent`, the parent's budget rolled up over its descendants, `rework.maxRounds`, `autoFix.max`,
`attempt.max`. Each child row has `origin_type`, `origin_task_id`, `origin_run_id`, `depth` (database.md). A proposal of
kind `decomposition` whose `covers` ids all exist in the parent's acceptance criteria and that is inside the limits
becomes a task by itself. Everything else, every `requirement_change` and `suggestion`, is a `task_proposals` row in
`open` for the owner; a limit reached leaves the proposal `open` with the reason and does not block the parent.
Dependency cycles are refused at creation. FAILED is final (call): work goes on in a new task with `origin_type =
continuation`.

## Replay and versions
- Workflow code in `factory/workflows/` imports only types and workflow APIs: no clock, random, network, environment or
  settings reads; time and ids come from the Workflow API; settings arrive from `LoadSettings` into the snapshot.
- Every release carries a deployment version (Worker Versioning); its general-availability status is to be checked
  (audit 8.5). Until confirmed, a code change on a live path uses `patched()`, removed by `deprecatePatch()` later.
- Replay of saved histories (one per state path, from integration tests) runs against new code by hand before a release
  (there is no CI, see testing.md); a failed replay stops the release.
- A release that cannot replay a running task keeps its workers online until it finishes or rolls over. The contract
  version is in every activity input; an older machine gets no work (architecture, Migrations).

## Open, decided in its phase

| Point | Why it is the owner's | Phase | Until then |
|---|---|---|---|
| Budgets of subscription agents (what counts as spend when a run is paid by a subscription) | cost, and when a run stops | 5 (several agents) | `max_tokens` and `max_cost` are unset; only the usage limit of the provider stops a run |
| Repeat interval of notices (`notice.repeatHours`) | what the owner sees | 6 | an open notice is not repeated |
