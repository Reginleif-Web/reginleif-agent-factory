// TaskWorkflow lifecycle (temporal.md "TaskWorkflow", package 55): a task goes from CREATED to its end through the activities
// in the documented order, on the real Postgres bench (the tasks row follows the workflow through PersistTaskEvent; only the DB
// triggers write transitions) and on time-skipping Temporal with fake machine activities on queue `machine-m1`.
// Waits are bounded polls of the `state` query (no wall-clock sleeps); gates are test-owned promises.
import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";
import { drizzle } from "drizzle-orm/postgres-js";
import { ApplicationFailure, Context } from "@temporalio/activity";
import { Worker } from "@temporalio/worker";
import type { TestWorkflowEnvironment } from "@temporalio/testing";
import type { WorkflowHandle } from "@temporalio/client";
import { allTimeoutKeys } from "../control/activity-catalog.ts";
import { createControlActivities, type ControlActivities } from "../control/activities/index.ts";
import { CONTROL_QUEUE, taskWorkflowId } from "../control/workflow-client.ts";
import { migrate } from "../db/migrate.ts";
import { createRepo, type Repo } from "../db/repo/index.ts";
import { stateQuery, wakeSignal } from "../workflows/task.ts";
import type { TaskState, UpdateKind } from "../workflows/task-state.ts";
import { connectBench, createDatabaseBench } from "./database-bench.ts";
import { ActivityError, MACHINE_ACTIVITY_NAMES } from "./fakes/contracts.ts";
import { createMachineFakes, type MachineFakes } from "./fakes/machine-activities.ts";
import { collected, reviewPass } from "./fakes/outcomes.ts";
import { startTimeSkipping } from "./temporal/bench.ts";
import { controlBundle, sendUpdate } from "./temporal/task-stub.ts";

const benchPromise = createDatabaseBench(import.meta.url);
let bench: Awaited<typeof benchPromise>;
let migratorClient: ReturnType<typeof connectBench>;
let controlClient: ReturnType<typeof connectBench>;
let migratorRepo: Repo;
let controlRepo: Repo;
let env: TestWorkflowEnvironment;

const ids = (n: number): string => `28000000-0000-7000-8000-${String(n).padStart(12, "0")}`;
const owner = ids(1);
const project = ids(2);
const agent = ids(3);
const reviewer = ids(4);
const MACHINE = "m1";
let counter = 100;
const nextId = (): string => ids(counter++);

type Handle = WorkflowHandle;
type Row = { status: string; resume_to: string | null; pause_requested: boolean; stopping: boolean; assigned_agent_id: string | null; priority: number | null };

const dbRow = async (taskId: string): Promise<Row> => (await migratorClient<Row[]>`SELECT status, resume_to, pause_requested, stopping, assigned_agent_id, priority FROM tasks WHERE id = ${taskId}`)[0]!;

const newTask = async (): Promise<string> => {
	const id = nextId();
	await migratorRepo.tasks.create({ id, projectId: project, title: `lifecycle-${id}`, priority: 1 }, { id: nextId(), requirements: "test", acceptanceCriteria: [], constraints: {} });
	return id;
};

const seedDecision = async (taskId: string, kind: UpdateKind, extra: { revisionNumber?: number | null; payload?: unknown } = {}): Promise<string> => {
	const decisionId = nextId();
	const outboxId = nextId();
	await migratorRepo.decisions.record({
		decision: { id: decisionId, taskId, kind, actorId: owner, channel: "client", idempotencyKey: `key-${decisionId}`, requestHash: "h", deliveryState: "recorded", outboxId, revisionNumber: extra.revisionNumber ?? null, payload: extra.payload ?? {} },
		outbox: { id: outboxId, kind: "deliver_decision", taskId, decisionId, payload: { decision_id: decisionId }, state: "pending", attempts: 0 },
	});
	return decisionId;
};

/** What a test controls about one run: the agent the router finds, gates on machine activities, and what each call saw. */
interface Harness {
	taskId: string;
	handle: Handle | null;
	fakes: MachineFakes;
	agentAvailable: boolean;
	gates: Map<string, { promise: Promise<void>; open: () => void }>;
	seen: { name: string; db: string; wf: string | null }[];
	aborts: Set<AbortController>;
}

const gate = (): { promise: Promise<void>; open: () => void } => {
	let open!: () => void;
	const promise = new Promise<void>((resolve) => { open = resolve; });
	return { promise, open };
};

const harness = async (): Promise<Harness> => ({
	taskId: await newTask(), handle: null, fakes: createMachineFakes(MACHINE), agentAvailable: true, gates: new Map(), seen: [], aborts: new Set(),
});

const observe = async (h: Harness, name: string): Promise<void> => {
	const db = (await dbRow(h.taskId)).status;
	const wf = h.handle === null ? null : (await h.handle.query(stateQuery)).status;
	h.seen.push({ name, db, wf });
};

const machineActivities = (h: Harness): Record<string, (input: unknown) => Promise<unknown>> => {
	const activities: Record<string, (input: unknown) => Promise<unknown>> = {};
	for (const name of MACHINE_ACTIVITY_NAMES) {
		activities[name] = async (input) => {
			const context = Context.current();
			const abort = new AbortController();
			h.aborts.add(abort);
			const relay = (): void => abort.abort(context.cancellationSignal.reason);
			context.cancellationSignal.addEventListener("abort", relay, { once: true });
			try {
				await observe(h, name);
				const hold = h.gates.get(name);
				if (hold !== undefined) {
					await new Promise<void>((resolve, reject) => {
						void hold.promise.then(resolve);
						abort.signal.addEventListener("abort", () => reject(abort.signal.reason), { once: true });
					});
				}
				return await h.fakes[name].run(input, { signal: abort.signal });
			} catch (error) {
				if (error instanceof ActivityError) throw ApplicationFailure.create({ type: error.kind, nonRetryable: !error.retryable, message: error.message });
				throw error;
			} finally {
				context.cancellationSignal.removeEventListener("abort", relay);
				h.aborts.delete(abort);
			}
		};
	}
	return activities;
};

const controlActivities = (h: Harness) => {
	const real = createControlActivities(controlRepo, drizzle(controlClient));
	const SelectAgent: ControlActivities["SelectAgent"] = async (input) => {
		h.seen.push({ name: "SelectAgent", db: (await dbRow(h.taskId)).status, wf: null });
		if (!h.agentAvailable) throw ApplicationFailure.nonRetryable("no agent allowed by profile", "NoEligibleAgent");
		void input;
		return { chosen: { agentId: agent, workerId: MACHINE } } as Awaited<ReturnType<ControlActivities["SelectAgent"]>>;
	};
	const RequestReview = (async () => ({ kind: "reviewer", agentId: reviewer, provider: "reviewer-provider", existingReviewId: null })) as unknown as ControlActivities["RequestReview"];
	return { ...real, SelectAgent, RequestReview };
};

/** Runs `body` while a control worker and the machine worker of `machine-m1` poll; both stop in every case, hung calls are released. */
const withWorkers = async <T>(h: Harness, body: () => Promise<T>): Promise<T> => {
	const control = await Worker.create({
		connection: env.nativeConnection, taskQueue: CONTROL_QUEUE, workflowBundle: await controlBundle(),
		activities: controlActivities(h), enableNonLocalActivities: false,
		// No workflow cache and so no sticky queue: a worker that is gone leaves no sticky queue behind, and the time-skipping server
		// (whose timers do not run while the test only queries) would never route a query to the next worker.
		maxCachedWorkflows: 0,
	});
	const machine = await Worker.create({ connection: env.nativeConnection, taskQueue: h.fakes.PrepareWorkspace.queue!, activities: machineActivities(h) });
	try {
		return await control.runUntil(machine.runUntil(body()));
	} finally {
		for (const abort of h.aborts) abort.abort(new Error("test ended"));
	}
};

const startLifecycle = async (h: Harness): Promise<Handle> => {
	h.handle = await env.client.workflow.start("TaskWorkflow", { taskQueue: CONTROL_QUEUE, workflowId: taskWorkflowId(h.taskId), args: [{ taskId: h.taskId }] });
	return h.handle;
};

const reopen = (h: Harness): Handle => (h.handle = env.client.workflow.getHandle(taskWorkflowId(h.taskId)));

/** Polls the `state` query until `done` holds; a bounded count of yields, no clock. */
const until = async (handle: Handle, label: string, done: (state: TaskState) => boolean): Promise<TaskState> => {
	let last: TaskState | null = null;
	for (let round = 0; round < 4000; round += 1) {
		last = await handle.query(stateQuery);
		if (done(last)) return last;
		await new Promise<void>((resolve) => setImmediate(resolve));
	}
	assert.fail(`${label}: not reached, the last state was ${last?.status} (${JSON.stringify({ stopping: last?.stopping, pause: last?.pauseRequested, block: last?.blockKind })})`);
};

/** The implementer run and then the reviewer run of VERIFYING both end at once: a report, then a review that passes. */
const monitorExits = (h: Harness): void => {
	const exited = { outcome: { kind: "succeed", output: { state: "exited", logOffset: 0 } } } as const;
	h.fakes.MonitorExecution.script(exited, exited);
	h.fakes.CollectResults.script({ outcome: { kind: "succeed", output: collected() } }, { outcome: { kind: "succeed", output: reviewPass() } });
};

/** The workflow state and the tasks row agree on every column the projection carries. */
const assertRowFollows = async (taskId: string, state: TaskState): Promise<Row> => {
	const row = await dbRow(taskId);
	assert.equal(row.status, state.status, "tasks.status follows the workflow");
	assert.equal(row.pause_requested, state.pauseRequested);
	assert.equal(row.stopping, state.stopping);
	assert.equal(row.resume_to, state.resumeTo);
	assert.equal(row.assigned_agent_id, state.agentId);
	return row;
};

before(async () => {
	bench = await benchPromise;
	await migrate({ url: bench.migratorUrl });
	migratorClient = connectBench(bench.migratorUrl);
	migratorRepo = createRepo(drizzle(migratorClient));
	controlClient = connectBench(bench.controlUrl);
	controlRepo = createRepo(drizzle(controlClient));
	await migratorClient`INSERT INTO owners (id, name) VALUES (${owner}, 'active owner')`;
	// The review trigger demands another agent and another provider than the implementer's.
	await migratorClient`INSERT INTO agents (id, provider) VALUES (${agent}, 'implementer-provider'), (${reviewer}, 'reviewer-provider')`;
	await migratorRepo.projects.create({ id: project, name: "lifecycle-test", repositoryUrl: "https://example.invalid/repo.git", defaultBranch: "main", workspacePolicy: "worktree" });
	// Every timeout 7 s (below the 10 s workflow task timeout); the limits fail closed at 0, so they are set: 3 runs a round, 2 rounds.
	for (const key of allTimeoutKeys()) await migratorRepo.settings.set(key, 7, null);
	await migratorRepo.settings.set("attempt.max", 3, null);
	await migratorRepo.settings.set("rework.maxRounds", 2, null);
	env = await startTimeSkipping();
});

after(async () => {
	await env?.teardown();
	await controlClient?.end();
	await migratorClient?.end();
	await bench?.dispose();
});

describe("TaskWorkflow lifecycle", () => {
	test("happy path: CREATED to ACCEPTED, the tasks row follows every step and the machine calls come in order", async () => {
		const h = await harness();
		monitorExits(h);
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			const parked = await until(handle, "awaiting approval", (s) => s.status === "AWAITING_APPROVAL");
			const row = await assertRowFollows(h.taskId, parked);
			assert.equal(row.assigned_agent_id, agent);
			assert.ok(parked.executionId === null || typeof parked.executionId === "string");

			const approve = await seedDecision(h.taskId, "approve", { revisionNumber: 1 });
			assert.equal((await sendUpdate(handle as never, "approve", approve)).status, "ACCEPTED");
			const done = await until(handle, "accepted", (s) => s.status === "ACCEPTED");
			await assertRowFollows(h.taskId, done);
			assert.equal((await dbRow(h.taskId)).status, "ACCEPTED");
		});

		// The machine activities ran in the documented order, each when the tasks row (and the workflow) was in the state of its step.
		const expected: [string, string][] = [
			["PrepareWorkspace", "ASSIGNED"], ["StartAgentExecution", "ASSIGNED"], ["MonitorExecution", "RUNNING"], ["CollectResults", "RUNNING"],
			["UploadResults", "RUNNING"], ["RunAutomatedChecks", "VERIFYING"],
			// The review of VERIFYING is a second run (the reviewer's) on the same machine, collected as a typed review outcome.
			["StartAgentExecution", "VERIFYING"], ["MonitorExecution", "VERIFYING"], ["CollectResults", "VERIFYING"],
			["PublishArtifacts", "ACCEPTED"],
		];
		assert.deepEqual(h.seen.filter((s) => s.name !== "SelectAgent").map((s) => [s.name, s.db]), expected);
		for (const seen of h.seen) if (seen.wf !== null) assert.equal(seen.wf, seen.db, `${seen.name}: workflow and tasks row agree`);
		assert.deepEqual(h.seen.find((s) => s.name === "SelectAgent"), { name: "SelectAgent", db: "ASSIGNED", wf: null });

		const publish = h.fakes.PublishArtifacts.calls()[0] as { branch: string; target: string };
		assert.equal(publish.branch, `task/${h.taskId}`);
		assert.equal(publish.target, "main");
		for (const name of ["PrepareWorkspace", "UploadResults", "RunAutomatedChecks", "PublishArtifacts"] as const) {
			assert.equal(h.fakes[name].executed(), 1, `${name} ran once`);
		}
		for (const name of ["StartAgentExecution", "CollectResults"] as const) {
			assert.equal(h.fakes[name].executed(), 2, `${name} ran twice: the implementer and the reviewer`);
		}
	});

	test("no agent: the task stays ASSIGNED, the notice goes out once, and a wake after a worker restart selects again", async () => {
		const h = await harness();
		h.agentAvailable = false;
		monitorExits(h);
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			const waiting = await until(handle, "no agent wait", (s) => s.status === "ASSIGNED" && s.noAgentNoticeAttempt !== null);
			await assertRowFollows(h.taskId, waiting);
			const notices = await migratorClient`SELECT event_type, occurrences FROM notifications WHERE task_id = ${h.taskId}`;
			assert.deepEqual(notices.map((n) => [n.event_type, Number(n.occurrences)]), [["no_eligible_agent", 1]]);
			assert.equal(h.fakes.PrepareWorkspace.executed(), 0);
		});

		// The workers are gone: the workflow is replayed by new ones and is still waiting; the agent shows up and a wake ends the wait.
		h.agentAvailable = true;
		await withWorkers(h, async () => {
			const handle = reopen(h);
			const still = await handle.query(stateQuery);
			assert.equal(still.status, "ASSIGNED");
			await handle.signal(wakeSignal);
			const parked = await until(handle, "awaiting approval", (s) => s.status === "AWAITING_APPROVAL");
			await assertRowFollows(h.taskId, parked);
			const notices = await migratorClient`SELECT occurrences FROM notifications WHERE task_id = ${h.taskId}`;
			assert.equal(notices.length, 1, "the notice was not repeated");
		});
		assert.equal(h.fakes.PrepareWorkspace.executed(), 1);
	});

	test("no agent: allow_agent ends the wait; malformed change_priority and allow_agent are refused with invalid_payload", async () => {
		const h = await harness();
		h.agentAvailable = false;
		monitorExits(h);
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			await until(handle, "no agent wait", (s) => s.status === "ASSIGNED" && s.noAgentNoticeAttempt !== null);

			for (const [kind, payload] of [["change_priority", { priority: "high" }], ["change_priority", {}], ["allow_agent", {}], ["allow_agent", { agent_id: 7 }]] as const) {
				const outcome = await sendUpdate(handle as never, kind, await seedDecision(h.taskId, kind, { payload }));
				assert.deepEqual({ outcome: outcome.outcome, refusal: outcome.refusal }, { outcome: "refused", refusal: "invalid_payload" }, `${kind} ${JSON.stringify(payload)}`);
			}
			const unchanged = await handle.query(stateQuery);
			assert.deepEqual([unchanged.status, unchanged.allowedAgents.length, unchanged.reselectAgent], ["ASSIGNED", 0, false]);

			const priority = await sendUpdate(handle as never, "change_priority", await seedDecision(h.taskId, "change_priority", { payload: { priority: 5 } }));
			assert.equal(priority.outcome, "applied");

			h.agentAvailable = true;
			const allowed = await sendUpdate(handle as never, "allow_agent", await seedDecision(h.taskId, "allow_agent", { payload: { agent_id: agent } }));
			assert.equal(allowed.outcome, "applied");
			const parked = await until(handle, "awaiting approval", (s) => s.status === "AWAITING_APPROVAL");
			assert.deepEqual([...parked.allowedAgents], [agent]);
			await assertRowFollows(h.taskId, parked);
		});
	});

	test("cancel in RUNNING: the run is stopped, and the task becomes CANCELLED only after the machine confirms", async () => {
		const h = await harness();
		h.gates.set("MonitorExecution", gate());
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			await until(handle, "running", (s) => s.status === "RUNNING");
			await until(handle, "monitor started", () => h.seen.some((s) => s.name === "MonitorExecution"));
			const outcome = await sendUpdate(handle as never, "cancel", await seedDecision(h.taskId, "cancel"));
			assert.equal(outcome.outcome, "applied");
			assert.equal(outcome.status, "CANCELLING", "CANCELLING (temporal.md matrix) until the machine confirms the stop");
			const done = await until(handle, "cancelled", (s) => s.status === "CANCELLED");
			const row = await assertRowFollows(h.taskId, done);
			assert.equal(row.status, "CANCELLED");
		});
		assert.equal(h.fakes.StopExecution.executed(), 1);
		assert.equal(h.fakes.CollectResults.executed(), 0);
	});

	test("pause in RUNNING: the run goes on, and the task is PAUSED with resume_to when the run ends; resume continues", async () => {
		const h = await harness();
		monitorExits(h);
		const monitorGate = gate();
		h.gates.set("MonitorExecution", monitorGate);
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			await until(handle, "monitor started", () => h.seen.some((s) => s.name === "MonitorExecution"));
			const outcome = await sendUpdate(handle as never, "pause", await seedDecision(h.taskId, "pause"));
			assert.equal(outcome.outcome, "applied");
			const requested = await handle.query(stateQuery);
			assert.deepEqual([requested.status, requested.pauseRequested], ["RUNNING", true]);
			const requestedRow = await assertRowFollows(h.taskId, requested);
			assert.equal(requestedRow.pause_requested, true);

			monitorGate.open();
			const paused = await until(handle, "paused", (s) => s.status === "PAUSED");
			assert.equal(paused.resumeTo, "VERIFYING");
			assert.equal(paused.pauseRequested, false);
			const row = await assertRowFollows(h.taskId, paused);
			assert.equal(row.resume_to, "VERIFYING");

			const resumed = await sendUpdate(handle as never, "resume", await seedDecision(h.taskId, "resume"));
			assert.equal(resumed.outcome, "applied");
			const parked = await until(handle, "awaiting approval", (s) => s.status === "AWAITING_APPROVAL");
			await assertRowFollows(h.taskId, parked);
		});
		assert.equal(h.fakes.CollectResults.executed(), 2, "the implementer's run and the reviewer's");
		assert.equal(h.fakes.StopExecution.executed(), 0);
	});

	test("worker restart at the approval wait: the workflow is replayed, no machine activity runs twice, and the decision ends it", async () => {
		const h = await harness();
		monitorExits(h);
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			await until(handle, "awaiting approval", (s) => s.status === "AWAITING_APPROVAL");
		});
		const before = Object.fromEntries(MACHINE_ACTIVITY_NAMES.map((name) => [name, h.fakes[name].executed()]));
		await withWorkers(h, async () => {
			const handle = reopen(h);
			const replayed = await handle.query(stateQuery);
			assert.equal(replayed.status, "AWAITING_APPROVAL");
			await assertRowFollows(h.taskId, replayed);
			const approve = await seedDecision(h.taskId, "approve", { revisionNumber: 1 });
			assert.equal((await sendUpdate(handle as never, "approve", approve)).status, "ACCEPTED");
			const done = await until(handle, "accepted", (s) => s.status === "ACCEPTED");
			await assertRowFollows(h.taskId, done);
		});
		const after = Object.fromEntries(MACHINE_ACTIVITY_NAMES.map((name) => [name, h.fakes[name].executed()]));
		for (const name of MACHINE_ACTIVITY_NAMES) {
			assert.equal(after[name], name === "PublishArtifacts" ? 1 : before[name], `${name} after the restart`);
		}
	});
});
