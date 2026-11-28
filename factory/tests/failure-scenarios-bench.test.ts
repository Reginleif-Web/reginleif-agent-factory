// Server-side failure scenarios on the time-skipping bench (testing.md, "Failure scenarios": F1, F5, F6), package 61.
// The real Postgres bench and the real control activities run behind a TaskWorkflow; the machine side is fake activities on
// queue `machine-m1` plus a test-owned fake agent adapter that stands for the run on the local machine.
// A "killed" or "cut" worker is a worker whose only activity slot is held by a call that never heartbeats and never returns
// (a ghost): Temporal sees an activity that stopped answering, exactly as it would after the process died.
// Waits are bounded polls of the `state` query and `env.sleep` on the fake clock; no wall-clock sleeps. The fake clock only
// runs inside `env.sleep`, so no machine gate is held while it runs.
// Not covered here (worker side, outside factory/): killing the real process by PID, the runner stopping the agent at its
// time limit, results kept in the execution folder.
import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";
import { drizzle } from "drizzle-orm/postgres-js";
import { ApplicationFailure, Context } from "@temporalio/activity";
import { GracefulShutdownPeriodExpiredError, Worker } from "@temporalio/worker";
import type { TestWorkflowEnvironment } from "@temporalio/testing";
import type { WorkflowHandle } from "@temporalio/client";
import { recordDecision } from "../api/decisions.ts";
import { allTimeoutKeys } from "../control/activity-catalog.ts";
import { createControlActivities, type ControlActivities } from "../control/activities/index.ts";
import { createOutboxRelay, type RelayRepo } from "../control/outbox-relay.ts";
import { CONTROL_QUEUE, createWorkflowClient, taskWorkflowId } from "../control/workflow-client.ts";
import { migrate } from "../db/migrate.ts";
import { createRepo, type Repo } from "../db/repo/index.ts";
import { stateQuery } from "../workflows/task.ts";
import type { TaskState } from "../workflows/task-state.ts";
import { connectBench, createDatabaseBench } from "./database-bench.ts";
import { createFakeAgentAdapter, type FakeAgentAdapter } from "./fakes/agent-adapter.ts";
import { ActivityError, MACHINE_ACTIVITY_NAMES } from "./fakes/contracts.ts";
import { createMachineFakes, type MachineFakes } from "./fakes/machine-activities.ts";
import { startTimeSkipping } from "./temporal/bench.ts";
import { controlBundle, sendUpdate } from "./temporal/task-stub.ts";

const benchPromise = createDatabaseBench(import.meta.url);
let bench: Awaited<typeof benchPromise>;
let migratorClient: ReturnType<typeof connectBench>;
let controlClient: ReturnType<typeof connectBench>;
let apiClient: ReturnType<typeof connectBench>;
let migratorRepo: Repo;
let controlRepo: Repo;
let apiRepo: Repo;
let env: TestWorkflowEnvironment;

const ids = (n: number): string => `61000000-0000-7000-8000-${String(n).padStart(12, "0")}`;
const owner = ids(1);
const project = ids(2);
const agent = ids(3);
const MACHINE = "m1";
let counter = 100;
const nextId = (): string => ids(counter++);

type Handle = WorkflowHandle;

const dbStatus = async (taskId: string): Promise<string> => (await migratorClient<{ status: string }[]>`SELECT status FROM tasks WHERE id = ${taskId}`)[0]!.status;
const eventTypes = async (taskId: string): Promise<string[]> =>
	(await migratorClient<{ event_type: string }[]>`SELECT event_type FROM task_events WHERE task_id = ${taskId} ORDER BY seq`).map((e) => e.event_type);
const countOf = (types: readonly string[], type: string): number => types.filter((t) => t === type).length;
const decisionRows = (taskId: string) =>
	migratorClient<{ id: string; kind: string; delivery_state: string; refusal_reason: string | null }[]>`SELECT id, kind, delivery_state, refusal_reason FROM task_decisions WHERE task_id = ${taskId} ORDER BY recorded_at, id`;
const outboxCount = async (decisionId: string): Promise<number> =>
	Number((await migratorClient<{ n: string }[]>`SELECT count(*)::text AS n FROM outbox_events WHERE decision_id = ${decisionId}`)[0]!.n);

const newTask = async (): Promise<string> => {
	const id = nextId();
	await migratorRepo.tasks.create({ id, projectId: project, title: `failure-${id}`, priority: 1 }, { id: nextId(), requirements: "test", acceptanceCriteria: [], constraints: {} });
	return id;
};

const gate = (): { promise: Promise<void>; open: () => void } => {
	let open!: () => void;
	const promise = new Promise<void>((resolve) => { open = resolve; });
	return { promise, open };
};

/** What a test controls about one run: the agent adapter (the run on the local machine), the ghost call, gates, and what each call saw. */
interface Harness {
	taskId: string;
	handle: Handle | null;
	fakes: MachineFakes;
	adapter: FakeAgentAdapter;
	launchId: string | null;
	ghostTaken: boolean;
	ghostEntered: Promise<void>;
	ghostEnter: () => void;
	ghostReleased: Promise<void>;
	ghostRelease: () => void;
	gates: Map<string, { promise: Promise<void>; open: () => void }>;
	seen: { name: string; db: string }[];
	aborts: Set<AbortController>;
}

const harness = async (): Promise<Harness> => {
	const entered = gate();
	const released = gate();
	return {
		taskId: await newTask(), handle: null, fakes: createMachineFakes(MACHINE), adapter: createFakeAgentAdapter(), launchId: null,
		ghostTaken: false, ghostEntered: entered.promise, ghostEnter: entered.open, ghostReleased: released.promise, ghostRelease: released.open,
		gates: new Map(), seen: [], aborts: new Set(),
	};
};

interface MachineOptions {
	/** The first MonitorExecution call hangs without heartbeats and holds the worker's only slot. */
	readonly ghost: boolean;
	/** StartAgentExecution and MonitorExecution act on the fake agent adapter instead of the scripted fakes. */
	readonly adapter: boolean;
}

/** One call of a machine activity as the local worker would run it. A gate holds it; the abort of its signal releases it. */
const machineActivities = (h: Harness, options: MachineOptions): Record<string, (input: never) => Promise<unknown>> => {
	const activities: Record<string, (input: never) => Promise<unknown>> = {};
	for (const name of MACHINE_ACTIVITY_NAMES) {
		activities[name] = async (input: never) => {
			const context = Context.current();
			const abort = new AbortController();
			h.aborts.add(abort);
			const relay = (): void => abort.abort(context.cancellationSignal.reason);
			context.cancellationSignal.addEventListener("abort", relay, { once: true });
			try {
				h.seen.push({ name, db: await dbStatus(h.taskId) });
				if (options.ghost && name === "MonitorExecution" && !h.ghostTaken) {
					// The process that held this call is gone: the call never heartbeats and does not hear the cancellation.
					h.ghostTaken = true;
					h.ghostEnter();
					await h.ghostReleased;
					return { state: "running", logOffset: 0 };
				}
				const hold = h.gates.get(name);
				if (hold !== undefined) {
					await new Promise<void>((resolve, reject) => {
						void hold.promise.then(resolve);
						abort.signal.addEventListener("abort", () => reject(abort.signal.reason), { once: true });
					});
				}
				if (options.adapter && name === "StartAgentExecution") {
					await h.fakes.StartAgentExecution.run(input, { signal: abort.signal });
					h.launchId = await h.adapter.start((input as { executionId: string }).executionId);
					return { launchId: h.launchId };
				}
				if (options.adapter && name === "MonitorExecution") {
					const status = await h.adapter.getStatus((input as { executionId: string }).executionId);
					return { state: status.state === "starting" ? "running" : status.state, logOffset: status.outputBytes };
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

const controlActivities = (h: Harness): ControlActivities => {
	const real = createControlActivities(controlRepo, drizzle(controlClient));
	const SelectAgent = (async () => {
		h.seen.push({ name: "SelectAgent", db: await dbStatus(h.taskId) });
		return { chosen: { agentId: agent, workerId: MACHINE } };
	}) as unknown as ControlActivities["SelectAgent"];
	const RequestReview = (async () => ({ kind: "reviewer", agentId: agent, provider: "test", existingReviewId: null })) as unknown as ControlActivities["RequestReview"];
	return { ...real, SelectAgent, RequestReview };
};

/** The control worker (queue `control`, local activities only) runs `body`; leftover calls are aborted so the worker can shut down. */
const withControl = async <T>(h: Harness, body: () => Promise<T>): Promise<T> => {
	const worker = await Worker.create({
		connection: env.nativeConnection, taskQueue: CONTROL_QUEUE, workflowBundle: await controlBundle(), activities: controlActivities(h),
		enableNonLocalActivities: false, maxCachedWorkflows: 0,
	});
	try {
		return await worker.runUntil(body());
	} finally {
		h.ghostRelease();
		for (const abort of h.aborts) abort.abort(new Error("test ended"));
	}
};

/** How long a worker that is told to stop may take before it is terminated as a killed process would be (real ms, no fake clock). */
const KILL_AFTER_MS = 300;

/** A machine worker on queue `machine-m1`; with a ghost it has one activity slot, which the ghost holds. */
const createMachine = (h: Harness, options: MachineOptions): Promise<Worker> =>
	Worker.create({
		connection: env.nativeConnection, taskQueue: h.fakes.PrepareWorkspace.queue!, activities: machineActivities(h, options),
		...(options.ghost ? { maxConcurrentActivityTaskExecutions: 1, shutdownForceTime: KILL_AFTER_MS } : {}),
	});

const withMachine = async <T>(h: Harness, options: MachineOptions, body: () => Promise<T>): Promise<T> => {
	const worker = await createMachine(h, options);
	try {
		return await worker.runUntil(body());
	} finally {
		h.ghostRelease();
		for (const abort of h.aborts) abort.abort(new Error("test ended"));
	}
};

/**
 * A machine worker whose process can be killed: `die()` stops it without waiting for its call in flight, which never answers.
 * The core allows one worker per task queue, so a restarted worker is created only after `die()` has returned.
 */
const withDyingMachine = async <T>(h: Harness, body: (die: () => Promise<void>) => Promise<T>): Promise<T> => {
	const worker = await createMachine(h, { ghost: true, adapter: true });
	const running = worker.run().then(() => null, (error: unknown) => error);
	let dead = false;
	const die = async (): Promise<void> => {
		dead = true;
		worker.shutdown();
		const outcome = await running;
		assert.ok(outcome instanceof GracefulShutdownPeriodExpiredError, `the worker was terminated, not drained: ${String(outcome)}`);
	};
	try {
		return await body(die);
	} finally {
		// A killed worker's call in flight stays held until `withControl` ends the test: were it released, the dead worker's core,
		// which may outlive its shutdown, would be free to take the next call and never answer it.
		if (!dead) {
			h.ghostRelease();
			worker.shutdown();
			await running;
		}
		for (const abort of h.aborts) abort.abort(new Error("test ended"));
	}
};

const startLifecycle = async (h: Harness): Promise<Handle> => {
	h.handle = await env.client.workflow.start("TaskWorkflow", { taskQueue: CONTROL_QUEUE, workflowId: taskWorkflowId(h.taskId), args: [{ taskId: h.taskId }] });
	return h.handle;
};

const until = async (handle: Handle, label: string, done: (state: TaskState) => boolean): Promise<TaskState> => {
	let last: TaskState | undefined;
	for (let poll = 0; poll < 4000; poll += 1) {
		last = await handle.query(stateQuery);
		if (done(last)) return last;
		await new Promise((resolve) => setImmediate(resolve));
	}
	return assert.fail(`${label}: not reached, the last state was ${last?.status} (${JSON.stringify({ stopping: last?.stopping, pause: last?.pauseRequested, block: last?.blockKind })})`);
};

const monitorExits = (h: Harness): void => {
	h.fakes.MonitorExecution.script({ outcome: { kind: "succeed", output: { state: "exited", logOffset: 0 } } });
};

const relay = () => createOutboxRelay({ repo: controlRepo as unknown as RelayRepo, workflows: createWorkflowClient(env.client), owner: "failure-relay" });

/** The API path of one decision: recorded with its outbox row in one transaction, not yet delivered. */
const decide = async (taskId: string, key: string) => {
	const result = await recordDecision(apiRepo, { taskId, kind: "approve", channel: "api", body: { revision_number: 1, comment: "" }, idempotencyKey: key, actorId: owner });
	if (result.status !== 202) return assert.fail(`decision ${key} was not recorded: ${JSON.stringify(result.body)}`);
	return result;
};

before(async () => {
	bench = await benchPromise;
	await migrate({ url: bench.migratorUrl });
	migratorClient = connectBench(bench.migratorUrl);
	migratorRepo = createRepo(drizzle(migratorClient));
	controlClient = connectBench(bench.controlUrl);
	controlRepo = createRepo(drizzle(controlClient));
	apiClient = connectBench(bench.apiUrl);
	apiRepo = createRepo(drizzle(apiClient));
	await migratorClient`INSERT INTO owners (id, name) VALUES (${owner}, 'active owner')`;
	await migratorClient`INSERT INTO agents (id) VALUES (${agent})`;
	await migratorRepo.projects.create({ id: project, name: "failure scenarios", repositoryUrl: "https://example.invalid/repo.git", defaultBranch: "main", workspacePolicy: "worktree" });
	// Every timeout 7 s (the bench value: below the 10 s workflow task timeout), so one env.sleep passes any of them.
	for (const key of allTimeoutKeys()) await migratorRepo.settings.set(key, 7, null);
	await migratorRepo.settings.set("attempt.max", 3, null);
	// Without a cap the waiting call is retried for ever while the machine is cut (the default 0 means unlimited); three attempts end the wait.
	await migratorRepo.settings.set("retry.infra.maxAttempts", 3, null);
	await migratorRepo.settings.set("rework.maxRounds", 2, null);
	env = await startTimeSkipping();
});

after(async () => {
	await env?.teardown();
	controlClient?.end();
	migratorClient?.end();
	apiClient?.end();
	bench?.dispose();
});

describe("F6 duplicated decision", () => {
	test("the same key and the same Update id apply once; another key on the decided task is refused", async () => {
		const h = await harness();
		monitorExits(h);
		// Held, so the workflow is still alive in ACCEPTED while the same Update id is sent again.
		const publish = gate();
		h.gates.set("PublishArtifacts", publish);
		await withControl(h, () => withMachine(h, { ghost: false, adapter: false }, async () => {
			try {
				const handle = await startLifecycle(h);
				await until(handle, "awaiting approval", (s) => s.status === "AWAITING_APPROVAL");

				const first = await decide(h.taskId, "f6-key-accept-1");
				assert.equal(first.replayed, false);
				const pressed = await relay().pressOnce();
				assert.equal(pressed.rows.filter((r) => r.outcome === "failed").length, 0, JSON.stringify(pressed.rows));
				await until(handle, "accepted", (s) => s.status === "ACCEPTED");

				const afterFirst = await eventTypes(h.taskId);
				assert.equal(countOf(afterFirst, "decision_approve"), 1, afterFirst.join(","));

				// Same idempotency key: the API answers with the same decision, no second row, no second outbox row.
				const replay = await decide(h.taskId, "f6-key-accept-1");
				assert.equal(replay.replayed, true);
				assert.equal(replay.body.id, first.body.id);
				assert.equal((await decisionRows(h.taskId)).length, 1);
				assert.equal(await outboxCount(first.body.id), 1);

				// Same Update id again (the relay re-sending after a lost answer): the first outcome comes back, nothing is applied twice.
				const again = await sendUpdate(handle as never, "approve", first.body.id);
				assert.equal(again.outcome, "applied");
				assert.equal(countOf(await eventTypes(h.taskId), "decision_approve"), 1);

				// A different key on the decided task: recorded as its own row, then refused, never applied.
				const other = await decide(h.taskId, "f6-key-accept-2");
				assert.notEqual(other.body.id, first.body.id);
				await relay().pressOnce();
				const rows = await decisionRows(h.taskId);
				assert.equal(rows.length, 2);
				const refused = rows.find((r) => r.id === other.body.id)!;
				assert.equal(refused.delivery_state, "refused");
				assert.equal(refused.refusal_reason, "task_closed");
				assert.equal(rows.find((r) => r.id === first.body.id)!.delivery_state, "applied");

				const final = await eventTypes(h.taskId);
				assert.equal(countOf(final, "decision_approve"), 1, final.join(","));
				assert.equal(await dbStatus(h.taskId), "ACCEPTED");
			} finally {
				publish.open();
			}
		}));
	});
});

describe("F1 the local worker stops", () => {
	test("the run goes on; after the restart the same execution is collected once and the task goes to verification", async () => {
		const h = await harness();
		await withControl(h, async () => {
			const handle = await startLifecycle(h);
			let launchId: string | null = null;
			let executionId: string | null = null;
			await withDyingMachine(h, async (die) => {
				await h.ghostEntered;
				assert.equal(h.adapter.launches(), 1);
				launchId = h.launchId;
				assert.notEqual(launchId, null);
				executionId = (await handle.query(stateQuery)).executionId;
				assert.notEqual(executionId, null);
				// The agent finishes while the worker is dead; the worker's call never answers.
				h.adapter.setState(executionId!, "exited");
				await die();
			});

			// The heartbeat timeout passes (7 s) and the retried call waits in the queue (it would expire at about 15 s).
			await env.sleep(9_000);
			assert.equal((await handle.query(stateQuery)).status, "RUNNING");
			assert.equal(h.fakes.CollectResults.executed(), 0);

			// The worker is started again and takes the queued call.
			await withMachine(h, { ghost: false, adapter: true }, async () => {
				await until(handle, "verifying or later", (s) => s.status === "VERIFYING" || s.status === "AWAITING_APPROVAL");
				await until(handle, "awaiting approval", (s) => s.status === "AWAITING_APPROVAL");
			});

			assert.equal(h.adapter.launches(), 1, "no second start");
			assert.equal(h.launchId, launchId, "the run is the same one");
			assert.equal(h.fakes.StartAgentExecution.executed(), 1);
			assert.equal(h.fakes.CollectResults.executed(), 1);
			assert.equal(h.fakes.UploadResults.executed(), 1);
			const events = await eventTypes(h.taskId);
			assert.equal(countOf(events, "lifecycle_run_started"), 1, events.join(","));
			assert.equal(countOf(events, "lifecycle_run_finished"), 1, events.join(","));
			assert.equal(countOf(events, "lifecycle_run_lost"), 0, events.join(","));
			assert.ok(h.seen.some((c) => c.name === "RunAutomatedChecks" && c.db === "VERIFYING"), JSON.stringify(h.seen));
		});
	});
});

describe("F5 the worker is cut from the server", () => {
	test("a short cut: nothing new starts, a decision is shown as refused, the results are uploaded once after the reconnect", async () => {
		const h = await harness();
		await withControl(h, async () => {
			const handle = await startLifecycle(h);
			let executionId = "";
			// The cut: the worker no longer answers and no other worker serves the queue; the fake clock runs on.
			await withDyingMachine(h, async (die) => {
				await h.ghostEntered;
				executionId = (await handle.query(stateQuery)).executionId!;
				await die();
			});
			await env.sleep(10_000);
			assert.equal((await handle.query(stateQuery)).status, "RUNNING");
			assert.equal(h.adapter.launches(), 1, "no new start while cut");
			assert.equal(h.fakes.StartAgentExecution.executed(), 1);
			assert.equal(h.fakes.CollectResults.executed(), 0);
			assert.equal(h.fakes.UploadResults.executed(), 0);

			// A decision that conflicts with the state is recorded and shown as refused, never applied silently.
			const decision = await decide(h.taskId, "f5-key-accept-1");
			await relay().pressOnce();
			const row = (await decisionRows(h.taskId)).find((r) => r.id === decision.body.id)!;
			assert.equal(row.delivery_state, "refused");
			assert.notEqual(row.refusal_reason, null);
			assert.equal(await dbStatus(h.taskId), "RUNNING");
			assert.equal(countOf(await eventTypes(h.taskId), "decision_approve"), 0);

			// The reconnect: the agent has finished meanwhile; its results are collected and uploaded once.
			h.adapter.setState(executionId, "exited");
			await withMachine(h, { ghost: false, adapter: true }, async () => {
				await until(handle, "awaiting approval", (s) => s.status === "AWAITING_APPROVAL");
			});
			assert.equal(h.adapter.launches(), 1);
			assert.equal(h.fakes.StartAgentExecution.executed(), 1);
			assert.equal(h.fakes.CollectResults.executed(), 1);
			assert.equal(h.fakes.UploadResults.executed(), 1);
		});
	});

});
