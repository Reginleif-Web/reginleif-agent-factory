// TaskWorkflow off the main path (temporal.md "BLOCKED", "Retry classes and limits", "The repair loop", package 56): the checks
// state-exit, review-provider, check-not-run and role-rights on the real Postgres bench and time-skipping Temporal with fake
// machine activities. RequestReview is the REAL activity (it picks the reviewer from the agents table); only SelectAgent is
// stubbed. Waits are bounded polls of the `state` query (no wall-clock sleeps); gates are test-owned promises.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
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
import { checkForSubject } from "../rights/check.ts";
import { assignRole, roleForParticipant } from "../rights/roles.ts";
import { stateQuery } from "../workflows/task.ts";
import type { TaskState, UpdateKind } from "../workflows/task-state.ts";
import { connectBench, createDatabaseBench, jsonb } from "./database-bench.ts";
import { ActivityError, MACHINE_ACTIVITY_NAMES } from "./fakes/contracts.ts";
import { collected, reviewPass } from "./fakes/outcomes.ts";
import { createMachineFakes, type MachineFakes } from "./fakes/machine-activities.ts";
import { startTimeSkipping } from "./temporal/bench.ts";
import { controlBundle, sendUpdate } from "./temporal/task-stub.ts";

const benchPromise = createDatabaseBench(import.meta.url);
let bench: Awaited<typeof benchPromise>;
let migratorClient: ReturnType<typeof connectBench>;
let controlClient: ReturnType<typeof connectBench>;
let migratorRepo: Repo;
let controlRepo: Repo;
let env: TestWorkflowEnvironment;

const ids = (n: number): string => `29000000-0000-7000-8000-${String(n).padStart(12, "0")}`;
const owner = ids(1);
const project = ids(2);
/** The implementer's agent: provider provA. */
const agent = ids(3);
/** Same provider as the implementer: never the reviewer. */
const sameProvider = ids(4);
/** Other providers with the reviewer role: the pick is the lowest id, so the first one. */
const reviewerB = ids(5);
const reviewerC = ids(6);
const MACHINE = "m1";
let counter = 100;
const nextId = (): string => ids(counter++);

type Handle = WorkflowHandle;
type BlockRow = { status: string; resume_to: string | null; block_kind: string | null; block_handler: string | null };

const blockRow = async (taskId: string): Promise<BlockRow> => (await migratorClient<BlockRow[]>`SELECT status, resume_to, block_kind, block_handler FROM tasks WHERE id = ${taskId}`)[0]!;
/** The task's events in order, with the workflow's `lifecycle_` prefix of the event type removed. */
const events = async (taskId: string): Promise<{ event_type: string; payload: Record<string, unknown> }[]> =>
	(await migratorClient<{ event_type: string; payload: Record<string, unknown> }[]>`SELECT event_type, payload FROM task_events WHERE task_id = ${taskId} ORDER BY seq`)
		.map((e) => ({ event_type: e.event_type.replace(/^lifecycle_/, ""), payload: e.payload }));
const eventTypes = async (taskId: string): Promise<string[]> => (await events(taskId)).map((e) => e.event_type);

const newTask = async (): Promise<string> => {
	const id = nextId();
	await migratorRepo.tasks.create({ id, projectId: project, title: `offpath-${id}`, priority: 1 }, { id: nextId(), requirements: "test", acceptanceCriteria: [], constraints: {} });
	return id;
};

/** The limits are read once at workflow start: set them before the workflow starts. 0 is the fail-closed default. */
const limits = async (value: { attemptMax?: number; reworkMax?: number; autoFixMax?: number; infraMax?: number }): Promise<void> => {
	await migratorRepo.settings.set("attempt.max", value.attemptMax ?? 3, null);
	await migratorRepo.settings.set("rework.maxRounds", value.reworkMax ?? 2, null);
	await migratorRepo.settings.set("autoFix.max", value.autoFixMax ?? 0, null);
	await migratorRepo.settings.set("retry.infra.maxAttempts", value.infraMax ?? 0, null);
};

/** An owner decision (actor set, channel client). */
const seedDecision = async (taskId: string, kind: UpdateKind, extra: { revisionNumber?: number | null; payload?: unknown } = {}): Promise<string> => {
	const decisionId = nextId();
	const outboxId = nextId();
	await migratorRepo.decisions.record({
		decision: { id: decisionId, taskId, kind, actorId: owner, channel: "client", idempotencyKey: `key-${decisionId}`, requestHash: "h", deliveryState: "recorded", outboxId, revisionNumber: extra.revisionNumber ?? null, payload: extra.payload ?? {} },
		outbox: { id: outboxId, kind: "deliver_decision", taskId, decisionId, payload: { decision_id: decisionId }, state: "pending", attempts: 0 },
	});
	return decisionId;
};

/** The chief engineer's run of the task: an engineer decision (no actor) carries it (the DB checks `task_decisions_actor_or_engineer`). */
const chiefRun = async (taskId: string): Promise<string> => {
	const id = nextId();
	await migratorClient`INSERT INTO task_runs(id, task_id, role, agent_id, participant, status, execution_id) VALUES (${id}, ${taskId}, 'chief_engineer', ${agent}, 'chief', 'running', ${randomUUID()})`;
	return id;
};

const seedEngineerDecision = async (taskId: string, engineerRunId: string, kind: UpdateKind, extra: { revisionNumber?: number | null; payload?: unknown } = {}): Promise<string> => {
	const decisionId = nextId();
	const outboxId = nextId();
	await migratorRepo.decisions.record({
		decision: { id: decisionId, taskId, kind, actorId: null, engineerRunId, channel: "api", idempotencyKey: `key-${decisionId}`, requestHash: "h", deliveryState: "recorded", outboxId, revisionNumber: extra.revisionNumber ?? null, payload: extra.payload ?? {} },
		outbox: { id: outboxId, kind: "deliver_decision", taskId, decisionId, payload: { decision_id: decisionId }, state: "pending", attempts: 0 },
	});
	return decisionId;
};

interface Harness {
	taskId: string;
	handle: Handle | null;
	fakes: MachineFakes;
	gates: Map<string, { promise: Promise<void>; open: () => void }>;
	aborts: Set<AbortController>;
}

const gate = (): { promise: Promise<void>; open: () => void } => {
	let open!: () => void;
	const promise = new Promise<void>((resolve) => { open = resolve; });
	return { promise, open };
};

const harness = async (): Promise<Harness> => ({ taskId: await newTask(), handle: null, fakes: createMachineFakes(MACHINE), gates: new Map(), aborts: new Set() });

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

/** The real control activities, RequestReview included; only SelectAgent is stubbed (the router is not under test here). */
const controlActivities = () => {
	const real = createControlActivities(controlRepo, drizzle(controlClient));
	const SelectAgent: ControlActivities["SelectAgent"] = async () =>
		({ chosen: { agentId: agent, workerId: MACHINE } }) as Awaited<ReturnType<ControlActivities["SelectAgent"]>>;
	return { ...real, SelectAgent };
};

const withWorkers = async <T>(h: Harness, body: () => Promise<T>): Promise<T> => {
	const control = await Worker.create({
		connection: env.nativeConnection, taskQueue: CONTROL_QUEUE, workflowBundle: await controlBundle(),
		activities: controlActivities(), enableNonLocalActivities: false, maxCachedWorkflows: 0,
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

const until = async (handle: Handle, label: string, done: (state: TaskState) => boolean): Promise<TaskState> => {
	let last: TaskState | null = null;
	for (let round = 0; round < 4000; round += 1) {
		last = await handle.query(stateQuery);
		if (done(last)) return last;
		await new Promise<void>((resolve) => setImmediate(resolve));
	}
	assert.fail(`${label}: not reached, the last state was ${last?.status} (${JSON.stringify({ stopping: last?.stopping, pause: last?.pauseRequested, block: last?.blockKind, handler: last?.blockHandler })})`);
};

const monitorExits = (h: Harness, times = 1): void => {
	for (let i = 0; i < times; i += 1) h.fakes.MonitorExecution.script({ outcome: { kind: "succeed", output: { state: "exited", logOffset: 0 } } });
};

/**
 * A run the review approves: `priorRuns` implementer runs ended before it (rework rounds), then this run, then the reviewer's run.
 * Every run ends with one MonitorExecution exit and one collected report; the last CollectResults answer is the reviewer's pass.
 */
const approvedRun = (h: Harness, priorRuns = 0): void => {
	monitorExits(h, priorRuns + 2);
	for (let i = 0; i < priorRuns + 1; i += 1) h.fakes.CollectResults.script({ outcome: { kind: "succeed", output: collected() } });
	h.fakes.CollectResults.script({ outcome: { kind: "succeed", output: reviewPass() } });
};

const checksResult = (h: Harness, status: "passed" | "failed" | "not_run"): void =>
	h.fakes.RunAutomatedChecks.script({ outcome: { kind: "succeed", output: { checkId: "unit", status, exitCode: status === "not_run" ? null : status === "passed" ? 0 : 1, logRef: "log-1" } } });

const blocked = (kind: string) => (s: TaskState): boolean => s.status === "BLOCKED" && s.blockKind === kind;

before(async () => {
	bench = await benchPromise;
	await migrate({ url: bench.migratorUrl });
	migratorClient = connectBench(bench.migratorUrl);
	migratorRepo = createRepo(drizzle(migratorClient));
	controlClient = connectBench(bench.controlUrl);
	controlRepo = createRepo(drizzle(controlClient));
	await migratorClient`INSERT INTO owners (id, name) VALUES (${owner}, 'active owner')`;
	// provA implements; the reviewer is the lowest-id enabled agent with the reviewer role from another provider.
	const roles = (names: string[]): string => jsonb({ roles: names });
	await migratorClient`INSERT INTO agents (id, provider, capabilities, enabled) VALUES (${agent}, 'provA', ${roles(["implementer"])}::text::jsonb, true)`;
	await migratorClient`INSERT INTO agents (id, provider, capabilities, enabled) VALUES (${sameProvider}, 'provA', ${roles(["reviewer"])}::text::jsonb, true)`;
	await migratorClient`INSERT INTO agents (id, provider, capabilities, enabled) VALUES (${reviewerB}, 'provB', ${roles(["reviewer"])}::text::jsonb, true)`;
	await migratorClient`INSERT INTO agents (id, provider, capabilities, enabled) VALUES (${reviewerC}, 'provC', ${roles(["reviewer"])}::text::jsonb, true)`;
	await migratorRepo.projects.create({ id: project, name: "offpath-test", repositoryUrl: "https://example.invalid/repo.git", defaultBranch: "main", workspacePolicy: "worktree" });
	for (const key of allTimeoutKeys()) await migratorRepo.settings.set(key, 7, null);
	env = await startTimeSkipping();
});

after(async () => {
	await env?.teardown();
	await controlClient?.end();
	await migratorClient?.end();
	await bench?.dispose();
});

describe("check-not-run: the repair loop of mandatory checks (D26)", () => {
	test("autoFix.max 1: one repair round, then the second not_run blocks with check_environment and nothing reaches review", async () => {
		await limits({ autoFixMax: 1 });
		const h = await harness();
		monitorExits(h, 2);
		checksResult(h, "not_run");
		checksResult(h, "not_run");
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			const state = await until(handle, "check_environment block", blocked("check_environment"));
			assert.equal(state.blockHandler, "chief_engineer");
			const row = await blockRow(h.taskId);
			assert.deepEqual([row.status, row.block_kind, row.block_handler], ["BLOCKED", "check_environment", "chief_engineer"]);
		});
		assert.equal(h.fakes.RunAutomatedChecks.executed(), 2, "the checks ran again on the repaired attempt");
		const types = await eventTypes(h.taskId);
		assert.ok(types.indexOf("check_repair") >= 0 && types.indexOf("checks_not_run") > types.indexOf("check_repair"), `repair then block: ${types.join(",")}`);
		assert.ok(!types.includes("review_started"), "a task whose mandatory checks did not run is never reviewed");
	});

	test("autoFix.max 0 (the fail-closed default): the first not_run blocks at once, with no repair", async () => {
		await limits({ autoFixMax: 0 });
		const h = await harness();
		monitorExits(h);
		checksResult(h, "not_run");
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			await until(handle, "check_environment block", blocked("check_environment"));
		});
		assert.equal(h.fakes.RunAutomatedChecks.executed(), 1);
		const types = await eventTypes(h.taskId);
		assert.ok(types.includes("checks_not_run") && !types.includes("check_repair"), types.join(","));
	});
});

describe("review-provider: the reviewer is another provider (D20)", () => {
	test("the pick skips the implementer's provider and takes the first eligible agent of another provider", async () => {
		await limits({});
		const h = await harness();
		approvedRun(h);
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			await until(handle, "awaiting approval", (s) => s.status === "AWAITING_APPROVAL");
		});
		const started = (await events(h.taskId)).find((e) => e.event_type === "review_started");
		assert.ok(started, "review_started was written");
		assert.equal(started.payload.agentId, reviewerB, "the same-provider agent was skipped, the lowest id of another provider chosen");
		const runs = await migratorClient`SELECT role, agent_id FROM task_runs WHERE task_id = ${h.taskId}`;
		const implementer = runs.filter((r) => r.role === "implementer");
		assert.ok(implementer.length > 0 && implementer.every((r) => r.agent_id === agent), "the implementer runs belong to the routed agent");
		const reviewer = runs.filter((r) => r.role === "reviewer");
		assert.deepEqual(reviewer.map((r) => r.agent_id), [reviewerB], "the reviewer's run is recorded for the picked reviewer, not the implementer");
	});

	test("no reviewer of another provider: BLOCKED check_environment (review_unavailable); once one is enabled, retry reaches review", async () => {
		await limits({});
		const h = await harness();
		approvedRun(h);
		await migratorClient`UPDATE agents SET enabled = false WHERE id IN (${reviewerB}, ${reviewerC})`;
		try {
			await withWorkers(h, async () => {
				const handle = await startLifecycle(h);
				const state = await until(handle, "check_environment block", blocked("check_environment"));
				assert.deepEqual((await blockRow(h.taskId)).block_kind, "check_environment");
				assert.ok((await eventTypes(h.taskId)).includes("review_unavailable"));

				await migratorClient`UPDATE agents SET enabled = true WHERE id = ${reviewerC}`;
				const decision = await seedDecision(h.taskId, "resolve_block", { revisionNumber: state.revision, payload: { block_kind: "check_environment", choice: "retry" } });
				const outcome = await sendUpdate(handle as never, "resolve_block", decision);
				assert.deepEqual({ outcome: outcome.outcome, refusal: outcome.refusal }, { outcome: "applied", refusal: null });
				await until(handle, "awaiting approval", (s) => s.status === "AWAITING_APPROVAL");
			});
		} finally {
			await migratorClient`UPDATE agents SET enabled = true WHERE id IN (${reviewerB}, ${reviewerC})`;
		}
		const started = (await events(h.taskId)).find((e) => e.event_type === "review_started");
		assert.equal(started?.payload.agentId, reviewerC);
	});
});

describe("state-exit: BLOCKED kinds enter and leave through their exits", () => {
	test("execution block: the row shows kind and handler; call_owner hands it to the owner; the owner's retry runs again", async () => {
		await limits({ infraMax: 1 });
		const h = await harness();
		approvedRun(h);
		h.fakes.StartAgentExecution.script({ outcome: { kind: "fail", error: "HeartbeatTimeout" } });
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			const state = await until(handle, "execution block", blocked("execution"));
			assert.equal(state.blockHandler, "chief_engineer");
			assert.deepEqual(await blockRow(h.taskId), { status: "BLOCKED", resume_to: null, block_kind: "execution", block_handler: "chief_engineer" });

			// A choice the kind does not have is refused; a pure payload fault is not a decision.
			const bad = await seedDecision(h.taskId, "resolve_block", { revisionNumber: state.revision, payload: { block_kind: "execution", choice: "remove_dependency" } });
			assert.equal((await sendUpdate(handle as never, "resolve_block", bad)).refusal, "invalid_payload");

			const chief = await chiefRun(h.taskId);
			const call = await seedEngineerDecision(h.taskId, chief, "resolve_block", { revisionNumber: state.revision, payload: { block_kind: "execution", choice: "call_owner", reason: "the machine needs a human" } });
			const called = await sendUpdate(handle as never, "resolve_block", call);
			assert.deepEqual({ outcome: called.outcome, status: called.status }, { outcome: "applied", status: "BLOCKED" });
			assert.deepEqual(await blockRow(h.taskId), { status: "BLOCKED", resume_to: null, block_kind: "execution", block_handler: "owner" });

			const retry = await seedDecision(h.taskId, "resolve_block", { revisionNumber: state.revision, payload: { block_kind: "execution", choice: "retry" } });
			assert.equal((await sendUpdate(handle as never, "resolve_block", retry)).status, "ASSIGNED");
			await until(handle, "awaiting approval", (s) => s.status === "AWAITING_APPROVAL");
			assert.deepEqual((await blockRow(h.taskId)).block_kind, null);
		});
	});

	test("rework_limit: the round limit blocks the third failed review, and the owner raises the limit to leave", async () => {
		await limits({ reworkMax: 2 });
		const h = await harness();
		approvedRun(h, 3);
		for (let i = 0; i < 3; i += 1) checksResult(h, "failed");
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			const state = await until(handle, "rework_limit block", blocked("rework_limit"));
			assert.equal(state.rework, 2);
			assert.deepEqual(await blockRow(h.taskId), { status: "BLOCKED", resume_to: null, block_kind: "rework_limit", block_handler: "chief_engineer" });
			assert.equal(h.fakes.RunAutomatedChecks.executed(), 3);

			const raise = await seedDecision(h.taskId, "resolve_block", { revisionNumber: state.revision, payload: { block_kind: "rework_limit", choice: "raise_limit" } });
			assert.equal((await sendUpdate(handle as never, "resolve_block", raise)).status, "REWORK_REQUIRED");
			await until(handle, "awaiting approval", (s) => s.status === "AWAITING_APPROVAL");
		});
		const types = await eventTypes(h.taskId);
		assert.equal(types.filter((t) => t === "rework_required").length, 2);
		assert.ok(types.includes("rework_limit"));
	});
});

describe("block at the end of a run while a pause is requested", () => {
	test("the block goes to PAUSED (resume_to BLOCKED, no block columns in the row); resume restores the block", async () => {
		await limits({ attemptMax: 1, infraMax: 1 });
		const h = await harness();
		h.gates.set("MonitorExecution", gate());
		h.fakes.MonitorExecution.script({ outcome: { kind: "fail", error: "HeartbeatTimeout" } });
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			await until(handle, "running", (s) => s.status === "RUNNING" && s.executionId !== null);
			const pause = await seedDecision(h.taskId, "pause");
			assert.deepEqual((await sendUpdate(handle as never, "pause", pause)).status, "RUNNING");
			assert.equal((await handle.query(stateQuery)).pauseRequested, true);
			h.gates.get("MonitorExecution")!.open();

			const paused = await until(handle, "paused", (s) => s.status === "PAUSED");
			assert.equal(paused.resumeTo, "BLOCKED");
			assert.equal(paused.pauseRequested, false, "the request is consumed, not left set");
			assert.equal(paused.blockKind, "execution", "the block lives in the workflow memory");
			assert.deepEqual(await blockRow(h.taskId), { status: "PAUSED", resume_to: "BLOCKED", block_kind: null, block_handler: null });

			const resume = await seedDecision(h.taskId, "resume");
			assert.equal((await sendUpdate(handle as never, "resume", resume)).status, "BLOCKED");
			assert.deepEqual(await blockRow(h.taskId), { status: "BLOCKED", resume_to: null, block_kind: "execution", block_handler: "chief_engineer" });
		});
	});
});

describe("role-rights: the workflow gives runs no role; only the chief engineer's assignment does", () => {
	test("the recorded runs hold no role, and a role right reaches the assigned participant only", async () => {
		await limits({});
		const h = await harness();
		approvedRun(h);
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			await until(handle, "awaiting approval", (s) => s.status === "AWAITING_APPROVAL");
		});
		const repo = controlRepo;
		const runs = await migratorClient`SELECT role, participant FROM task_runs WHERE task_id = ${h.taskId}`;
		assert.ok(runs.length > 0 && runs.every((r) => (r.role === "implementer" || r.role === "reviewer") && r.participant === null));
		assert.equal((await migratorClient`SELECT 1 FROM role_assignments WHERE task_id = ${h.taskId}`).length, 0, "the workflow assigns no role");
		assert.equal(await repo.transaction((tx) => roleForParticipant(tx, h.taskId, "builder")), undefined);

		const right = { action: "POST /devices", target: "work", conditions: {} };
		const regulation = randomUUID();
		const role = randomUUID();
		await migratorClient`INSERT INTO regulation_versions(id,number,status,clauses,clauses_bytes) VALUES (${regulation},1,'draft','[]',0)`;
		await migratorClient`INSERT INTO roles(id,name,version,status,rights,deny,regulation_version_id) VALUES (${role},'offpath-role',1,'draft',${jsonb([right])}::text::jsonb,'[]',${regulation})`;
		await migratorClient`UPDATE regulation_versions SET status='active' WHERE id=${regulation}`;
		// A chief engineer run needs a request it handles (or a block it handles): the DB trigger `af_0007_run_guard`.
		await migratorClient`INSERT INTO right_requests(id,task_id,handling_task_id,action,target) VALUES (${randomUUID()}, ${h.taskId}, ${h.taskId}, 'read', 'work')`;
		const chief = await chiefRun(h.taskId);
		const subject = { projectId: project, taskId: h.taskId, participant: "builder", agentId: agent, sessionRef: null };
		const check = (call: typeof right, participant = "builder") => repo.transaction((tx) => checkForSubject(tx, { ...subject, participant }, call, new Date(0)));
		assert.equal((await check(right)).allowed, false, "no assignment, no role right");

		// An implementer run cannot assign a role: the engineer run must be the chief engineer's.
		const implementerRun = (await migratorClient`SELECT id FROM task_runs WHERE task_id = ${h.taskId} AND role = 'implementer' LIMIT 1`)[0]!.id as string;
		await assert.rejects(migratorRepo.transaction((tx) => assignRole(tx, { taskId: h.taskId, participant: "builder", roleId: role, engineerRunId: implementerRun, participantRunId: randomUUID() })));

		// The assignment is the API's write (control has no INSERT on role_assignments); the migrator stands in for it here.
		await migratorRepo.transaction((tx) => assignRole(tx, { taskId: h.taskId, participant: "builder", roleId: role, engineerRunId: chief, participantRunId: randomUUID() }));
		assert.equal((await check(right)).source, "role");
		assert.equal((await check({ ...right, target: "other" })).allowed, false);
		assert.equal((await check(right, "other")).allowed, false);
	});
});
