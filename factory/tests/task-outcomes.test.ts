// TaskWorkflow run outcomes (temporal.md "Run outcomes", "Reviewer and findings"): every typed outcome of the machine run contract
// (contract 2) reaches its handler, and the DB rows (tasks, task_runs, task_events, notifications, task_reviews,
// review_finding_outcomes, task_proposals) show the handler's effect. Real Postgres bench, time-skipping Temporal, fake machine
// activities on queue `machine-m1` that return the scripted outcomes. No wall-clock sleeps: the usage-limit wait is crossed with
// the time-skipping clock (`env.sleep`).
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
import { stateQuery } from "../workflows/task.ts";
import type { TaskState } from "../workflows/task-state.ts";
import { connectBench, createDatabaseBench } from "./database-bench.ts";
import { ActivityError, CollectOutcome, MACHINE_ACTIVITY_NAMES } from "./fakes/contracts.ts";
import type { CollectOutcomeValue } from "./fakes/contracts.ts";
import { createMachineFakes, type MachineFakes } from "./fakes/machine-activities.ts";
import { collected, died, limited, questioned, reviewFail, reviewPass, settled, unclear } from "./fakes/outcomes.ts";
import { startTimeSkipping } from "./temporal/bench.ts";
import { controlBundle } from "./temporal/task-stub.ts";

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
const agent = ids(3);
const reviewer = ids(4);
const MACHINE = "m1";
let counter = 100;
const nextId = (): string => ids(counter++);

type Handle = WorkflowHandle;
type Row = { status: string; block_kind: string | null; block_handler: string | null; block_reason: string | null };

const dbRow = async (taskId: string): Promise<Row> => (await migratorClient<Row[]>`SELECT status, block_kind, block_handler, block_reason FROM tasks WHERE id = ${taskId}`)[0]!;

const newTask = async (): Promise<string> => {
	const id = nextId();
	await migratorRepo.tasks.create({ id, projectId: project, title: `outcomes-${id}`, priority: 1 }, { id: nextId(), requirements: "test", acceptanceCriteria: [], constraints: {} });
	return id;
};

/** What a test controls about one task: the scripted machine fakes. */
interface Harness {
	taskId: string;
	handle: Handle | null;
	fakes: MachineFakes;
	aborts: Set<AbortController>;
}

const harness = async (): Promise<Harness> => ({ taskId: await newTask(), handle: null, fakes: createMachineFakes(MACHINE), aborts: new Set() });

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

const controlActivities = () => {
	const real = createControlActivities(controlRepo, drizzle(controlClient));
	const SelectAgent: ControlActivities["SelectAgent"] = async () => ({ chosen: { agentId: agent, workerId: MACHINE } }) as Awaited<ReturnType<ControlActivities["SelectAgent"]>>;
	// The review trigger demands another agent and provider than the implementer's; the real router is covered by its own tests.
	const RequestReview = (async () => ({ kind: "reviewer", agentId: reviewer, provider: "reviewer-provider", existingReviewId: null })) as unknown as ControlActivities["RequestReview"];
	return { ...real, SelectAgent, RequestReview };
};

const withWorkers = async <T>(h: Harness, body: () => Promise<T>): Promise<T> => {
	const control = await Worker.create({
		connection: env.nativeConnection, taskQueue: CONTROL_QUEUE, workflowBundle: await controlBundle(),
		activities: controlActivities(), enableNonLocalActivities: false,
		// No workflow cache and so no sticky queue: see task-lifecycle.test.ts.
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

/** Polls the `state` query until `done` holds; a bounded count of yields, no clock. */
const until = async (handle: Handle, label: string, done: (state: TaskState) => boolean): Promise<TaskState> => {
	let last: TaskState | null = null;
	for (let round = 0; round < 20000; round += 1) {
		last = await handle.query(stateQuery);
		if (done(last)) return last;
		await new Promise<void>((resolve) => setImmediate(resolve));
	}
	assert.fail(`${label}: not reached, the last state was ${last?.status} (${JSON.stringify({ block: last?.blockKind })})`);
};

/** Scripts the end of `n` runs (implementer and reviewer runs alike): MonitorExecution reports `exited` n times. */
const monitorExits = (h: Harness, n: number): void => {
	const exited = { outcome: { kind: "succeed", output: { state: "exited", logOffset: 0 } } } as const;
	h.fakes.MonitorExecution.script(...Array.from({ length: n }, () => exited));
};

/** The outcomes CollectResults returns for the runs in order; the contract validates each. */
const collects = (h: Harness, ...outcomes: CollectOutcomeValue[]): void => {
	h.fakes.CollectResults.script(...outcomes.map((output) => ({ outcome: { kind: "succeed", output } }) as const));
};

const events = async (taskId: string): Promise<string[]> => {
	const rows = await migratorClient<{ event_type: string }[]>`SELECT event_type FROM task_events WHERE task_id = ${taskId} ORDER BY seq`;
	return rows.map((r) => r.event_type).filter((t) => t.startsWith("lifecycle_")).map((t) => t.slice("lifecycle_".length));
};

/** `expected` appears in `actual` in this order (other events may sit between). */
const assertInOrder = (actual: string[], expected: string[]): void => {
	let at = 0;
	for (const type of actual) if (type === expected[at]) at += 1;
	assert.equal(at, expected.length, `events ${JSON.stringify(actual)} hold ${JSON.stringify(expected)} in order`);
};

/** The task's runs as sorted "role:status" strings (an error code is appended when the run has one). */
const runs = async (taskId: string): Promise<string[]> => {
	const rows = await migratorClient<{ role: string; status: string; error_code: string | null }[]>`SELECT role, status, error_code FROM task_runs WHERE task_id = ${taskId}`;
	return rows.map((r) => `${r.role}:${r.status}${r.error_code === null ? "" : `:${r.error_code}`}`).sort();
};

const notices = async (taskId: string): Promise<{ event_type: string; dedup_key: string; state: string; occurrences: number }[]> =>
	[...(await migratorClient<{ event_type: string; dedup_key: string; state: string; occurrences: number }[]>`SELECT event_type, dedup_key, state, occurrences FROM notifications WHERE task_id = ${taskId} ORDER BY event_type, dedup_key`)];

const outcomeRows = async (taskId: string): Promise<string[]> => {
	const rows = await migratorClient<{ finding_key: string; outcome: string }[]>`SELECT finding_key, outcome FROM review_finding_outcomes WHERE review_id IN (SELECT id FROM task_reviews WHERE task_id = ${taskId})`;
	return rows.map((r) => `${r.finding_key}:${r.outcome}`).sort();
};

const defect = (key: string) => ({ key, kind: "defect" as const, text: `defect ${key}` });
const disputedTechnical = (key: string) => ({ key, answer: "disputed" as const, scope: "technical" as const, reason: "the finding is wrong" });
const accepted = (key: string) => ({ key, answer: "accepted" as const, scope: "technical" as const, reason: "fixed" });

before(async () => {
	bench = await benchPromise;
	await migrate({ url: bench.migratorUrl });
	migratorClient = connectBench(bench.migratorUrl);
	migratorRepo = createRepo(drizzle(migratorClient));
	controlClient = connectBench(bench.controlUrl);
	controlRepo = createRepo(drizzle(controlClient));
	await migratorClient`INSERT INTO owners (id, name) VALUES (${owner}, 'active owner')`;
	await migratorClient`INSERT INTO agents (id, provider) VALUES (${agent}, 'implementer-provider'), (${reviewer}, 'reviewer-provider')`;
	await migratorRepo.projects.create({ id: project, name: "outcomes-test", repositoryUrl: "https://example.invalid/repo.git", defaultBranch: "main", workspacePolicy: "worktree" });
	// Every timeout 7 s (below the 10 s workflow task timeout); the limits fail closed at 0, so they are set: 3 runs a round, 2 rework
	// rounds, a usage-limit wait up to 100 s, the question gate's search budget (the search is unavailable in the bench: the gate fails open).
	for (const key of allTimeoutKeys()) await migratorRepo.settings.set(key, 7, null);
	await migratorRepo.settings.set("attempt.max", 3, null);
	await migratorRepo.settings.set("rework.maxRounds", 2, null);
	await migratorRepo.settings.set("limit.maxWait", 100, null);
	await migratorRepo.settings.set("registry.gateTimeoutMs", 100, null);
	env = await startTimeSkipping();
});

after(async () => {
	await env?.teardown();
	await controlClient?.end();
	await migratorClient?.end();
	await bench?.dispose();
});

describe("typed run outcomes reach their handlers", () => {
	test("limit with a known reset within limit.maxWait: a usage_limit_wait notice, a durable wait, then a rerun", async () => {
		const h = await harness();
		monitorExits(h, 3);
		collects(h, limited(30), collected(), reviewPass());
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			// The wait is a timer of the workflow: poll the notice (queries do not advance time), then cross the reset.
			let waiting = false;
			for (let round = 0; round < 20000 && !waiting; round += 1) {
				waiting = (await notices(h.taskId)).some((n) => n.event_type === "usage_limit_wait");
				if (!waiting) await new Promise<void>((resolve) => setImmediate(resolve));
			}
			assert.ok(waiting, "the usage_limit_wait notice went out");
			assert.equal((await dbRow(h.taskId)).status, "RUNNING", "the task waits where the run stopped, it is not blocked");
			await env.sleep(31_000);
			await until(handle, "awaiting approval", (s) => s.status === "AWAITING_APPROVAL");
		});
		const wait = (await notices(h.taskId)).filter((n) => n.event_type === "usage_limit_wait");
		assert.equal(wait.length, 1);
		assert.equal(wait[0]!.dedup_key, `usage_limit_wait:${h.taskId}:implementer:1`);
		assert.equal((await dbRow(h.taskId)).status, "AWAITING_APPROVAL");
		assert.equal((await dbRow(h.taskId)).block_kind, null);
		assert.deepEqual(await runs(h.taskId), ["implementer:failed:usage_limit", "implementer:finished", "reviewer:finished"]);
		assertInOrder(await events(h.taskId), ["usage_limit_elapsed", "run_finished", "review_passed"]);
	});

	for (const [label, waitSeconds] of [["an unknown reset", null], ["a reset beyond limit.maxWait", 500]] as const) {
		test(`limit with ${label}: BLOCKED usage_limit for the chief engineer, no owner notice`, async () => {
			const h = await harness();
			monitorExits(h, 1);
			collects(h, limited(waitSeconds));
			await withWorkers(h, async () => {
				const handle = await startLifecycle(h);
				await until(handle, "blocked", (s) => s.status === "BLOCKED");
			});
			const row = await dbRow(h.taskId);
			assert.equal(row.status, "BLOCKED");
			assert.equal(row.block_kind, "usage_limit");
			assert.equal(row.block_handler, "chief_engineer");
			assert.deepEqual(await runs(h.taskId), ["implementer:failed:usage_limit"]);
			assertInOrder(await events(h.taskId), ["usage_limit"]);
			assert.deepEqual(await notices(h.taskId), [], "a chief-engineer block raises no owner notice and no wait notice");
		});
	}

	test("question: through the question gate to BLOCKED agent_question for the owner, one task_blocked notice", async () => {
		const h = await harness();
		monitorExits(h, 1);
		collects(h, questioned());
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			await until(handle, "blocked", (s) => s.status === "BLOCKED");
		});
		const row = await dbRow(h.taskId);
		assert.equal(row.block_kind, "agent_question");
		assert.equal(row.block_handler, "owner");
		const blocked = (await notices(h.taskId)).filter((n) => n.event_type === "task_blocked");
		assert.equal(blocked.length, 1);
		assert.equal(blocked[0]!.state, "open");
		assert.deepEqual(await runs(h.taskId), ["implementer:finished"]);
		assertInOrder(await events(h.taskId), ["agent_question"]);
	});

	test("died: the retry class starts a new attempt, the dead run is recorded as died", async () => {
		const h = await harness();
		monitorExits(h, 3);
		collects(h, died(), collected(), reviewPass());
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			await until(handle, "awaiting approval", (s) => s.status === "AWAITING_APPROVAL");
		});
		assert.equal((await dbRow(h.taskId)).status, "AWAITING_APPROVAL");
		assert.deepEqual(await runs(h.taskId), ["implementer:died", "implementer:finished", "reviewer:finished"]);
		assertInOrder(await events(h.taskId), ["run_lost", "run_finished", "review_passed"]);
	});

	test("died on every attempt of the round: BLOCKED execution for the chief engineer", async () => {
		const h = await harness();
		monitorExits(h, 3);
		collects(h, died(), died(), died());
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			await until(handle, "blocked", (s) => s.status === "BLOCKED");
		});
		const row = await dbRow(h.taskId);
		assert.equal(row.block_kind, "execution");
		assert.equal(row.block_handler, "chief_engineer");
		assert.deepEqual(await runs(h.taskId), ["implementer:died", "implementer:died", "implementer:died"]);
		assertInOrder(await events(h.taskId), ["run_lost", "run_lost", "attempts_exhausted"]);
	});

	test("unclear: BLOCKED execution at once, without a retry", async () => {
		const h = await harness();
		monitorExits(h, 1);
		collects(h, unclear());
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			await until(handle, "blocked", (s) => s.status === "BLOCKED");
		});
		const row = await dbRow(h.taskId);
		assert.equal(row.block_kind, "execution");
		assert.equal(row.block_handler, "chief_engineer");
		assert.deepEqual(await runs(h.taskId), ["implementer:unclear"]);
		assertInOrder(await events(h.taskId), ["run_unclear"]);
	});
});

describe("review findings and the settling reviewer", () => {
	test("a disputed technical defect is settled by one reviewer run; dismissed, the task goes on to a passing review; other findings become proposals", async () => {
		const h = await harness();
		monitorExits(h, 5);
		collects(h,
			collected(),
			reviewFail([defect("d1"), { key: "s1", kind: "suggestion", text: "rename the helper" }, { key: "r1", kind: "requirement_change", text: "the limit should be configurable" }]),
			collected({ findingAnswers: [disputedTechnical("d1")] }),
			settled([{ key: "d1", outcome: "dismissed", reason: "the code does what the requirement says" }]),
			reviewPass(),
		);
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			await until(handle, "awaiting approval", (s) => s.status === "AWAITING_APPROVAL");
		});
		assert.equal((await dbRow(h.taskId)).status, "AWAITING_APPROVAL");
		assert.deepEqual(await runs(h.taskId), ["implementer:finished", "implementer:finished", "reviewer:finished", "reviewer:finished", "reviewer:finished"]);

		const reviews = await migratorClient<{ verdict: string | null; settling: boolean }[]>`SELECT verdict, disputes_review_id IS NOT NULL AS settling FROM task_reviews WHERE task_id = ${h.taskId}`;
		assert.equal(reviews.length, 3, "the failing review, the settling review and the passing review");
		assert.equal(reviews.filter((r) => r.settling).length, 1, "exactly one review settles a dispute");
		assert.deepEqual(reviews.filter((r) => !r.settling).map((r) => r.verdict).sort(), ["fail", "pass"]);
		const outcomes = await outcomeRows(h.taskId);
		assert.ok(outcomes.includes("d1:disputed") && outcomes.includes("d1:dismissed"), `the answer and the verdict are recorded: ${JSON.stringify(outcomes)}`);
		const proposals = await migratorClient<{ kind: string; state: string }[]>`SELECT kind, state FROM task_proposals WHERE task_id = ${h.taskId}`;
		assert.deepEqual(proposals.map((p) => `${p.kind}:${p.state}`).sort(), ["requirement_change:open", "suggestion:open"]);
		assertInOrder(await events(h.taskId), ["review_started", "rework_required", "run_finished", "review_passed"]);
	});

	test("a confirmed settlement sends the task to REWORK_REQUIRED and the next round passes", async () => {
		const h = await harness();
		monitorExits(h, 6);
		collects(h,
			collected(),
			reviewFail([defect("d1")]),
			collected({ findingAnswers: [disputedTechnical("d1")] }),
			settled([{ key: "d1", outcome: "confirmed", reason: "the defect is real" }]),
			collected(),
			reviewPass(),
		);
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			await until(handle, "awaiting approval", (s) => s.status === "AWAITING_APPROVAL");
		});
		const outcomes = await outcomeRows(h.taskId);
		assert.ok(outcomes.includes("d1:disputed") && outcomes.includes("d1:confirmed"), JSON.stringify(outcomes));
		const kinds = await events(h.taskId);
		assert.equal(kinds.filter((t) => t === "rework_required").length, 2, "one rework for the failing review, one for the confirmed defect");
		assertInOrder(kinds, ["rework_required", "rework_required", "run_finished", "review_passed"]);
	});

	test("defects accepted every round: after rework.maxRounds the task is BLOCKED rework_limit for the chief engineer", async () => {
		const h = await harness();
		monitorExits(h, 6);
		collects(h,
			collected(), reviewFail([defect("d1")]),
			collected({ findingAnswers: [accepted("d1")] }), reviewFail([defect("d2")]),
			collected({ findingAnswers: [accepted("d2")] }), reviewFail([defect("d3")]),
		);
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			await until(handle, "blocked", (s) => s.status === "BLOCKED");
		});
		const row = await dbRow(h.taskId);
		assert.equal(row.block_kind, "rework_limit");
		assert.equal(row.block_handler, "chief_engineer");
		assert.deepEqual((await outcomeRows(h.taskId)).filter((o) => o.endsWith(":accepted")), ["d1:accepted", "d2:accepted"]);
		assertInOrder(await events(h.taskId), ["rework_required", "rework_required", "rework_limit"]);
	});

	test("a dispute about the requirements goes to BLOCKED scope for the owner, no settling run", async () => {
		const h = await harness();
		monitorExits(h, 3);
		collects(h,
			collected(),
			reviewFail([defect("d1")]),
			collected({ findingAnswers: [{ key: "d1", answer: "disputed", scope: "requirements", reason: "the requirement contradicts itself" }] }),
		);
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			await until(handle, "blocked", (s) => s.status === "BLOCKED");
		});
		const row = await dbRow(h.taskId);
		assert.equal(row.block_kind, "scope");
		assert.equal(row.block_handler, "owner");
		assert.ok((await outcomeRows(h.taskId)).includes("d1:owner"));
		assert.equal((await notices(h.taskId)).filter((n) => n.event_type === "task_blocked").length, 1);
		assert.deepEqual((await runs(h.taskId)).filter((r) => r.startsWith("reviewer:")), ["reviewer:finished"], "only the first review ran");
		assertInOrder(await events(h.taskId), ["rework_required", "finding_disputed"]);
	});
});

describe("the contract refuses an unknown outcome", () => {
	test("an outcome of an unknown status or with missing fields fails validation, also when scripted into the fake", async () => {
		const bogus = { status: "bogus", reason: "x" };
		assert.throws(() => CollectOutcome.parse(bogus));
		assert.throws(() => CollectOutcome.parse({ status: "question", question: "no ids" }));
		assert.throws(() => CollectOutcome.parse({ status: "review", verdict: "maybe", findings: [] }));
		const h = await harness();
		assert.throws(() => h.fakes.CollectResults.script({ outcome: { kind: "succeed", output: bogus } } as never));
	});
});
