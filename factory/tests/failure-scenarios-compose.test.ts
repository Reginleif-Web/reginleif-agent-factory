// Server-side failure scenarios that need real containers (testing.md, "Failure scenarios": F3, F4, F14), package 61.
// The stack is the production compose stack (Postgres, the schema step, the Temporal server), brought up the way
// tests/machine-liveness-schedule.test.ts does it, with its own project name. The control activities are the real ones on the
// real database; the machine side is fake activities on queue `machine-m1`. Everything runs in real time: a restart is a
// `docker stop` / `docker start` of the real container, and every wait is a bounded poll of a real condition (a health
// check, a row, a workflow state), never a fixed sleep.
// Not covered here (worker side, outside factory/): killing the real process by PID, the runner stopping the agent at its
// time limit, results kept in the execution folder.
import assert from "node:assert/strict";
import { randomBytes, randomUUID, createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { after, before, describe, it } from "node:test";
import { drizzle } from "drizzle-orm/postgres-js";
import { ApplicationFailure, Context } from "@temporalio/activity";
import { Client, Connection, type WorkflowHandle } from "@temporalio/client";
import { NativeConnection, Worker } from "@temporalio/worker";
import { loadApiConfig, startApi } from "../api/main.ts";
import { recordDecision } from "../api/decisions.ts";
import { allTimeoutKeys } from "../control/activity-catalog.ts";
import { createControlActivities, type ControlActivities } from "../control/activities/index.ts";
import { createOutboxRelay, type RelayRepo } from "../control/outbox-relay.ts";
import { CONTROL_QUEUE, createWorkflowClient, taskWorkflowId } from "../control/workflow-client.ts";
import { migrate } from "../db/migrate.ts";
import { createRepo, type Repo } from "../db/repo/index.ts";
import { freePort, realClock, runChecked, stackArgs, stackEnv, waitHealthy, findContainer } from "../deploy/lib.ts";
import { stateQuery } from "../workflows/task.ts";
import type { TaskState } from "../workflows/task-state.ts";
import { connectBench } from "./database-bench.ts";
import { ActivityError, MACHINE_ACTIVITY_NAMES } from "./fakes/contracts.ts";
import { createMachineFakes, type MachineFakes } from "./fakes/machine-activities.ts";
import { Cleanups, newBenchStackInfo, type SharedStackInfo } from "./helpers.ts";
import { controlBundle } from "./temporal/task-stub.ts";

const NAMESPACE = "af-failure-scenarios-test";
// 1 day: the shortest retention the server accepts for a namespace (same as tests/machine-liveness-schedule.test.ts).
const RETENTION_SECONDS = 24 * 60 * 60;
// The stack start is about 60 s on this machine, four times that for a loaded one (same bound as the other compose tests).
const START_MS = 240_000;
// One restart of the Temporal server or of Postgres, and the workflow's way back: the server is healthy again after about
// 60 s, the rest is polling; same factor four.
const RESTART_MS = 240_000;
const TEST_MS = 600_000;
// Every activity timeout of the product, in seconds. Real time, so no timeout may end a wait the test is making on purpose.
const TIMEOUT_SECONDS = 600;

const ids = (n: number): string => `62000000-0000-7000-8000-${String(n).padStart(12, "0")}`;
const owner = ids(1);
const project = ids(2);
const agent = ids(3);
const MACHINE = "m1";
let counter = 100;
const nextId = (): string => ids(counter++);

type Handle = WorkflowHandle;

describe("failure scenarios on the real stack", { concurrency: false }, () => {
	const cleanups = new Cleanups();
	let info: SharedStackInfo;
	let temporalContainer = "";
	let postgresContainer = "";
	let connection: Connection;
	let nativeConnection: NativeConnection;
	let client: Client;
	let migratorClient: ReturnType<typeof connectBench>;
	let controlClient: ReturnType<typeof connectBench>;
	let apiClient: ReturnType<typeof connectBench>;
	let migratorRepo: Repo;
	let controlRepo: Repo;
	let apiRepo: Repo;
	let bearer = "";

	// Polls `check` until it returns without throwing; rethrows the last error at the deadline (conditions, not fixed waits).
	const eventually = async <T>(check: () => Promise<T>, timeoutMs = 60_000): Promise<T> => {
		const deadline = realClock.now() + timeoutMs;
		for (;;) {
			try {
				return await check();
			} catch (error) {
				if (realClock.now() >= deadline) throw error;
				await realClock.sleep(300);
			}
		}
	};

	const urlFor = async (role: string): Promise<string> => {
		const password = (await readFile(join(info.secretsDir, role), "utf8")).trim();
		return `postgres://${encodeURIComponent(role)}:${encodeURIComponent(password)}@127.0.0.1:${info.port}/agent_factory`;
	};

	// Reads go through `eventually`: a postgres.js connection that the restart cut fails its first query and reconnects on the next.
	const dbStatus = (taskId: string): Promise<string> => eventually(async () => (await migratorClient<{ status: string }[]>`SELECT status FROM tasks WHERE id = ${taskId}`)[0]!.status);
	const eventRows = (taskId: string) =>
		eventually(() => migratorClient<{ seq: string; event_type: string; dedupe_key: string | null }[]>`SELECT seq::text AS seq, event_type, dedupe_key FROM task_events WHERE task_id = ${taskId} ORDER BY seq`);
	const eventTypes = async (taskId: string): Promise<string[]> => (await eventRows(taskId)).map((e) => e.event_type);
	const countOf = (types: readonly string[], type: string): number => types.filter((t) => t === type).length;
	const decisionRows = (taskId: string) =>
		eventually(() => migratorClient<{ id: string; kind: string; delivery_state: string; refusal_reason: string | null; idempotency_key: string }[]>`SELECT id, kind, delivery_state, refusal_reason, idempotency_key FROM task_decisions WHERE task_id = ${taskId} ORDER BY recorded_at, id`);
	const outboxCount = (decisionId: string): Promise<number> =>
		eventually(async () => Number((await migratorClient<{ n: string }[]>`SELECT count(*)::text AS n FROM outbox_events WHERE decision_id = ${decisionId}`)[0]!.n));

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

	/** What a test controls about one task: the fake machine side, gates, and what each machine call saw. */
	interface Harness {
		taskId: string;
		handle: Handle | null;
		fakes: MachineFakes;
		gates: Map<string, { promise: Promise<void>; open: () => void }>;
		seen: { name: string; db: string }[];
		aborts: Set<AbortController>;
	}

	const harness = async (): Promise<Harness> => ({ taskId: await newTask(), handle: null, fakes: createMachineFakes(MACHINE), gates: new Map(), seen: [], aborts: new Set() });

	/** One call of a machine activity as the local worker would run it. A gate holds it; the abort of its signal releases it. */
	const machineActivities = (h: Harness): Record<string, (input: never) => Promise<unknown>> => {
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

	const controlActivities = (h: Harness): ControlActivities => {
		const real = createControlActivities(controlRepo, drizzle(controlClient));
		const SelectAgent = (async () => {
			h.seen.push({ name: "SelectAgent", db: await dbStatus(h.taskId) });
			return { chosen: { agentId: agent, workerId: MACHINE } };
		}) as unknown as ControlActivities["SelectAgent"];
		const RequestReview = (async () => ({ kind: "reviewer", agentId: agent, provider: "test", existingReviewId: null })) as unknown as ControlActivities["RequestReview"];
		return { ...real, SelectAgent, RequestReview };
	};

	/** The control worker (queue `control`, local activities only) and the machine worker (queue `machine-m1`) run `body`; they stop when it ends. */
	const withWorkers = async <T>(h: Harness, body: () => Promise<T>): Promise<T> => {
		const control = await Worker.create({
			connection: nativeConnection, namespace: NAMESPACE, taskQueue: CONTROL_QUEUE, workflowBundle: await controlBundle(), activities: controlActivities(h),
			enableNonLocalActivities: false, maxCachedWorkflows: 0,
		});
		const machine = await Worker.create({ connection: nativeConnection, namespace: NAMESPACE, taskQueue: h.fakes.PrepareWorkspace.queue!, activities: machineActivities(h) });
		try {
			return await control.runUntil(machine.runUntil(body()));
		} finally {
			for (const gateEntry of h.gates.values()) gateEntry.open();
			for (const abort of h.aborts) abort.abort(new Error("test ended"));
		}
	};

	const startLifecycle = async (h: Harness): Promise<Handle> => {
		h.handle = await client.workflow.start("TaskWorkflow", { taskQueue: CONTROL_QUEUE, workflowId: taskWorkflowId(h.taskId), args: [{ taskId: h.taskId }] });
		return h.handle;
	};

	/** Polls the `state` query until `done` holds; a query that fails (the server is not there yet) is a poll that has not succeeded. */
	const until = async (handle: Handle, label: string, done: (state: TaskState) => boolean, timeoutMs = RESTART_MS): Promise<TaskState> => {
		const deadline = realClock.now() + timeoutMs;
		let last: TaskState | undefined;
		let failure: unknown;
		while (realClock.now() < deadline) {
			try {
				last = await handle.query(stateQuery);
				if (done(last)) return last;
			} catch (error) {
				failure = error;
			}
			await realClock.sleep(200);
		}
		return assert.fail(`${label}: not reached, the last state was ${last?.status}${failure === undefined ? "" : `, the last query error was ${String(failure)}`}`);
	};

	const seenCall = (h: Harness, name: string): Promise<void> =>
		eventually(async () => { assert.ok(h.seen.some((call) => call.name === name), `${name} has not been called yet`); }, RESTART_MS);

	const monitorExits = (h: Harness): void => {
		h.fakes.MonitorExecution.script({ outcome: { kind: "succeed", output: { state: "exited", logOffset: 0 } } });
	};

	const relay = () => createOutboxRelay({ repo: controlRepo as unknown as RelayRepo, workflows: createWorkflowClient(client), owner: "failure-relay" });

	/** Presses the outbox until the decision is applied: the delivery to Temporal is retried by the relay, as it is in production. */
	const delivered = (decisionId: string) =>
		eventually(async () => {
			await relay().pressOnce();
			const row = (await migratorClient<{ delivery_state: string }[]>`SELECT delivery_state FROM task_decisions WHERE id = ${decisionId}`)[0];
			assert.equal(row?.delivery_state, "applied", `decision ${decisionId} is not applied yet`);
		}, RESTART_MS);

	const record = async (taskId: string, kind: "approve" | "add_feedback", key: string) => {
		const result = await recordDecision(apiRepo, {
			taskId, kind, channel: "api", body: kind === "approve" ? { revision_number: 1, comment: "" } : { text: "note" }, idempotencyKey: key, actorId: owner,
		});
		if (result.status !== 202) return assert.fail(`decision ${key} was not recorded: ${JSON.stringify(result.body)}`);
		return result;
	};

	const docker = (...args: string[]): Promise<string> => runChecked("docker", args);
	const restartPostgresAndTemporal = async (): Promise<void> => {
		await docker("start", postgresContainer);
		await waitHealthy(postgresContainer, RESTART_MS);
		await docker("start", temporalContainer);
		await waitHealthy(temporalContainer, RESTART_MS);
	};

	before(async () => {
		info = await newBenchStackInfo(cleanups);
		const port = await freePort();
		const address = `127.0.0.1:${port}`;
		const env = { ...stackEnv(info), AF_TEMPORAL_PORT: String(port) };
		try {
			await cleanups.track(runChecked("docker", stackArgs(info, "up", "-d", "temporal"), { env }));
		} catch (error) {
			throw new Error(`the Temporal stack did not start: ${(error as Error).message}`);
		}
		const server = (await runChecked("docker", ["ps", "-q", "--filter", `label=com.docker.compose.project=${info.project}`, "--filter", "label=com.docker.compose.service=temporal"])).split(/\s+/).filter(Boolean);
		assert.equal(server.length, 1, "one temporal container expected");
		temporalContainer = server[0] as string;
		postgresContainer = await findContainer(info.project);
		await waitHealthy(temporalContainer, START_MS);
		await waitHealthy(postgresContainer, START_MS);
		connection = await Connection.connect({ address });
		cleanups.add(() => connection.close());
		await eventually(async () => {
			await connection.workflowService.registerNamespace({ namespace: NAMESPACE, workflowExecutionRetentionPeriod: { seconds: RETENTION_SECONDS as never } });
		});
		await eventually(async () => {
			await connection.workflowService.describeNamespace({ namespace: NAMESPACE });
		});
		client = new Client({ connection, namespace: NAMESPACE });
		nativeConnection = await NativeConnection.connect({ address });
		cleanups.add(() => nativeConnection.close());

		await migrate({ url: await urlFor("af_migrator") });
		migratorClient = connectBench(await urlFor("af_migrator"));
		migratorRepo = createRepo(drizzle(migratorClient));
		controlClient = connectBench(await urlFor("af_control"));
		controlRepo = createRepo(drizzle(controlClient));
		apiClient = connectBench(await urlFor("af_api"));
		apiRepo = createRepo(drizzle(apiClient));
		await migratorClient`INSERT INTO owners (id, name) VALUES (${owner}, 'active owner')`;
		await migratorClient`INSERT INTO agents (id) VALUES (${agent})`;
		await migratorRepo.projects.create({ id: project, name: "failure scenarios", repositoryUrl: "https://example.invalid/repo.git", defaultBranch: "main", workspacePolicy: "worktree" });
		for (const key of allTimeoutKeys()) await migratorRepo.settings.set(key, TIMEOUT_SECONDS, null);
		await migratorRepo.settings.set("attempt.max", 3, null);
		await migratorRepo.settings.set("retry.infra.maxAttempts", 3, null);
		await migratorRepo.settings.set("rework.maxRounds", 2, null);
		// A delivery that fails because the server is away is retried by the relay; a short back-off keeps the polls of the tests short.
		await migratorRepo.settings.set("relay.retrySeconds", 1, null);
		await migratorRepo.settings.set("relay.retryMaxSeconds", 2, null);
		await migratorRepo.settings.set("relay.maxAttempts", 200, null);
		// The token of the HTTP path (F4): created with the migrator while the database is up.
		const prefix = randomBytes(6).toString("hex");
		const secret = randomBytes(24).toString("base64url");
		await migratorRepo.tokens.create({ id: randomUUID(), name: "owner", role: "owner", prefix, tokenHash: createHash("sha256").update(secret).digest("hex"), ownerId: owner });
		bearer = `Bearer afk_${prefix}_${secret}`;
	}, { timeout: START_MS * 2 });

	after(async () => {
		await Promise.allSettled([controlClient?.end(), migratorClient?.end(), apiClient?.end()]);
		await cleanups.runAll();
	});

	it("F3 Temporal restarts while a task runs: the outbox decision is applied once, the workflow continues from persistence", { timeout: TEST_MS }, async () => {
		const h = await harness();
		monitorExits(h);
		// Holds the task in RUNNING (the agent is "working") across the restart.
		const working = gate();
		h.gates.set("MonitorExecution", working);
		await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			await seenCall(h, "MonitorExecution");
			assert.equal(await dbStatus(h.taskId), "RUNNING");
			assert.equal(h.fakes.StartAgentExecution.executed(), 1);

			// One decision is in the outbox, not delivered; the server goes away and comes back.
			const first = await record(h.taskId, "add_feedback", "f3-key-feedback-1");
			assert.equal(first.replayed, false);
			await docker("stop", temporalContainer);
			await docker("start", temporalContainer);
			await waitHealthy(temporalContainer, RESTART_MS);

			await delivered(first.body.id);
			const types = await eventTypes(h.taskId);
			assert.equal(countOf(types, "decision_add_feedback"), 1, types.join(","));
			assert.equal((await decisionRows(h.taskId)).filter((row) => row.delivery_state === "applied").length, 1);
			assert.equal(await outboxCount(first.body.id), 1);
			assert.equal(h.fakes.StartAgentExecution.executed(), 1, "no external effect was repeated");
			assert.equal(await dbStatus(h.taskId), "RUNNING");
			assert.equal((await until(handle, "running", (s) => s.status === "RUNNING")).status, "RUNNING");

			// A second press finds nothing left to deliver and applies nothing again.
			await relay().pressOnce();
			assert.equal(countOf(await eventTypes(h.taskId), "decision_add_feedback"), 1);

			// The workflow itself survived the restart: it goes on when the agent is done.
			working.open();
			await until(handle, "awaiting approval", (s) => s.status === "AWAITING_APPROVAL");
			assert.equal(h.fakes.StartAgentExecution.executed(), 1, "the continuation started no second agent");
		});
	});

	it("F4 PostgreSQL restarts: the API answers unavailable and confirms nothing; the same key then applies once", { timeout: TEST_MS }, async () => {
		const h = await harness();
		monitorExits(h);
		const api = await startApi(await loadApiConfig({ AF_SECRETS_DIR: info.secretsDir, AF_PG_PORT: String(info.port), AF_API_PORT: "0" }));
		const approve = (key: string): Promise<Response> =>
			fetch(`http://127.0.0.1:${api.port}/tasks/${h.taskId}/approve`, {
				method: "POST", headers: { authorization: bearer, "idempotency-key": key, "content-type": "application/json" },
				body: JSON.stringify({ revision_number: 1, comment: "" }), signal: AbortSignal.timeout(120_000),
			});
		try {
			await withWorkers(h, async () => {
				const handle = await startLifecycle(h);
				await until(handle, "awaiting approval", (s) => s.status === "AWAITING_APPROVAL");

				await docker("stop", postgresContainer);
				const key = "f4-key-accept-1";
				const down = await approve(key);
				const downBody = await down.text();
				assert.equal(down.status, 503, `the API answered ${down.status} ${downBody} while the database was down`);
				assert.equal((JSON.parse(downBody) as { code: string }).code, "unavailable");

				await restartPostgresAndTemporal();
				// Nothing was confirmed during the outage: no decision exists for the key.
				assert.equal((await decisionRows(h.taskId)).filter((row) => row.idempotency_key === key).length, 0, "no decision row for a request that was answered unavailable");
				assert.equal(await dbStatus(h.taskId), "AWAITING_APPROVAL");

				const retry = await eventually(async () => {
					const response = await approve(key);
					if (response.status !== 202) assert.fail(`the resend answered ${response.status} ${await response.text()}`);
					return response;
				}, RESTART_MS);
				const decision = (await retry.json()) as { id: string };
				const rows = (await decisionRows(h.taskId)).filter((row) => row.idempotency_key === key);
				assert.equal(rows.length, 1, "one decision row after the resend");
				assert.equal(rows[0]!.id, decision.id);

				await delivered(decision.id);
				await eventually(async () => assert.equal(await dbStatus(h.taskId), "ACCEPTED"), 60_000);
				const types = await eventTypes(h.taskId);
				assert.equal(countOf(types, "decision_approve"), 1, types.join(","));

				// Sending the same key again is a replay of the confirmed decision, not a second one.
				const again = await approve(key);
				assert.equal(again.status, 202);
				assert.equal(again.headers.get("idempotent-replay"), "true");
				assert.equal((await decisionRows(h.taskId)).filter((row) => row.idempotency_key === key).length, 1);
			});
		} finally {
			await api.stop();
		}
	});

	it("F14 the whole server is off while a task awaits acceptance: the task survives, nothing starts, ACCEPT applies once", { timeout: TEST_MS }, async () => {
		const h = await harness();
		monitorExits(h);
		const record0 = async () => ({ state: await h.handle!.query(stateQuery), status: await dbStatus(h.taskId), events: await eventRows(h.taskId) });
		const before0 = await withWorkers(h, async () => {
			const handle = await startLifecycle(h);
			await until(handle, "awaiting approval", (s) => s.status === "AWAITING_APPROVAL");
			return record0();
		});
		assert.equal(before0.status, "AWAITING_APPROVAL");

		// The whole server stack is off: the workers (stopped above), Temporal and Postgres.
		await docker("stop", temporalContainer);
		await docker("stop", postgresContainer);
		await restartPostgresAndTemporal();
		const callsBefore = h.seen.length;

		await withWorkers(h, async () => {
			await until(h.handle!, "awaiting approval after the restart", (s) => s.status === "AWAITING_APPROVAL");
			const after0 = await eventually(record0);
			assert.deepEqual(after0.state, before0.state, "the task state is the same after the restart");
			assert.equal(after0.status, before0.status);
			assert.deepEqual(after0.events, before0.events, "the same events, the same order");
			assert.equal(h.seen.length, callsBefore, "the local worker idles and starts nothing");

			const accept = await eventually(() => record(h.taskId, "approve", "f14-key-accept-1"));
			await delivered(accept.body.id);
			await until(h.handle!, "accepted", (s) => s.status === "ACCEPTED");
			assert.equal(await dbStatus(h.taskId), "ACCEPTED");
			const types = await eventTypes(h.taskId);
			assert.equal(countOf(types, "decision_approve"), 1, types.join(","));
			assert.equal((await decisionRows(h.taskId)).filter((row) => row.kind === "approve" && row.delivery_state === "applied").length, 1);
		});
	});
});
