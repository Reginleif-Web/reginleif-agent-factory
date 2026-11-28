import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { drizzle } from "drizzle-orm/postgres-js";
import { migrate } from "../db/migrate.ts";
import { createRepo } from "../db/repo/index.ts";
import { connectBench, createDatabaseBench } from "./database-bench.ts";
import { PersistTaskEvent } from "../control/index.ts";

const benchPromise = createDatabaseBench(import.meta.url);
let bench: Awaited<typeof benchPromise>;
let controlClient: ReturnType<typeof connectBench>;
let controlRepo: ReturnType<typeof createRepo>;
let taskId = "26000000-0000-7000-8000-000000000001";

before(async () => {
	bench = await benchPromise;
	await migrate({ url: bench.migratorUrl });
	const migrationClient = connectBench(bench.migratorUrl);
	const migrationRepo = createRepo(drizzle(migrationClient));
	await migrationRepo.projects.create({ id: "26000000-0000-7000-8000-000000000002", name: "control-test", workspacePolicy: "worktree" });
	await migrationRepo.tasks.create({ id: taskId, projectId: "26000000-0000-7000-8000-000000000002", title: "control-test", priority: 1 }, {
		id: "26000000-0000-7000-8000-000000000003", requirements: "test", acceptanceCriteria: [], constraints: {},
	});
	await migrationClient.end();
	controlClient = connectBench(bench.controlUrl);
	controlRepo = createRepo(drizzle(controlClient));
});

after(async () => {
	await controlClient?.end();
	await bench?.dispose();
});

test("PersistTaskEvent writes one event and retry returns it without another row", async () => {
	const event = {
		id: "26000000-0000-7000-8000-000000000004", taskId, runId: null, eventType: "phase2_test",
		actorType: "workflow" as const, actorId: null, payload: { status: "PLANNING" }, correlationId: null, dedupeKey: "phase2-test",
	};
	const first = await PersistTaskEvent(controlRepo.taskEvents, event);
	const retry = await PersistTaskEvent(controlRepo.taskEvents, event);
	assert.equal(first.id, event.id);
	assert.equal(retry.id, first.id);
	assert.equal((await controlRepo.taskEvents.stream({ below: "18446744073709551615", taskId })).filter((row) => row.dedupeKey === event.dedupeKey).length, 1);
});

// A task event is identified by (task_id, dedupe_key); the id the workflow supplies comes from a replayed uuid sequence that is not
// stable when a lost workflow task re-runs in another order, so the same id may arrive for a different dedupe key. That event must
// still be stored (a duplicate-key error here is retried forever and the workflow stays RUNNING), and its retry must not add a row.
test("PersistTaskEvent stores two events that share an id under different dedupe keys, once each", async () => {
	const shared = "26000000-0000-7000-8000-000000000005";
	const make = (dedupeKey: string) => ({
		id: shared, taskId, runId: null, eventType: "phase2_shared_id", actorType: "workflow" as const, actorId: null,
		payload: { dedupeKey }, correlationId: null, dedupeKey,
	});
	const first = await PersistTaskEvent(controlRepo.taskEvents, make("shared-id-a"));
	const second = await PersistTaskEvent(controlRepo.taskEvents, make("shared-id-b"));
	const retry = await PersistTaskEvent(controlRepo.taskEvents, make("shared-id-b"));
	assert.equal(first.id, shared);
	assert.notEqual(second.id, shared);
	assert.equal(retry.id, second.id);
	const rows = (await controlRepo.taskEvents.stream({ below: "18446744073709551615", taskId, type: "phase2_shared_id" }));
	assert.deepEqual(rows.map((row) => row.dedupeKey).sort(), ["shared-id-a", "shared-id-b"]);
});

test("af_control is refused a forbidden delete statement", async () => {
	await assert.rejects(controlClient`DELETE FROM task_events WHERE id = ${"26000000-0000-7000-8000-000000000004"}`,
		(error: unknown) => (error as { code?: string }).code === "42501");
});
