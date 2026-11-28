import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { it } from "node:test";
import { phaseTwoBlockKinds, taskTransitions } from "../contracts/states.ts";
import { migrate } from "../db/migrate.ts";
import { connectBench, databaseBench } from "./database-bench.ts";

const bench = databaseBench(import.meta.url);
const handlerOf = (kind: string) => (kind === "scope" || kind === "agent_question" ? "owner" : "chief_engineer");

// A blocked task keeps every exit that task_transitions lists for BLOCKED: cancel, pause and revise (to PLANNING) leave
// BLOCKED whatever its kind; only the progress exit (resolve) is decided by the kind (state-exit rule).
it("lets a blocked task of any kind be cancelled, paused, or revised to planning", async () => {
	const db = await bench;
	await migrate({ url: db.migratorUrl });
	const sql = connectBench(db.migratorUrl);
	const project = randomUUID();
	try {
		await sql`INSERT INTO projects (id) VALUES (${project})`;
		const blockedTask = async (kind: string) => {
			const id = randomUUID();
			await sql`INSERT INTO tasks (id, project_id, status, origin_type, depth) VALUES (${id}, ${project}, 'PLANNING', 'owner', 0)`;
			await sql`UPDATE tasks SET status = 'BLOCKED', block_kind = ${kind}, block_handler = ${handlerOf(kind)} WHERE id = ${id}`;
			return id;
		};
		const exits = taskTransitions.filter((row) => row.from_status === "BLOCKED" && ["CANCELLED", "CANCELLING", "PAUSED", "PLANNING"].includes(row.to_status));
		assert.equal(exits.length, 4);
		for (const kind of phaseTwoBlockKinds) {
			for (const exit of exits) {
				const id = await blockedTask(kind);
				const resumeTo = exit.to_status === "PAUSED" ? "BLOCKED" : null;
				await sql`UPDATE tasks SET status = ${exit.to_status}, resume_to = ${resumeTo}, block_kind = NULL, block_handler = NULL WHERE id = ${id}`;
				const [row] = await sql<{ status: string }[]>`SELECT status FROM tasks WHERE id = ${id}`;
				assert.equal(row?.status, exit.to_status, `${kind} -> ${exit.to_status}`);
			}
		}
		// The progress exit is still decided by the kind.
		const id = await blockedTask("dependency");
		await assert.rejects(sql`UPDATE tasks SET status = 'ASSIGNED', block_kind = NULL, block_handler = NULL WHERE id = ${id}`, /blocked target/i);
		await assert.rejects(sql`UPDATE tasks SET status = 'VERIFYING', block_kind = NULL, block_handler = NULL WHERE id = ${id}`, /blocked target/i);
		await sql`UPDATE tasks SET status = 'READY', block_kind = NULL, block_handler = NULL WHERE id = ${id}`;
	} finally { await sql.end(); }
});
