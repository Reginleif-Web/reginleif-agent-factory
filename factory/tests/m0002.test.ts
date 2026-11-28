import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { it } from "node:test";
import { selectTransitionsForMigration } from "../contracts/states.ts";
import { migrate } from "../db/migrate.ts";
import { connectBench, databaseBench } from "./database-bench.ts";

const bench = databaseBench(import.meta.url);

it("enforces task transitions, hierarchy, dependencies and account rights", async () => {
	const db = await bench;
	await migrate({ url: db.migratorUrl });
	const sql = connectBench(db.migratorUrl);
	const project = randomUUID(), otherProject = randomUUID();
	const root = randomUUID(), child = randomUUID(), third = randomUUID(), outsider = randomUUID();
	try {
		const transitions = await sql<{ from_status: string; to_status: string }[]>`SELECT from_status, to_status FROM task_transitions ORDER BY from_status, to_status`;
		const pair = (row: { from_status: string; to_status: string }) => `${row.from_status}:${row.to_status}`;
		assert.deepEqual(transitions.map(pair).sort(), [...selectTransitionsForMigration("0002"), ...selectTransitionsForMigration("0011")].map(pair).sort());
		await sql`INSERT INTO projects (id) VALUES (${project}), (${otherProject})`;
		await sql`INSERT INTO execution_policies (id, project_id, version, max_child_depth) VALUES (${randomUUID()}, ${project}, 1, 1)`;
		await sql`INSERT INTO tasks (id, project_id, status, origin_type, depth) VALUES (${root}, ${project}, 'CREATED', 'owner', 0), (${third}, ${project}, 'CREATED', 'owner', 0), (${outsider}, ${otherProject}, 'CREATED', 'owner', 0)`;
		await sql`INSERT INTO tasks (id, project_id, parent_task_id, status, depth) VALUES (${child}, ${project}, ${root}, 'CREATED', 0)`;
		await assert.rejects(sql`INSERT INTO tasks (id, project_id, parent_task_id, status, depth) VALUES (${randomUUID()}, ${project}, ${child}, 'CREATED', 0)`, /depth/i);
		await assert.rejects(sql`INSERT INTO tasks (id, project_id, parent_task_id, status, depth) VALUES (${randomUUID()}, ${otherProject}, ${root}, 'CREATED', 0)`, /different project/i);
		await assert.rejects(sql`UPDATE tasks SET status = 'RUNNING' WHERE id = ${root}`, /transition/i);
		await sql`INSERT INTO task_dependencies (task_id, depends_on_task_id) VALUES (${root}, ${child}), (${child}, ${third})`;
		await assert.rejects(sql`UPDATE task_dependencies SET depends_on_task_id = ${third} WHERE task_id = ${root}`, /immutable/i);
		await assert.rejects(sql`INSERT INTO task_dependencies (task_id, depends_on_task_id) VALUES (${third}, ${root})`, /dependency_cycle/i);
		await assert.rejects(sql`INSERT INTO task_dependencies (task_id, depends_on_task_id) VALUES (${root}, ${outsider})`, /project/i);
		await sql`UPDATE tasks SET status = 'PLANNING' WHERE id = ${root}`;
		await assert.rejects(sql`UPDATE tasks SET status = 'BLOCKED', block_kind = 'scope', block_handler = 'chief_engineer' WHERE id = ${root}`, /owner/i);
		await sql`UPDATE tasks SET status = 'BLOCKED', block_kind = 'dependency', block_handler = 'chief_engineer' WHERE id = ${root}`;
		await sql`UPDATE tasks SET block_handler = 'owner' WHERE id = ${root}`;
		await assert.rejects(sql`UPDATE tasks SET block_handler = 'chief_engineer' WHERE id = ${root}`, /handler/i);
		await assert.rejects(sql`UPDATE tasks SET status = 'ASSIGNED', block_kind = NULL, block_handler = NULL WHERE id = ${root}`, /blocked target/i);
		await assert.rejects(sql`UPDATE tasks SET status = 'READY', block_kind = NULL WHERE id = ${root}`, /block/i);
		await sql`UPDATE tasks SET status = 'READY', block_kind = NULL, block_handler = NULL WHERE id = ${root}`;
		await assert.rejects(sql`UPDATE tasks SET block_handler = 'owner' WHERE id = ${root}`, /block/i);
		await sql`INSERT INTO tasks (id, project_id, status, origin_type, depth) VALUES (${randomUUID()}, ${project}, 'RUNNING', 'standing', 0)`; // database.md line 418: RUNNING from insert
		await assert.rejects(sql`INSERT INTO tasks (id, project_id, status, origin_type, depth) VALUES (${randomUUID()}, ${project}, 'RUNNING', 'standing', 0)`, /unique/i);
		await sql`UPDATE projects SET archived_at = now() WHERE id = ${project}`;
		await assert.rejects(sql`UPDATE projects SET deleted_at = now() WHERE id = ${project}`, /active tasks/i);
		const deletedProject = randomUUID();
		await sql`INSERT INTO projects (id, archived_at, deleted_at) VALUES (${deletedProject}, now(), now())`;
		await assert.rejects(sql`INSERT INTO tasks (id, project_id, status, depth) VALUES (${randomUUID()}, ${deletedProject}, 'CREATED', 0)`, /project is deleted/i);
		for (const [role, url] of [["af_api", db.apiUrl], ["af_control", db.controlUrl], ["af_backup", db.backupUrl]] as const) {
			const connection = connectBench(url);
			try {
				await connection`SELECT * FROM task_transitions LIMIT 1`;
				await assert.rejects(connection`DELETE FROM tasks WHERE id = ${root}`, /permission denied/i, role);
				if (role === "af_backup") await assert.rejects(connection`INSERT INTO tasks (id, depth) VALUES (${randomUUID()}, 0)`, /permission denied/i);
				else await connection`INSERT INTO tasks (id, depth) VALUES (${randomUUID()}, 0)`;
				if (role === "af_control") await connection`UPDATE tasks SET status = 'ASSIGNED' WHERE id = ${root}`;
				else await assert.rejects(connection`UPDATE tasks SET status = 'ASSIGNED' WHERE id = ${root}`, /permission denied/i);
			} finally { await connection.end(); }
		}
		const reader = await sql<{ can_read: boolean }[]>`SELECT has_table_privilege('af_reader', 'tasks', 'SELECT') AS can_read`;
		assert.equal(reader[0]?.can_read, false);
	} finally { await sql.end(); }
});
