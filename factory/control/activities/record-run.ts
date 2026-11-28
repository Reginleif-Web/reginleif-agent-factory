import { randomUUID } from "node:crypto";
import type { PostgresJsDatabase } from "drizzle-orm/postgres-js";
import { taskRuns } from "../../db/schema/runs.ts";

// RecordRun (temporal.md "Run rows"): the one production insert of a task_runs row. The reviewer pick (RequestReview, D20)
// reads the implementers from these rows, so a run the workflow starts is recorded when its execution id is committed, and a
// publish that failed after ACCEPTED is recorded as a failed run (not only as an event). Idempotent: the key is the execution
// id, a retry of the activity (or a replayed step) inserts nothing. The worker row is not known to the workflow (it holds the
// machine id, the row holds the worker uuid), so it stays null.

export interface RecordRunInput {
	readonly taskId: string;
	readonly executionId: string;
	readonly role: "planner" | "implementer" | "tester" | "reviewer" | "chief_engineer";
	readonly agentId: string | null;
	readonly attemptNumber: number;
	readonly status: "created" | "starting" | "running" | "finished" | "failed" | "died" | "unclear" | "cancelling" | "cancelled";
	readonly errorCode?: string;
	readonly errorMessage?: string;
}

export const RecordRun = async (db: PostgresJsDatabase<any>, input: RecordRunInput): Promise<{ readonly recorded: boolean }> => {
	const rows = await db.insert(taskRuns).values({
		id: randomUUID(),
		taskId: input.taskId,
		executionId: input.executionId,
		role: input.role,
		agentId: input.agentId,
		attemptNumber: input.attemptNumber,
		status: input.status,
		errorCode: input.errorCode ?? null,
		errorMessage: input.errorMessage ?? null,
	}).onConflictDoNothing({ target: taskRuns.executionId }).returning({ id: taskRuns.id });
	return { recorded: rows.length > 0 };
};
