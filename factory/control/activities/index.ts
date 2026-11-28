import type { PostgresJsDatabase } from "drizzle-orm/postgres-js";
import type { Repo } from "../../db/repo/index.ts";
import { routingReadRepo } from "../../db/repo/routing-reads.ts";
import { MachineLiveness, type MachineLivenessInput } from "./machine-liveness.ts";
import { LoadSettings } from "./load-settings.ts";
import { NotifyUser, type NotifyUserInput } from "./notify-user.ts";
import { PersistTaskEvent, type PersistTaskEventInput } from "./persist-task-event.ts";
import { PrepareTask, type PrepareTaskInput } from "./prepare-task.ts";
import { ReadDecision, type ReadDecisionInput } from "./read-decision.ts";
import { RecordDecisionOutcome, type RecordDecisionOutcomeInput } from "./record-decision-outcome.ts";
import { RecordRun, type RecordRunInput } from "./record-run.ts";
import { RequestReview, type RequestReviewInput } from "./request-review.ts";
import type { ChooseBlockHandlerInput } from "./block-handler.ts";
import { ResolveDependencies } from "./resolve-dependencies.ts";
import { AskQuestionGate, ChooseBlockHandler, RecordReview, type AskQuestionGateInput, type RecordReviewInput } from "./run-outcomes.ts";
import { SelectAgent, type SelectAgentInput } from "./select-agent.ts";

export { MachineLiveness, type MachineLivenessInput, type MachineLivenessResult } from "./machine-liveness.ts";
export { LoadSettings } from "./load-settings.ts";
export { PersistTaskEvent, type PersistTaskEventInput } from "./persist-task-event.ts";
export { ReadDecision, type ReadDecisionInput, type ReadDecisionResult } from "./read-decision.ts";
export { RecordDecisionOutcome, type RecordDecisionOutcomeInput, type RecordDecisionOutcomeResult } from "./record-decision-outcome.ts";
export { DecisionNotFoundError, isRefusalCode, refusalCodes, TASK_CLOSED, type RefusalCode } from "./refusal.ts";

// The local activities bound to a repo and a database (connected as af_control), keyed by their ActivityCatalog names. Each
// takes one argument, as Temporal passes it. Timeouts come from the settings `act.<Name>.*` through the ActivityCatalog.
// NotifyUser takes the time here, outside the workflow: the workflow code has no clock it may pass on.
export const createControlActivities = (repo: Repo, db: PostgresJsDatabase<any>) => ({
	ReadDecision: (input: ReadDecisionInput) => ReadDecision(repo, input),
	RecordDecisionOutcome: (input: RecordDecisionOutcomeInput) => RecordDecisionOutcome(repo, input),
	PersistTaskEvent: (event: PersistTaskEventInput) => PersistTaskEvent(repo.taskEvents, event),
	LoadSettings: (input: { keys: readonly string[] }) => LoadSettings(repo.settings, input.keys),
	// Package 57: one liveness pass; it needs the database handle for its per-worker transactions.
	MachineLiveness: (input: MachineLivenessInput) => MachineLiveness(db, input),
	PrepareTask: (input: PrepareTaskInput) => PrepareTask(repo, input),
	ResolveDependencies: (input: { taskId: string }) => ResolveDependencies(repo, input),
	SelectAgent: (input: SelectAgentInput) => SelectAgent({ ...repo, reads: routingReadRepo(db) }, input),
	// Only the id and the flag leave the activity: the stored row carries a bigint `seq` and an xid8, which the JSON payload
	// converter of a local activity cannot hold (a failed conversion retries the activity forever).
	NotifyUser: async (input: Omit<NotifyUserInput, "now">) => {
		const { notification, created } = await NotifyUser(db, { ...input, now: new Date() });
		return { id: notification.id, created };
	},
	RequestReview: (input: RequestReviewInput) => RequestReview(db, input),
	RecordRun: (input: RecordRunInput) => RecordRun(db, input),
	// The typed run outcomes (temporal.md "Run outcomes"): the question gate, the block handler choice and the review record.
	AskQuestionGate: (input: AskQuestionGateInput) => AskQuestionGate(repo, db, input),
	ChooseBlockHandler: (input: Omit<ChooseBlockHandlerInput, "engineerAvailable">) => ChooseBlockHandler(input),
	RecordReview: (input: RecordReviewInput) => RecordReview(db, input),
});

export type ControlActivities = ReturnType<typeof createControlActivities>;
