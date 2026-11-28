import { createHash } from "node:crypto";
import { eq } from "drizzle-orm";
import type { PostgresJsDatabase } from "drizzle-orm/postgres-js";
import type { Repo } from "../../db/repo/index.ts";
import { reviewFindingOutcomes, taskProposals, taskReviews } from "../../db/schema/reviews.ts";
import { taskRuns } from "../../db/schema/runs.ts";
import { createRegistrySearch } from "../../registry/search.ts";
import { ChooseBlockHandler as chooseBlockHandler, type ChooseBlockHandlerInput } from "./block-handler.ts";
import { PersistTaskEvent } from "./persist-task-event.ts";
import { createQuestionGate, type QuestionGateInput } from "./question-gate.ts";

// The control-side handlers of the typed run outcomes (temporal.md "Run outcomes"): the question gate for an `agent_question`
// outcome, the block handler choice, and the recording of a review (D20, D25). Each takes and returns JSON-plain values only
// (a local activity payload), and each write is idempotent: the ids of the rows are derived from the ids the workflow passes,
// because task_reviews and review_finding_outcomes carry no unique key besides the id.

// A deterministic UUID from a key, so a repeated activity attempt (or a replayed step) writes the same row id.
export const rowId = (key: string): string => {
	const hex = createHash("sha256").update(key).digest("hex");
	return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-5${hex.slice(13, 16)}-8${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
};

const runIdOf = async (db: PostgresJsDatabase<any>, executionId: string): Promise<string> => {
	const row = (await db.select({ id: taskRuns.id }).from(taskRuns).where(eq(taskRuns.executionId, executionId)).limit(1))[0];
	if (!row) throw new Error(`no task_runs row for execution ${executionId}`);
	return row.id;
};

// Registry records hold a bigint row version and dates; the payload converter of a local activity holds neither.
const jsonSafe = (value: unknown): unknown => JSON.parse(JSON.stringify(value, (_key, item) => typeof item === "bigint" ? item.toString() : item));

export interface AskQuestionGateInput {
	readonly taskId: string;
	/** The machine run that raised the question: its task_runs row is the run the inquiry belongs to. */
	readonly executionId: string;
	readonly kind: QuestionGateInput["kind"];
	readonly raisedByQuestion?: boolean;
	readonly inquiryId: string;
	readonly tokenId: string;
	readonly sessionRef: string | null;
	readonly question: string;
}

export type AskQuestionGateResult =
	| { readonly gated: false }
	| { readonly gated: true; readonly pass: true; readonly failedOpen: boolean }
	| { readonly gated: true; readonly pass: false; readonly records: readonly unknown[] };

export const AskQuestionGate = async (repo: Repo, db: PostgresJsDatabase<any>, input: AskQuestionGateInput): Promise<AskQuestionGateResult> => {
	const runId = await runIdOf(db, input.executionId);
	const gate = createQuestionGate({
		repo, search: createRegistrySearch(repo, () => undefined),
		persistEvent: (event) => PersistTaskEvent(repo.taskEvents, event),
	});
	const answer = await gate({
		taskId: input.taskId, runId, kind: input.kind, ...(input.raisedByQuestion === undefined ? {} : { raisedByQuestion: input.raisedByQuestion }),
		actor: { tokenId: input.tokenId, sessionRef: input.sessionRef, runId }, inquiryId: input.inquiryId, question: input.question,
	});
	return answer.gated && !answer.pass ? { ...answer, records: jsonSafe(answer.records) as unknown[] } : answer;
};

// No chief-engineer wiring exists yet, so the engineer counts as available: a chief_engineer block is not held back as waiting.
export const ChooseBlockHandler = async (input: Omit<ChooseBlockHandlerInput, "engineerAvailable">) =>
	chooseBlockHandler({ ...input, engineerAvailable: true });

export type FindingKind = "defect" | "requirement_change" | "suggestion";
export interface ReviewFindingInput { readonly key: string; readonly kind: FindingKind; readonly text: string }

export type RecordReviewInput =
	// A review run's verdict: the task_reviews row, and a task_proposals row for every finding that is not a defect (D25: they never block).
	| {
		readonly step: "review"; readonly taskId: string; readonly reviewId: string; readonly executionId: string; readonly reviewerId: string; readonly round: number;
		readonly verdict: "pass" | "fail" | "inconclusive"; readonly findings: readonly ReviewFindingInput[];
	}
	// The implementer's answers to the defects of a review, one outcome row per defect (`owner`: a dispute about requirements, scope or cost).
	| {
		readonly step: "answers"; readonly reviewId: string; readonly executionId: string;
		readonly answers: readonly { readonly key: string; readonly outcome: "accepted" | "disputed" | "owner"; readonly reason: string }[];
	}
	// The settling reviewer run (D25): a fresh review row that disputes the first one, and a final outcome per disputed finding.
	| {
		readonly step: "settled"; readonly taskId: string; readonly reviewId: string; readonly disputesReviewId: string; readonly executionId: string; readonly reviewerId: string; readonly round: number;
		readonly outcomes: readonly { readonly key: string; readonly outcome: "confirmed" | "dismissed"; readonly reason: string }[];
	};

export const RecordReview = async (db: PostgresJsDatabase<any>, input: RecordReviewInput): Promise<{ readonly recorded: true }> => {
	const runId = await runIdOf(db, input.executionId);
	await db.transaction(async (tx) => {
		if (input.step === "review") {
			await tx.insert(taskReviews).values({
				id: input.reviewId, taskId: input.taskId, runId, reviewerId: input.reviewerId, round: input.round, disputesReviewId: null,
				verdict: input.verdict, findings: input.findings,
			}).onConflictDoNothing({ target: taskReviews.id });
			const proposals = input.findings.filter((finding) => finding.kind !== "defect");
			if (proposals.length > 0) {
				await tx.insert(taskProposals).values(proposals.map((finding) => ({
					id: rowId(`proposal|${input.reviewId}|${finding.key}`), taskId: input.taskId, runId, kind: finding.kind, title: finding.text.slice(0, 200),
					requirements: finding.text, covers: [], state: "open",
				}))).onConflictDoNothing({ target: taskProposals.id });
			}
		} else if (input.step === "answers") {
			if (input.answers.length > 0) {
				await tx.insert(reviewFindingOutcomes).values(input.answers.map((answer) => ({
					id: rowId(`outcome|${input.reviewId}|${answer.key}|${answer.outcome}|${input.executionId}`), reviewId: input.reviewId, findingKey: answer.key,
					outcome: answer.outcome, runId, reason: answer.reason,
				}))).onConflictDoNothing({ target: reviewFindingOutcomes.id });
			}
		} else {
			await tx.insert(taskReviews).values({
				id: input.reviewId, taskId: input.taskId, runId, reviewerId: input.reviewerId, round: input.round, disputesReviewId: input.disputesReviewId,
				verdict: input.outcomes.some((outcome) => outcome.outcome === "confirmed") ? "fail" : "pass", findings: input.outcomes,
			}).onConflictDoNothing({ target: taskReviews.id });
			if (input.outcomes.length > 0) {
				await tx.insert(reviewFindingOutcomes).values(input.outcomes.map((outcome) => ({
					id: rowId(`outcome|${input.disputesReviewId}|${outcome.key}|${outcome.outcome}|${input.executionId}`), reviewId: input.disputesReviewId, findingKey: outcome.key,
					outcome: outcome.outcome, runId, reason: outcome.reason,
				}))).onConflictDoNothing({ target: reviewFindingOutcomes.id });
			}
		}
	});
	return { recorded: true };
};
