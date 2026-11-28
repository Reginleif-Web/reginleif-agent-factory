// In-memory state of TaskWorkflow (temporal.md "TaskWorkflow keeps in memory") and the names it is made of. Workflow-side
// module: pure data and types, no clock, no randomness, no I/O, so it bundles with the workflow code and the tests use it too.
import type { RefusalCode } from "../control/activities/refusal.ts";
import { CONTRACT_VERSION } from "../contracts/contract-version.ts";
import type { TaskProjection } from "../db/repo/decisions.ts";

/** The 16 states of the architecture table (architecture.md, task state machine). */
export const TASK_STATUSES = [
	"CREATED", "READY", "WAITING_DEPENDENCIES", "PLANNING", "ASSIGNED", "RUNNING", "VERIFYING", "AWAITING_APPROVAL",
	"REWORK_REQUIRED", "BLOCKED", "PAUSED", "CANCELLING", "ACCEPTED", "REJECTED", "CANCELLED", "FAILED",
] as const;
export type TaskStatus = (typeof TASK_STATUSES)[number];

export const FINAL_STATUSES: readonly TaskStatus[] = ["ACCEPTED", "REJECTED", "CANCELLED", "FAILED"];
export const isFinal = (status: TaskStatus): boolean => FINAL_STATUSES.includes(status);

/** The 12 Update ids (temporal.md "Operations"): exactly the decision kinds except `physical_decision`. */
export const UPDATE_KINDS = [
	"approve", "reject", "request_changes", "cancel", "pause", "resume", "revise_requirements", "add_feedback",
	"change_priority", "resolve_block", "force_close", "allow_agent",
] as const;
export type UpdateKind = (typeof UPDATE_KINDS)[number];
export const isUpdateKind = (value: string): value is UpdateKind => (UPDATE_KINDS as readonly string[]).includes(value);

/** Kinds that carry the revision the actor saw (temporal.md; the same list as ReadDecision's `revisionKinds`). */
export const REVISION_KINDS: readonly UpdateKind[] = ["approve", "reject", "request_changes", "resolve_block", "revise_requirements"];

export type BlockHandler = "owner" | "chief_engineer";

/** BLOCKED kinds (temporal.md table "Kind / Handled first by / Exit"): where `resolve_block` leads and who handles first. */
export const BLOCK_KINDS = {
	scope: { exit: "PLANNING", handler: "owner" },
	dependency: { exit: "READY", handler: "chief_engineer" },
	execution: { exit: "ASSIGNED", handler: "chief_engineer" },
	usage_limit: { exit: "ASSIGNED", handler: "chief_engineer" },
	agent_question: { exit: "ASSIGNED", handler: "owner" },
	check_environment: { exit: "VERIFYING", handler: "chief_engineer" },
	rework_limit: { exit: "REWORK_REQUIRED", handler: "chief_engineer" },
	safe_stop: { exit: "ASSIGNED", handler: "chief_engineer" },
	physical_unknown: { exit: "ASSIGNED", handler: "chief_engineer" },
	maintenance: { exit: "ASSIGNED", handler: "chief_engineer" },
	quarantine: { exit: "ASSIGNED", handler: "owner" },
} as const satisfies Record<string, { readonly exit: TaskStatus; readonly handler: BlockHandler }>;
export type BlockKind = keyof typeof BLOCK_KINDS;
export const isBlockKind = (value: unknown): value is BlockKind => typeof value === "string" && Object.hasOwn(BLOCK_KINDS, value);

/** What a decision came to; the value of `handled` and the result of an Update. JSON-plain: it crosses Continue-As-New. */
export interface HandledOutcome {
	readonly outcome: "applied" | "refused";
	readonly refusal: RefusalCode | null;
	/** Status after the decision (unchanged for a refusal). */
	readonly status: TaskStatus;
}

export interface TaskState {
	readonly taskId: string;
	readonly status: TaskStatus;
	readonly revision: number;
	readonly attempt: number;
	readonly rework: number;
	readonly autoFix: number;
	readonly priority: number | null;
	/** Empty (null) when no run is alive: "L" of the matrix. */
	readonly executionId: string | null;
	readonly machineId: string | null;
	readonly worktree: string | null;
	readonly pauseRequested: boolean;
	/** A stop of the live run was asked; `afterStop` is where the task goes when the machine confirms its end. */
	readonly stopping: boolean;
	readonly afterStop: TaskStatus | null;
	readonly endUnconfirmed: boolean;
	readonly resumeTo: TaskStatus | null;
	readonly blockKind: BlockKind | null;
	readonly blockHandler: BlockHandler | null;
	readonly blockReason: string | null;
	/** A new revision was stored while a block that must not be left by it (physical kinds) holds the task; `resolve_block` then plans again. */
	readonly revisionStored: boolean;
	/** `allow_agent` was applied: the next ASSIGNED step selects the agent again. */
	readonly reselectAgent: boolean;
	readonly allowedAgents: readonly string[];
	readonly dependencyIds: readonly string[];
	readonly childIds: readonly string[];
	/** decision id to outcome: the dedup that survives Continue-As-New (Temporal's own Update dedup does not). */
	readonly handled: Readonly<Record<string, HandledOutcome>>;
	/** The settings snapshot of the catalog and the history bound; null until loaded. */
	readonly settings: Readonly<Record<string, unknown>> | null;
	readonly regulationVersionId: string | null;
	readonly budgetUsed: number;
	readonly contractVersion: number;
	/** The agent the current attempt was routed to (null before SelectAgent); projected to `tasks.assigned_agent_id`. */
	readonly agentId: string | null;
	/** Count of lifecycle events written: the dedupe key `lifecycle:<n>` of the next one, so a replay or a Continue-As-New repeats the same key. */
	readonly eventSeq: number;
	/** Set once the whole `no_eligible_agent` wait of this attempt was announced, so the notice goes out once per attempt. */
	readonly noAgentNoticeAttempt: number | null;
	/** The question or limit the last run ended with, kept for the BLOCKED reason and the gate. */
	readonly runOutcome: string | null;
	/** Runs started since the last rework round began: bounded by `attempt.max`. `attempt` itself only grows. */
	readonly attemptsInRound: number;
	/** Counts the SelectAgent calls made so far: the `attempt` field of its key, so a wake or `allow_agent` selects again under a new key. */
	readonly selectionSeq: number;
	/** The last run's execution id, kept after `executionId` is cleared: the publish after ACCEPTED goes to that run's machine. */
	readonly lastExecutionId: string | null;
}

/** The start argument of the workflow (`[{taskId}]`); `carried` is set only by Continue-As-New. */
export interface TaskWorkflowInput {
	readonly taskId: string;
	readonly carried?: TaskState;
	/** Run only the operation matrix and decisions, not the lifecycle (package 54's bench; tests of the matrix alone). Carried over Continue-As-New. */
	readonly decisionsOnly?: boolean;
}

/** The Update argument: only the decision id (the relay never carries the payload; ReadDecision reads it, D23). */
export interface DecisionUpdateInput {
	readonly decisionId: string;
}

/**
 * The columns of the `tasks` row that follow the workflow (temporal.md "The task row follows the workflow"). The row holds a block
 * kind and handler only in BLOCKED (CHECKs `tasks_block_kind_status` and `tasks_block_handler_status`); a PAUSED task that will
 * resume into BLOCKED keeps its block in the workflow memory only, and the row shows `resume_to = 'BLOCKED'`.
 */
export const projectionOf = (state: TaskState): TaskProjection => ({
	status: state.status, currentRevision: state.revision,
	blockKind: state.status === "BLOCKED" ? state.blockKind : null, blockReason: state.blockReason,
	blockHandler: state.status === "BLOCKED" ? state.blockHandler : null, resumeTo: state.resumeTo, pauseRequested: state.pauseRequested, stopping: state.stopping,
	endUnconfirmed: state.endUnconfirmed, assignedAgentId: state.agentId, completed: isFinal(state.status),
});

/** Revision of the contract of the carried state; a Continue-As-New into code with another one is a patch concern (PatchGate). */
export { CONTRACT_VERSION };

export const initialState = (taskId: string): TaskState => ({
	taskId, status: "CREATED", revision: 1, attempt: 0, rework: 0, autoFix: 0, priority: null,
	executionId: null, machineId: null, worktree: null,
	pauseRequested: false, stopping: false, afterStop: null, endUnconfirmed: false, resumeTo: null,
	blockKind: null, blockHandler: null, blockReason: null, revisionStored: false, reselectAgent: false,
	allowedAgents: [], dependencyIds: [], childIds: [], handled: {}, settings: null,
	regulationVersionId: null, budgetUsed: 0, contractVersion: CONTRACT_VERSION,
	agentId: null, eventSeq: 0, noAgentNoticeAttempt: null, runOutcome: null, attemptsInRound: 0, selectionSeq: 0,
	lastExecutionId: null,
});
