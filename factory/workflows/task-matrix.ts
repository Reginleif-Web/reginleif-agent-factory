// The decision matrix of temporal.md ("Operations", table "State x operation") as data, and the pure interpreter that turns a
// cell into the next in-memory state. Pure: no clock, no randomness, no I/O. The workflow and the state-exit test share it, so
// the table the test walks is the table the workflow runs.
import type { RefusalCode } from "../control/activities/refusal.ts";
import {
	BLOCK_KINDS, REVISION_KINDS, UPDATE_KINDS, isBlockKind, isFinal,
	type BlockKind, type TaskState, type TaskStatus, type UpdateKind,
} from "./task-state.ts";

/**
 * A cell of the table. R refused `not_allowed_in_state`; X refused `task_closed` (the relay owns that outcome); note recorded,
 * no move; prio / prioLater store the new priority (it "applies" before RUNNING, afterwards it waits for the next attempt);
 * flag sets `pauseRequested`; unflag clears it (R when it is not set); L and P are rules L (cancel) and P (pause); resume goes
 * to `resumeTo`; `revise*` is the starred cell of BLOCKED and PAUSED; resolve leaves BLOCKED by its kind; allow adds an agent;
 * force closes CANCELLING with `endUnconfirmed`; `->X` moves; `stop->X` stops a live run first, then moves.
 */
export type Cell =
	| "R" | "X" | "note" | "prio" | "prioLater" | "flag" | "unflag" | "L" | "P" | "resume" | "revise*" | "resolve" | "allow" | "force"
	| `->${TaskStatus}` | `stop->${TaskStatus}`;

type Row = readonly [Cell, Cell, Cell, Cell, Cell, Cell, Cell, Cell, Cell, Cell, Cell, Cell];
// Cells in the column order of UPDATE_KINDS (the order of the table in temporal.md).
const row = (cells: Row): Readonly<Record<UpdateKind, Cell>> => Object.fromEntries(UPDATE_KINDS.map((kind, index) => [kind, cells[index]])) as Record<UpdateKind, Cell>;

const created = row(["R", "R", "R", "->CANCELLED", "->PAUSED", "R", "->PLANNING", "note", "prio", "R", "R", "R"]);
// architecture.md has no READY->PLANNING or WAITING_DEPENDENCIES->PLANNING: a revise there is stored and taken at the start.
const beforeStart = row(["R", "R", "R", "->CANCELLED", "->PAUSED", "R", "note", "note", "prio", "R", "R", "R"]);
const closed = row(["X", "X", "X", "X", "X", "X", "X", "X", "X", "X", "X", "X"]);

export const MATRIX: Readonly<Record<TaskStatus, Readonly<Record<UpdateKind, Cell>>>> = {
	CREATED: created,
	READY: beforeStart,
	WAITING_DEPENDENCIES: beforeStart,
	PLANNING: row(["R", "R", "R", "L", "P", "unflag", "stop->PLANNING", "note", "prio", "R", "R", "R"]),
	ASSIGNED: row(["R", "R", "R", "L", "P", "unflag", "note", "note", "prio", "R", "R", "allow"]),
	RUNNING: row(["R", "R", "R", "->CANCELLING", "flag", "unflag", "stop->ASSIGNED", "note", "prioLater", "R", "R", "R"]),
	VERIFYING: row(["R", "R", "R", "L", "P", "unflag", "stop->REWORK_REQUIRED", "note", "prioLater", "R", "R", "R"]),
	AWAITING_APPROVAL: row(["->ACCEPTED", "->REJECTED", "->REWORK_REQUIRED", "->CANCELLED", "->PAUSED", "R", "->REWORK_REQUIRED", "note", "prioLater", "R", "R", "R"]),
	REWORK_REQUIRED: row(["R", "R", "R", "->CANCELLED", "->PAUSED", "R", "note", "note", "prioLater", "R", "R", "R"]),
	BLOCKED: row(["R", "R", "R", "L", "->PAUSED", "R", "revise*", "note", "prioLater", "resolve", "R", "R"]),
	// D22: PAUSED allows only resume, cancel, add_feedback, revise_requirements (the same list ReadDecision refuses by).
	PAUSED: row(["R", "R", "R", "L", "R", "resume", "revise*", "note", "R", "R", "R", "R"]),
	CANCELLING: row(["R", "R", "R", "note", "R", "R", "R", "R", "R", "R", "force", "R"]),
	ACCEPTED: closed,
	REJECTED: closed,
	CANCELLED: closed,
	FAILED: closed,
};

/** The cell a decision meets in this state; `unflag` with no flag set is a refusal (the table's "clears flag, else R"). */
export const resolveCell = (state: TaskState, kind: UpdateKind): Cell => {
	const cell = MATRIX[state.status][kind];
	return cell === "unflag" && !state.pauseRequested ? "R" : cell;
};

/** Forward exits with no owner (temporal.md "Every state has an exit"): the system moves package 55 drives. */
export const FORWARD_EXITS: Readonly<Record<TaskStatus, readonly TaskStatus[]>> = {
	CREATED: ["PLANNING"],
	PLANNING: ["READY", "BLOCKED", "FAILED"],
	READY: ["WAITING_DEPENDENCIES", "ASSIGNED"],
	WAITING_DEPENDENCIES: ["ASSIGNED", "BLOCKED"],
	ASSIGNED: ["RUNNING", "BLOCKED", "FAILED"],
	RUNNING: ["VERIFYING", "ASSIGNED", "BLOCKED", "FAILED"],
	VERIFYING: ["AWAITING_APPROVAL", "REWORK_REQUIRED", "BLOCKED", "FAILED"],
	AWAITING_APPROVAL: [],
	REWORK_REQUIRED: ["ASSIGNED"],
	BLOCKED: [],
	PAUSED: [],
	CANCELLING: ["CANCELLED"],
	ACCEPTED: [],
	REJECTED: [],
	CANCELLED: [],
	FAILED: [],
};

const BLOCK_CLEARED = { blockKind: null, blockHandler: null, blockReason: null, revisionStored: false } as const;
// Kinds where the platform is not confirmed safe (D34): a revise is stored and never leaves the block.
const PHYSICAL_KINDS: readonly BlockKind[] = ["safe_stop", "physical_unknown", "quarantine"];

const holdsBlock = (state: TaskState): boolean => state.status === "BLOCKED" || (state.status === "PAUSED" && state.resumeTo === "BLOCKED");

/** A move. Leaving a block clears it (PAUSED keeps it: `resume` returns to BLOCKED); PAUSED remembers where it came from. */
export const moveTo = (state: TaskState, to: TaskStatus): TaskState => {
	const from = state.status;
	let next: TaskState = { ...state, status: to };
	if (holdsBlock(state) && to !== "BLOCKED" && to !== "PAUSED") next = { ...next, ...BLOCK_CLEARED };
	if (from === "PAUSED" && to !== "PAUSED") next = { ...next, resumeTo: null };
	if (to === "PAUSED") next = { ...next, resumeTo: from === "PAUSED" ? state.resumeTo : from, pauseRequested: false };
	if (to === "CANCELLING") next = { ...next, stopping: true, afterStop: "CANCELLED" };
	if (isFinal(to)) next = { ...next, pauseRequested: false, stopping: false, afterStop: null };
	return next;
};

/** The workflow's own move into BLOCKED with a kind (package 55 and 56 call it; the state-exit test uses it too). */
export const blockTask = (state: TaskState, kind: BlockKind, reason: string): TaskState =>
	({ ...moveTo(state, "BLOCKED"), blockKind: kind, blockHandler: BLOCK_KINDS[kind].handler, blockReason: reason });

// Rule L: a live run is stopped first; the move waits for the machine to confirm the end (`stopping`, `afterStop`).
const stopThenMove = (state: TaskState, to: TaskStatus): TaskState =>
	state.executionId !== null ? { ...state, stopping: true, afterStop: to } : moveTo(state, to);

const storeOnly = (state: TaskState): boolean =>
	state.blockKind !== null && (PHYSICAL_KINDS.includes(state.blockKind) || (state.blockKind === "execution" && state.executionId !== null));

const payloadField = (payload: unknown, key: string): unknown =>
	typeof payload === "object" && payload !== null ? (payload as Record<string, unknown>)[key] : undefined;

export interface Applied {
	readonly state: TaskState;
	/** What happened, for the event: moved, noted, pause_requested, pause_cancelled, cancelling, stopping, revision_stored, resume_to_planning, priority_applied, priority_next_attempt, agent_allowed, closed_unconfirmed. */
	readonly effect: string;
}

/** Applies one non-refused cell. `revise_requirements` always takes the next revision; the cell says what else it does. */
export const applyCell = (state: TaskState, kind: UpdateKind, cell: Cell, payload: unknown): Applied => {
	const base: TaskState = kind === "revise_requirements" ? { ...state, revision: state.revision + 1 } : state;
	if (cell === "R" || cell === "X") throw new Error(`applyCell: cell ${cell} is a refusal, not an effect`);
	if (cell.startsWith("->")) return { state: moveTo(base, cell.slice(2) as TaskStatus), effect: "moved" };
	if (cell.startsWith("stop->")) {
		const next = stopThenMove(base, cell.slice(6) as TaskStatus);
		return { state: next, effect: next.status === base.status && next.stopping && !base.stopping ? "stopping" : "moved" };
	}
	switch (cell) {
		case "note": return { state: base, effect: "noted" };
		case "prio":
		case "prioLater": {
			const priority = payloadField(payload, "priority");
			const next = typeof priority === "number" ? { ...base, priority } : base;
			return { state: next, effect: cell === "prio" ? "priority_applied" : "priority_next_attempt" };
		}
		case "flag": return { state: { ...base, pauseRequested: true }, effect: "pause_requested" };
		case "unflag": return { state: { ...base, pauseRequested: false }, effect: "pause_cancelled" };
		case "L":
			return base.executionId !== null ? { state: moveTo(base, "CANCELLING"), effect: "cancelling" } : { state: moveTo(base, "CANCELLED"), effect: "moved" };
		case "P":
			return base.executionId !== null ? { state: { ...base, pauseRequested: true }, effect: "pause_requested" } : { state: moveTo(base, "PAUSED"), effect: "moved" };
		case "resume": return { state: moveTo(base, base.resumeTo ?? "PLANNING"), effect: "moved" };
		case "force": return { state: { ...moveTo(base, "CANCELLED"), endUnconfirmed: true }, effect: "closed_unconfirmed" };
		case "allow": {
			const agent = payloadField(payload, "agent_id");
			const allowed = typeof agent === "string" && !base.allowedAgents.includes(agent) ? [...base.allowedAgents, agent] : base.allowedAgents;
			return { state: { ...base, allowedAgents: allowed, reselectAgent: true }, effect: "agent_allowed" };
		}
		case "resolve": {
			// call_owner (temporal.md "Choices by kind"): the status stays, the handler becomes the owner; a later engineer resolution is `block_handled`.
			if (payloadField(payload, "choice") === "call_owner" && base.blockKind !== null && base.blockHandler === "chief_engineer") {
				return { state: { ...base, blockHandler: "owner" }, effect: "owner_called" };
			}
			const exit = base.revisionStored ? "PLANNING" : base.blockKind === null ? "PLANNING" : BLOCK_KINDS[base.blockKind].exit;
			return { state: moveTo(base, exit), effect: "moved" };
		}
		case "revise*": {
			if (holdsBlock(base) && storeOnly(base)) return { state: { ...base, revisionStored: true }, effect: "revision_stored" };
			if (base.status === "BLOCKED") return { state: moveTo(base, "PLANNING"), effect: "moved" };
			const cleared = holdsBlock(base) ? { ...base, ...BLOCK_CLEARED } : base;
			return { state: { ...cleared, resumeTo: "PLANNING" }, effect: "resume_to_planning" };
		}
	}
	// Unreachable: the `->X` and `stop->X` forms returned above, the switch covers the rest (the tests walk every cell of the matrix).
	throw new Error(`applyCell: unknown cell ${String(cell)}`);
};

export interface DecisionRead {
	readonly actorId: string | null;
	readonly payload: unknown;
	readonly revisionNumber: number | null;
}

/**
 * The refusals the workflow finds from its own memory, which is newer than the rows ReadDecision saw. Same order as the
 * handler (temporal.md "Order of checks"): the actor stage, the matrix, the block kind, the revision. Refusals that need the
 * rows (already_resolved, an inactive owner) are ReadDecision's; the relay's `task_closed` is never found here.
 */
export const memoryRefusal = (state: TaskState, kind: UpdateKind, read: DecisionRead): Exclude<RefusalCode, "task_closed"> | null => {
	if (read.actorId === null) {
		if (kind !== "resolve_block") return "actor_not_allowed";
		if (state.blockHandler !== "chief_engineer") return "block_handled";
	}
	const cell = resolveCell(state, kind);
	if (cell === "R") return "not_allowed_in_state";
	if (cell === "X") return "not_allowed_in_state";
	if (kind === "resolve_block") {
		const asked = payloadField(read.payload, "block_kind");
		if (typeof asked === "string" && (!isBlockKind(asked) || asked !== state.blockKind)) return "block_kind_mismatch";
	}
	if (REVISION_KINDS.includes(kind) && read.revisionNumber !== state.revision) return "revision_stale";
	if (kind === "resolve_block") return resolveRefusal(state, read);
	return payloadRefusal(kind, read.payload);
};

/** The payload forms of the two kinds that carry a value (temporal.md "Refusal codes", `invalid_payload`): last, after every state check. */
const payloadRefusal = (kind: UpdateKind, payload: unknown): "invalid_payload" | null => {
	if (kind === "change_priority") {
		const priority = payloadField(payload, "priority");
		return typeof priority === "number" && Number.isFinite(priority) ? null : "invalid_payload";
	}
	if (kind === "allow_agent") {
		const agent = payloadField(payload, "agent_id");
		return typeof agent === "string" && agent !== "" ? null : "invalid_payload";
	}
	return null;
};

/** The choices of `resolve_block` by kind (temporal.md "Choices by kind"); `call_owner` is the engineer's on any block it handles. */
const RESOLVE_CHOICES: Readonly<Record<string, readonly string[]>> = {
	scope: [],
	dependency: ["remove_dependency", "replace_dependency"],
	execution: ["retry", "raise_limit"],
	usage_limit: ["retry"],
	agent_question: [],
	check_environment: ["retry"],
	rework_limit: ["raise_limit"],
};

/**
 * The form of a resolve_block payload, checked only for the fields it carries (a bare `{block_kind}` stays valid: the exit is the kind's).
 * `answer` is text; `resume_to` is the kind's own exit; `choice` is one of the kind's choices or `call_owner`, which the owner does
 * not use (`actorId` set) and which names a reason. A cost-raising choice by an engineer is the API's `owner_only`, not here.
 */
const resolveRefusal = (state: TaskState, read: DecisionRead): "invalid_payload" | null => {
	const { payload } = read;
	const answer = payloadField(payload, "answer");
	if (answer !== undefined && (typeof answer !== "string" || answer.trim() === "")) return "invalid_payload";
	const resumeTo = payloadField(payload, "resume_to");
	if (resumeTo !== undefined && (state.blockKind === null || resumeTo !== BLOCK_KINDS[state.blockKind].exit)) return "invalid_payload";
	const choice = payloadField(payload, "choice");
	if (choice === undefined) return null;
	if (choice === "call_owner") {
		const reason = payloadField(payload, "reason");
		return read.actorId === null && typeof reason === "string" && reason.trim() !== "" ? null : "invalid_payload";
	}
	return typeof choice === "string" && state.blockKind !== null && (RESOLVE_CHOICES[state.blockKind] ?? []).includes(choice) ? null : "invalid_payload";
};
