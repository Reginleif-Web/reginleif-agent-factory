// The main lifecycle path of TaskWorkflow (temporal.md "Main path", "Verification", "Retry classes", "Run end"): CREATED to a
// final state through the control activities and the machine activities of the machine that holds the run. Workflow code: only
// `@temporalio/workflow`, types and other workflow modules; no clock, randomness or I/O.
//
// The driver runs beside the decision Updates. Both change the one in-memory state, and every change the driver makes goes through
// `host.commit`, which holds the Update lock, checks that the state is still the one the step started from (a decision may have moved
// it meanwhile), writes the event and the `tasks` projection with PersistTaskEvent and only then changes the memory. A step whose
// commit lost the race simply reports progress: the loop reads the new state and decides again.
import {
	ActivityFailure, ApplicationFailure, CancellationScope, condition, isCancellation, proxyActivities, uuid4,
	type ActivityOptions,
} from "@temporalio/workflow";
import type { ControlActivities } from "../control/activities/index.ts";
import type { ActivityCatalog, ActivityName } from "../control/activity-catalog.ts";
import { blockTask, moveTo } from "./task-matrix.ts";
import type { BlockCause } from "../rights/blocks.ts";
import { CONTRACT_VERSION, isFinal, type BlockKind, type TaskState, type TaskStatus } from "./task-state.ts";

/** Settings of the lifecycle (database.md policy columns, read as settings): absent means 0, so a missing limit fails closed. */
export const ATTEMPT_MAX_KEY = "attempt.max";
export const REWORK_MAX_ROUNDS_KEY = "rework.maxRounds";
/** Optional bound of Temporal's retries of one infra failure (temporal.md retry classes); absent means only schedule-to-close bounds them. */
export const RETRY_INFRA_MAX_ATTEMPTS_KEY = "retry.infra.maxAttempts";
/** How many repair runs of a mandatory check that did not run (D26) before BLOCKED `check_environment`; policy column `max_auto_fixes`. */
export const AUTO_FIX_MAX_KEY = "autoFix.max";
/** The longest the workflow sleeps to a known usage-limit reset (seconds, temporal.md retry class `usageLimit`); absent means it does not sleep. */
export const LIMIT_MAX_WAIT_KEY = "limit.maxWait";
export const LIFECYCLE_SETTING_KEYS = [ATTEMPT_MAX_KEY, REWORK_MAX_ROUNDS_KEY, RETRY_INFRA_MAX_ATTEMPTS_KEY, AUTO_FIX_MAX_KEY, LIMIT_MAX_WAIT_KEY];

/** Failure types of the machine activities that retrying cannot cure (temporal.md retry classes `invalid` and `external`). */
const INVALID_TYPES = ["ValidationError", "PermissionDenied", "BudgetExceeded", "BadInput"];
export const MACHINE_NON_RETRYABLE = [...INVALID_TYPES, "PublishFailed"];

export const NO_ELIGIBLE_AGENT = "NoEligibleAgent";
export const DEPENDENCY_CYCLE = "DependencyCycle";

const CHECK_ID = "unit";

/** A finding of a review run (D25); the mirror of `ReviewFinding` in the machine contract. */
interface ReviewFindingOut { readonly key: string; readonly kind: "defect" | "requirement_change" | "suggestion"; readonly text: string }
/** The typed outcome of a run (temporal.md "Run outcomes"); the mirror of `CollectOutcome` in the machine contract (contract version 2). */
type CollectOutcome =
	| { status: "collected"; report: { finalStatus: "done" | "partial" | "failed"; findingAnswers?: { key: string; answer: "accepted" | "disputed"; scope: "technical" | "requirements"; reason: string }[] } }
	| { status: "schemaFinding"; findings: string[] }
	| { status: "died"; reason: string }
	| { status: "limit"; waitSeconds: number | null }
	| { status: "question"; question: string; inquiryId: string; tokenId: string; sessionRef: string | null }
	| { status: "unclear"; reason: string }
	| { status: "review"; verdict: "pass" | "fail" | "inconclusive"; findings: ReviewFindingOut[] }
	| { status: "settled"; outcomes: { key: string; outcome: "confirmed" | "dismissed"; reason: string }[] };

// The machine contracts as the workflow sees them (the zod contracts live with the fakes and the machine worker, not in workflow code).
interface MachineIO {
	PrepareWorkspace: { in: { executionId: string; repo: string; baseRef: string }; out: { worktreePath: string; baseCommit: string } };
	StartAgentExecution: { in: { executionId: string; worktreePath: string }; out: { launchId: string } };
	MonitorExecution: { in: { executionId: string; logOffset: number }; out: { state: "starting" | "running" | "exited" | "lost"; logOffset: number } };
	StopExecution: { in: { executionId: string }; out: { confirmed: boolean } };
	CollectResults: { in: { executionId: string }; out: CollectOutcome };
	UploadResults: { in: { executionId: string; fileHash: string }; out: { uploadedHash: string } };
	RunAutomatedChecks: { in: { executionId: string; checkId: string }; out: { checkId: string; status: "passed" | "failed" | "not_run"; exitCode: number | null; logRef: string } };
	PublishArtifacts: { in: { branch: string; target: string }; out: { url: string; created: boolean } };
}
type MachineName = keyof MachineIO;

export type LifecycleControl = Pick<ControlActivities,
	"PrepareTask" | "ResolveDependencies" | "SelectAgent" | "NotifyUser" | "RequestReview" | "RecordRun" | "RecordReview" | "AskQuestionGate" | "ChooseBlockHandler">;

export interface LifecycleEvent {
	readonly type: string;
	readonly payload: Record<string, unknown>;
}

/** What the lifecycle needs of TaskWorkflow. */
export interface LifecycleHost {
	readonly taskId: string;
	readonly state: () => TaskState;
	/** Count of `wake` signals received (the wake that ends the no-agent wait). */
	readonly wakes: () => number;
	/** Count of everything that may end a wait: wake signals and every handled decision. */
	readonly nudges: () => number;
	/**
	 * Applies `change` (null: an event only) if `guard` still holds of the current state; true when it was applied. Writes the event
	 * and the projection first, so a failure leaves the memory untouched.
	 */
	readonly commit: (guard: (s: TaskState) => boolean, change: ((s: TaskState) => TaskState) | null, event: LifecycleEvent) => Promise<boolean>;
	/** The driver waits for a nudge (true) or is working (false): Continue-As-New is taken only at a wait. */
	readonly parked: (flag: boolean) => void;
	readonly settings: Readonly<Record<string, unknown>>;
	readonly catalog: ActivityCatalog;
	readonly control: LifecycleControl;
}

// A remote activity failure arrives wrapped in an ActivityFailure; a local activity's arrives as the ApplicationFailure itself.
const failureType = (error: unknown): string | null =>
	error instanceof ApplicationFailure ? (error.type ?? null)
		: error instanceof ActivityFailure && error.cause instanceof ApplicationFailure ? (error.cause.type ?? null) : null;

const limitOf = (settings: Readonly<Record<string, unknown>>, key: string): number => {
	const value = settings[key];
	return typeof value === "number" && Number.isFinite(value) && value > 0 ? value : 0;
};

// The run has ended and the flag asked for a pause: the task goes to PAUSED holding the move it would have made (temporal.md "Run end").
const landing = (state: TaskState, to: TaskStatus): TaskState =>
	state.pauseRequested && !isFinal(to) ? { ...moveTo(state, "PAUSED"), resumeTo: to } : moveTo(state, to);

// A block raised while a pause is pending: the task goes to PAUSED that will resume into BLOCKED, holding the block (the flag is not left set).
const blocking = (state: TaskState, kind: Parameters<typeof blockTask>[1], reason: string): TaskState =>
	state.pauseRequested ? { ...moveTo(blockTask(state, kind, reason), "PAUSED"), resumeTo: "BLOCKED" } : blockTask(state, kind, reason);

const runEnded = (state: TaskState): TaskState => ({ ...state, executionId: null, stopping: false, afterStop: null });

type RunRowEnd = { status: Parameters<LifecycleControl["RecordRun"]>[0]["status"]; errorCode?: string; errorMessage?: string };

// What a task_runs row ends as for each typed outcome (temporal.md "Run outcomes"); the review outcomes are runs that finished.
const endRow = (outcome: CollectOutcome): RunRowEnd => {
	switch (outcome.status) {
		case "collected": return outcome.report.finalStatus === "failed" ? { status: "failed", errorCode: "agent_failed" } : { status: "finished" };
		case "schemaFinding": return { status: "failed", errorCode: "schemaFinding", errorMessage: outcome.findings.join("; ") };
		case "died": return { status: "died", errorMessage: outcome.reason };
		case "limit": return { status: "failed", errorCode: "usage_limit" };
		case "unclear": return { status: "unclear", errorMessage: outcome.reason };
		default: return { status: "finished" };
	}
};

/** How a reviewer run (or the settling run of a dispute) ended, after any usage-limit wait. */
type ReviewerEnd = { kind: "stopped" } | { kind: "limit" } | { kind: "outcome"; outcome: CollectOutcome; executionId: string };

export const runLifecycle = async (host: LifecycleHost): Promise<void> => {
	const { control, catalog, settings, taskId } = host;
	const attemptMax = limitOf(settings, ATTEMPT_MAX_KEY);
	const reworkMax = limitOf(settings, REWORK_MAX_ROUNDS_KEY);
	const infraMax = limitOf(settings, RETRY_INFRA_MAX_ATTEMPTS_KEY);
	const autoFixMax = limitOf(settings, AUTO_FIX_MAX_KEY);
	// Driver-local, rebuilt by replay: where the `no_eligible_agent` wait of this selection started, and the Monitor's log offset.
	let noAgentWakes: number | null = null;

	const call = <N extends MachineName>(name: N, machineId: string, input: MachineIO[N]["in"]): Promise<MachineIO[N]["out"]> => {
		const t = catalog.timeouts(name as ActivityName);
		const ms = (seconds: number): number => Math.round(seconds * 1000);
		const options: ActivityOptions = {
			taskQueue: catalog.queue(name as ActivityName, machineId) as string,
			startToCloseTimeout: ms(t.S ?? 0),
			...(t.Q === undefined ? {} : { scheduleToStartTimeout: ms(t.Q) }),
			...(t.H === undefined ? {} : { heartbeatTimeout: ms(t.H) }),
			...(t.C === undefined ? {} : { scheduleToCloseTimeout: ms(t.C) }),
			retry: { nonRetryableErrorTypes: MACHINE_NON_RETRYABLE, ...(infraMax > 0 ? { maximumAttempts: infraMax } : {}) },
		};
		const proxy = proxyActivities<Record<string, (input: unknown) => Promise<unknown>>>(options);
		return proxy[name]!({ contractVersion: CONTRACT_VERSION, ...input }) as Promise<MachineIO[N]["out"]>;
	};

	const note = (type: string, payload: Record<string, unknown> = {}): LifecycleEvent => ({ type, payload });
	const sameRun = (executionId: string) => (s: TaskState): boolean => s.executionId === executionId && !s.stopping;
	// The run is still the one the task is running: not stopped, cancelled or ended meanwhile.
	const runAlive = (executionId: string) => (s: TaskState): boolean => s.status === "RUNNING" && s.executionId === executionId && !s.stopping;

	/**
	 * Runs `work` until it ends or a stop of the run is asked (a stop, a cancel, or any change of the run): the activity is
	 * abandoned at once on the workflow side (TRY_CANCEL), so a hung machine call cannot hold the stop.
	 */
	const raceStop = async <T>(alive: (s: TaskState) => boolean, work: () => Promise<T>): Promise<{ stopped: true } | { stopped: false; value: T }> => {
		const workScope = new CancellationScope();
		const watchScope = new CancellationScope();
		const worked = workScope.run(work).then((value) => ({ stopped: false as const, value }));
		const watched = watchScope.run(() => condition(() => !alive(host.state()))).then(() => ({ stopped: true as const }));
		worked.catch(() => undefined);
		watched.catch(() => undefined);
		try {
			const first = await Promise.race([worked, watched]);
			if (first.stopped) {
				workScope.cancel();
				await worked.catch(() => undefined);
			}
			return first;
		} catch (error) {
			if (isCancellation(error)) return { stopped: true };
			throw error;
		} finally {
			watchScope.cancel();
		}
	};

	/** A machine failure after Temporal's retries: `invalid` fails the task, anything else blocks it `execution` (migration 0011 gave ASSIGNED both exits). */
	const onMachineFailure = async (s: TaskState, error: unknown, where: string): Promise<boolean> => {
		if (!(error instanceof ActivityFailure || error instanceof ApplicationFailure)) throw error;
		const type = failureType(error) ?? "ActivityFailure";
		const reason = `${where}: ${error instanceof ActivityFailure ? (error.cause?.message ?? error.message) : error.message}`;
		const event = note("machine_failure", { where, type, message: reason });
		const stillHere = (cur: TaskState): boolean => cur.status === s.status && cur.executionId === s.executionId;
		if (INVALID_TYPES.includes(type)) await host.commit(stillHere, (cur) => moveTo({ ...runEnded(cur) }, "FAILED"), event);
		else await host.commit(stillHere, (cur) => blocking(cur, "execution", reason), event);
		return true;
	};

	const stopRun = async (s: TaskState): Promise<boolean> => {
		const target = s.afterStop ?? "CANCELLED";
		if (s.executionId === null || s.machineId === null) {
			await host.commit((cur) => cur.executionId === s.executionId && cur.status === s.status, (cur) => landing(runEnded(cur), target), note("stopped", { target }));
			return true;
		}
		let confirmed = false;
		try {
			confirmed = (await call("StopExecution", s.machineId, { executionId: s.executionId })).confirmed;
		} catch (error) {
			if (!(error instanceof ActivityFailure)) throw error;
		}
		if (!confirmed) {
			await host.commit((cur) => cur.executionId === s.executionId, null, note("stop_unconfirmed", { executionId: s.executionId }));
			return false;
		}
		await host.commit((cur) => cur.executionId === s.executionId, (cur) => landing(runEnded(cur), cur.afterStop ?? target), note("stopped", { target }));
		return true;
	};

	/**
	 * Raises a BLOCKED kind through its handler (temporal.md block rule): ChooseBlockHandler decides who is told first, the commit
	 * moves the task (the handler stored on the row is the kind's own, `BLOCK_KINDS`), and an owner-handled block gets one notice.
	 * The notice goes after the commit, keyed by the attempt and rework round, so a replayed step repeats the same key.
	 */
	const raiseBlock = async (
		guard: (s: TaskState) => boolean, change: (s: TaskState) => TaskState, kind: BlockKind, reason: string, event: LifecycleEvent,
		extra: { cause?: BlockCause; raisedByQuestion?: boolean } = {},
	): Promise<boolean> => {
		const decision = await control.ChooseBlockHandler({
			kind, ...(extra.cause === undefined ? {} : { cause: extra.cause }),
			...(extra.raisedByQuestion === undefined ? {} : { raisedByQuestion: extra.raisedByQuestion }),
		});
		const before = host.state();
		const committed = await host.commit(guard, (cur) => blocking(change(cur), kind, reason), event);
		if (committed && decision.notifyOwner) {
			await control.NotifyUser({ id: uuid4(), eventType: "task_blocked", dedupKey: `task_blocked:${taskId}:${kind}:${before.attempt}:${before.rework}`, taskId });
		}
		return committed;
	};

	const lostRun = async (s: TaskState, why: string): Promise<boolean> => {
		if (s.executionId !== null && s.machineId !== null) {
			try { await call("StopExecution", s.machineId, { executionId: s.executionId }); } catch (error) { if (!(error instanceof ActivityFailure)) throw error; }
		}
		const here = sameRun(s.executionId as string);
		if (s.attemptsInRound >= attemptMax) {
			// The budget of attempts is spent: the dead run is let go and a resolved block starts a fresh round.
			await raiseBlock(here, (cur) => ({ ...runEnded(cur), attemptsInRound: 0 }), "execution", `${why}; attempt.max reached`, note("attempts_exhausted", { why }));
		} else {
			await host.commit(here, (cur) => landing(runEnded(cur), "ASSIGNED"), note("run_lost", { why }));
		}
		return true;
	};

	// A usage limit whose reset is known and not above `limit.maxWait` seconds is waited out; any other one blocks (temporal.md retry class `usageLimit`).
	const canWait = (waitSeconds: number | null): waitSeconds is number => {
		const cap = limitOf(settings, LIMIT_MAX_WAIT_KEY);
		return cap > 0 && waitSeconds !== null && waitSeconds <= cap;
	};

	/**
	 * Tells the owner the wait began, then sleeps to the reset on a durable timer (the Update lock is free meanwhile, so decisions still
	 * land). True when the run was stopped or changed before the timer fired; false when the reset time has come.
	 */
	const sleepToReset = async (who: string, waitSeconds: number, alive: (s: TaskState) => boolean): Promise<boolean> => {
		await control.NotifyUser({ id: uuid4(), eventType: "usage_limit_wait", dedupKey: `usage_limit_wait:${taskId}:${who}`, taskId });
		return condition(() => !alive(host.state()), Math.max(1, Math.round(waitSeconds * 1000)));
	};

	/**
	 * One run of an agent on the machine that holds the task's worktree: start, watch to the end, collect the typed outcome. A stop of
	 * the run (`alive` turning false) abandons the activity at once and asks the machine to stop it. Machine failures propagate.
	 */
	const runOnMachine = async (machineId: string, executionId: string, worktreePath: string, alive: (s: TaskState) => boolean) => {
		const raced = await raceStop(alive, async (): Promise<CollectOutcome> => {
			await call("StartAgentExecution", machineId, { executionId, worktreePath });
			let logOffset = 0;
			for (;;) {
				const monitored = await call("MonitorExecution", machineId, { executionId, logOffset });
				logOffset = monitored.logOffset;
				if (monitored.state === "lost") return { status: "died", reason: "execution lost" };
				if (monitored.state === "exited") return call("CollectResults", machineId, { executionId });
			}
		});
		if (raced.stopped) {
			try { await call("StopExecution", machineId, { executionId }); } catch (error) { if (!(error instanceof ActivityFailure)) throw error; }
		}
		return raced;
	};

	/**
	 * A reviewer run (D20), or the settling run of a dispute (D25): a fresh run row with no attempt number (the attempts are the
	 * implementer's), a usage limit waited out as for an implementer. The reviewer works in the implementer's worktree.
	 */
	const reviewerRun = async (machineId: string, agentId: string, worktreePath: string, alive: (s: TaskState) => boolean, round: number): Promise<ReviewerEnd> => {
		for (;;) {
			const executionId = uuid4();
			const row = { taskId, executionId, role: "reviewer" as const, agentId, attemptNumber: null };
			await control.RecordRun({ ...row, status: "starting" });
			let raced;
			try {
				raced = await runOnMachine(machineId, executionId, worktreePath, alive);
			} catch (error) {
				await control.RecordRun({ ...row, status: "failed", errorCode: "machine_failure" });
				throw error;
			}
			if (raced.stopped) {
				await control.RecordRun({ ...row, status: "cancelled" });
				return { kind: "stopped" };
			}
			const outcome = raced.value;
			await control.RecordRun({ ...row, ...endRow(outcome) });
			if (outcome.status !== "limit") return { kind: "outcome", outcome, executionId };
			if (!canWait(outcome.waitSeconds)) return { kind: "limit" };
			if (await sleepToReset(`reviewer:${round}`, outcome.waitSeconds, alive)) return { kind: "stopped" };
		}
	};

	/**
	 * The review of VERIFYING (D20, D25): the picked reviewer runs, its verdict is recorded and the task goes on. `pass` to
	 * AWAITING_APPROVAL; `fail` to REWORK_REQUIRED (or BLOCKED `rework_limit` at `rework.maxRounds`) with the defects to answer; a
	 * reviewer that gave no verdict blocks `check_environment`, a usage limit it cannot wait out blocks `usage_limit`.
	 */
	const runReview = async (s: TaskState, reviewerId: string, round: number, still: (cur: TaskState) => boolean): Promise<void> => {
		if (!(await host.commit(still, null, note("review_started", { round, agentId: reviewerId })))) return;
		const alive = (cur: TaskState): boolean => still(cur) && !cur.stopping;
		const blockHere = (kind: BlockKind, reason: string, event: LifecycleEvent, change: (cur: TaskState) => TaskState = (cur) => ({ ...cur, autoFix: 0 })) =>
			raiseBlock(still, change, kind, reason, event);
		if (s.machineId === null || s.worktree === null) {
			await blockHere("check_environment", "no workspace to review in", note("review_unavailable", { reason: "no workspace" }));
			return;
		}
		const ended = await reviewerRun(s.machineId, reviewerId, s.worktree, alive, round);
		if (ended.kind === "stopped") return;
		if (ended.kind === "limit") {
			await blockHere("usage_limit", "the reviewer hit a usage limit", note("usage_limit", { round, role: "reviewer" }));
			return;
		}
		const outcome = ended.outcome;
		if (outcome.status !== "review") {
			await blockHere("check_environment", `the reviewer run ended as ${outcome.status}`, note("review_unavailable", { reason: outcome.status }));
			return;
		}
		const reviewId = uuid4();
		await control.RecordReview({ step: "review", taskId, reviewId, executionId: ended.executionId, reviewerId, round, verdict: outcome.verdict, findings: outcome.findings });
		if (outcome.verdict === "pass") {
			await host.commit(still, (cur) => landing({ ...cur, autoFix: 0 }, "AWAITING_APPROVAL"), note("review_passed", { round, reviewId }));
			return;
		}
		if (outcome.verdict === "inconclusive") {
			await blockHere("check_environment", "the reviewer could not reach a verdict", note("review_unavailable", { reason: "inconclusive" }));
			return;
		}
		const defects = outcome.findings.filter((finding) => finding.kind === "defect").map((finding) => finding.key);
		const openReview = defects.length > 0 ? { reviewId, keys: defects, reviewerId } : null;
		if (s.rework >= reworkMax) {
			await blockHere("rework_limit", `rework.maxRounds ${reworkMax} reached: the review found defects`, note("rework_limit", { round }), (cur) => ({ ...cur, autoFix: 0, openReview }));
			return;
		}
		await host.commit(still, (cur) => ({ ...moveTo(cur, "REWORK_REQUIRED"), rework: cur.rework + 1, autoFix: 0, openReview }), note("rework_required", { round, reviewId, defects }));
	};

	/**
	 * What the run's typed outcome (temporal.md "Run outcomes") does to the task. The run row ends first. `collected` goes on to the
	 * answers to the open review and to VERIFYING; `died` and a report the schema refused are a lost run (retry class `attempt`);
	 * `unclear` blocks at once; `limit` waits or blocks; `question` goes through the question gate.
	 */
	const finishOutcome = async (s: TaskState, outcome: CollectOutcome): Promise<boolean> => {
		const executionId = s.executionId as string;
		const machineId = s.machineId as string;
		const here = sameRun(executionId);
		await control.RecordRun({ taskId, executionId, role: "implementer", agentId: s.agentId, attemptNumber: s.attempt, ...endRow(outcome) });
		switch (outcome.status) {
			case "schemaFinding":
				return lostRun(s, `report does not match the schema: ${outcome.findings.join("; ")}`);
			case "died":
				return lostRun(s, `the agent died: ${outcome.reason}`);
			case "review":
			case "settled":
				return lostRun(s, "an implementer run returned the outcome of a review");
			case "unclear":
				return raiseBlock(here, (cur) => ({ ...runEnded(cur), runOutcome: outcome.reason, attemptsInRound: 0 }), "execution", `run outcome unclear: ${outcome.reason}`, note("run_unclear", { executionId, reason: outcome.reason }));
			case "limit": {
				if (!canWait(outcome.waitSeconds)) {
					return raiseBlock(here, (cur) => ({ ...runEnded(cur), runOutcome: "usage limit", attemptsInRound: 0 }), "usage_limit", "the agent hit a usage limit", note("usage_limit", { executionId, waitSeconds: outcome.waitSeconds }));
				}
				if (await sleepToReset(`implementer:${s.attempt}`, outcome.waitSeconds, runAlive(executionId))) return true;
				// The wait is not an attempt: the run that hit the limit is given back to the budget and the task is assigned again.
				await host.commit(here, (cur) => ({ ...landing(runEnded(cur), "ASSIGNED"), attemptsInRound: Math.max(0, cur.attemptsInRound - 1), runOutcome: null }), note("usage_limit_elapsed", { executionId, waitSeconds: outcome.waitSeconds }));
				return true;
			}
			case "question": {
				const gate = await control.AskQuestionGate({
					taskId, executionId, kind: "agent_question", inquiryId: outcome.inquiryId, tokenId: outcome.tokenId, sessionRef: outcome.sessionRef, question: outcome.question,
				});
				const sentBack = gate.gated && !gate.pass;
				if (sentBack && s.attemptsInRound < attemptMax) {
					// The registry holds an answer the agent did not use: a new run of the same attempt budget gets the records (temporal.md, G9).
					await host.commit(here, (cur) => landing(runEnded(cur), "ASSIGNED"), note("question_returned", { executionId, records: gate.records }));
					return true;
				}
				const reason = sentBack ? `${outcome.question} (the registry records were returned to the agent; attempt.max reached)` : outcome.question;
				return raiseBlock(here, (cur) => ({ ...runEnded(cur), runOutcome: outcome.question, attemptsInRound: 0 }), "agent_question", reason, note("agent_question", { executionId, question: outcome.question, ...(sentBack ? { records: gate.records } : {}) }));
			}
			case "collected": {
				if (outcome.report.finalStatus === "failed") return lostRun(s, "the agent reported a failed run");
				// The hash of the report file is the machine's to compute; the run's own id stands in for it until the machine returns one.
				await call("UploadResults", machineId, { executionId, fileHash: executionId });
				const open = s.openReview;
				if (open !== null && open.keys.length > 0) {
					const answers = outcome.report.findingAnswers ?? [];
					const missing = open.keys.filter((key) => !answers.some((answer) => answer.key === key));
					if (missing.length > 0) return lostRun(s, `report lacks answers for findings: ${missing.join(", ")}`);
					const given = open.keys.map((key) => answers.find((answer) => answer.key === key)!);
					await control.RecordReview({
						step: "answers", reviewId: open.reviewId, executionId,
						answers: given.map((answer) => ({ key: answer.key, outcome: answer.answer === "accepted" ? "accepted" as const : answer.scope === "requirements" ? "owner" as const : "disputed" as const, reason: answer.reason })),
					});
					const toOwner = given.filter((answer) => answer.answer === "disputed" && answer.scope === "requirements").map((answer) => answer.key);
					if (toOwner.length > 0) {
						return raiseBlock(here, (cur) => ({ ...runEnded(cur), openReview: null, attemptsInRound: 0 }), "scope", `the implementer disputes the requirements behind: ${toOwner.join(", ")}`, note("finding_disputed", { executionId, keys: toOwner, scope: "requirements" }));
					}
					const disputed = given.filter((answer) => answer.answer === "disputed").map((answer) => answer.key);
					if (disputed.length > 0) {
						// D25: a technical dispute goes to one fresh run of the reviewer; its word is final.
						if (s.worktree === null) return lostRun(s, "no workspace for the settling review");
						const ended = await reviewerRun(machineId, open.reviewerId, s.worktree, runAlive(executionId), s.rework);
						if (ended.kind === "stopped") return true;
						if (ended.kind === "limit") {
							return raiseBlock(here, (cur) => ({ ...runEnded(cur), openReview: null, attemptsInRound: 0 }), "usage_limit", "the reviewer hit a usage limit settling a dispute", note("usage_limit", { executionId, role: "reviewer" }));
						}
						if (ended.outcome.status !== "settled") {
							return raiseBlock(here, (cur) => ({ ...runEnded(cur), openReview: null, attemptsInRound: 0 }), "check_environment", `the settling run ended as ${ended.outcome.status}`, note("review_unavailable", { reason: ended.outcome.status }));
						}
						const settledOutcomes = ended.outcome.outcomes;
						await control.RecordReview({ step: "settled", taskId, reviewId: uuid4(), disputesReviewId: open.reviewId, executionId: ended.executionId, reviewerId: open.reviewerId, round: s.rework, outcomes: settledOutcomes });
						const confirmed = settledOutcomes.filter((item) => item.outcome === "confirmed").map((item) => item.key);
						if (confirmed.length > 0) {
							if (s.rework >= reworkMax) {
								return raiseBlock(here, (cur) => ({ ...runEnded(cur), openReview: null, attemptsInRound: 0 }), "rework_limit", `rework.maxRounds ${reworkMax} reached: the reviewer confirmed ${confirmed.join(", ")}`, note("rework_limit", { executionId, confirmed }));
							}
							// The run is RUNNING: REWORK_REQUIRED is reachable only from VERIFYING, so the run ends into VERIFYING first.
							await host.commit(here, (cur) => moveTo(runEnded(cur), "VERIFYING"), note("run_finished", { executionId }));
							await host.commit((cur) => cur.status === "VERIFYING" && cur.executionId === null, (cur) => ({ ...landing(cur, "REWORK_REQUIRED"), rework: cur.rework + 1, openReview: null }), note("rework_required", { executionId, confirmed }));
							return true;
						}
					}
				}
				await host.commit(here, (cur) => landing({ ...runEnded(cur), openReview: null }, "VERIFYING"), note("run_finished", { executionId }));
				return true;
			}
		}
	};

	const monitorRun = async (s: TaskState): Promise<boolean> => {
		const executionId = s.executionId as string;
		const machineId = s.machineId as string;
		let logOffset = 0;
		for (;;) {
			let raced;
			try {
				raced = await raceStop(runAlive(executionId), () => call("MonitorExecution", machineId, { executionId, logOffset }));
			} catch (error) {
				return onMachineFailure(host.state(), error, "MonitorExecution");
			}
			if (raced.stopped) return true;
			const monitored = raced.value;
			logOffset = monitored.logOffset;
			if (monitored.state === "lost") return lostRun(host.state(), "execution lost");
			if (monitored.state !== "exited") continue;
			let outcome: CollectOutcome;
			try {
				outcome = await call("CollectResults", machineId, { executionId });
			} catch (error) {
				return onMachineFailure(host.state(), error, "results");
			}
			try {
				return await finishOutcome(host.state(), outcome);
			} catch (error) {
				return onMachineFailure(host.state(), error, "results");
			}
		}
	};

	const assign = async (s: TaskState): Promise<boolean> => {
		if (s.reselectAgent) {
			await host.commit((cur) => cur.reselectAgent && cur.selectionSeq === s.selectionSeq, (cur) => ({ ...cur, selectionSeq: cur.selectionSeq + 1, reselectAgent: false }), note("agent_reselect", { reason: "allow_agent" }));
			noAgentWakes = null;
			return true;
		}
		if (s.executionId === null) {
			if (noAgentWakes !== null) {
				if (host.wakes() === noAgentWakes) return false;
				noAgentWakes = null;
				await host.commit((cur) => cur.executionId === null && cur.selectionSeq === s.selectionSeq, (cur) => ({ ...cur, selectionSeq: cur.selectionSeq + 1 }), note("agent_reselect", { reason: "wake" }));
				return true;
			}
			let chosen;
			try {
				chosen = (await control.SelectAgent({ taskId, attempt: s.selectionSeq })).chosen;
			} catch (error) {
				if (failureType(error) !== NO_ELIGIBLE_AGENT) throw error;
				noAgentWakes = host.wakes();
				if (s.noAgentNoticeAttempt !== s.attempt) {
					await control.NotifyUser({ id: uuid4(), eventType: "no_eligible_agent", dedupKey: `no_eligible_agent:${taskId}`, taskId });
					await host.commit((cur) => cur.status === "ASSIGNED", (cur) => ({ ...cur, noAgentNoticeAttempt: s.attempt }), note("no_eligible_agent", { attempt: s.attempt }));
				}
				return true;
			}
			const executionId = uuid4();
			await host.commit(
				(cur) => cur.status === "ASSIGNED" && cur.executionId === null && cur.selectionSeq === s.selectionSeq,
				(cur) => ({
					...cur, executionId, lastExecutionId: executionId, agentId: chosen.agentId, machineId: chosen.workerId, attempt: cur.attempt + 1,
					attemptsInRound: cur.attemptsInRound + 1, selectionSeq: cur.selectionSeq + 1, reselectAgent: false,
				}),
				note("agent_assigned", { executionId, agentId: chosen.agentId, machineId: chosen.workerId }),
			);
			// The run row the reviewer pick (D20) reads its implementers from; idempotent by execution id.
			await control.RecordRun({ taskId, executionId, role: "implementer", agentId: chosen.agentId, attemptNumber: host.state().attempt, status: "starting" });
			return true;
		}
		const executionId = s.executionId;
		const machineId = s.machineId as string;
		try {
			const task = await control.PrepareTask({ taskId, revision: s.revision });
			if (task.repositoryUrl === null) {
				await host.commit((cur) => cur.executionId === executionId, null, note("no_repository", { projectId: task.projectId }));
				return false;
			}
			const workspace = await call("PrepareWorkspace", machineId, { executionId, repo: task.repositoryUrl, baseRef: task.defaultBranch ?? "main" });
			await call("StartAgentExecution", machineId, { executionId, worktreePath: workspace.worktreePath });
			await host.commit(
				(cur) => cur.status === "ASSIGNED" && cur.executionId === executionId,
				(cur) => moveTo({ ...cur, worktree: workspace.worktreePath }, "RUNNING"),
				note("run_started", { executionId, machineId }),
			);
			return true;
		} catch (error) {
			return onMachineFailure(s, error, "start");
		}
	};

	const verify = async (s: TaskState): Promise<boolean> => {
		const executionId = s.lastExecutionId as string;
		const machineId = s.machineId as string;
		const round = s.rework + 1;
		try {
			const first = await call("RunAutomatedChecks", machineId, { executionId, checkId: CHECK_ID });
			// D26: a mandatory check that did not run first gets an agent run to repair its cause (autoFix.max of them); only then it blocks.
			const repairAttempted = first.status === "not_run" && s.autoFix >= autoFixMax;
			const review = await control.RequestReview({ taskId, round, repairAttempted, checks: [{ name: CHECK_ID, mandatory: true, status: first.status }] });
			const still = (cur: TaskState): boolean => cur.status === "VERIFYING" && cur.rework === s.rework;
			switch (review.kind) {
				case "reviewer":
					await runReview(s, review.agentId, round, still);
					break;
				case "repair":
					await host.commit(still, (cur) => ({ ...moveTo(cur, "REWORK_REQUIRED"), autoFix: cur.autoFix + 1 }), note("check_repair", { round, names: review.names, autoFix: s.autoFix + 1 }));
					break;
				case "failed":
					if (s.rework >= reworkMax) await host.commit(still, (cur) => blocking({ ...cur, autoFix: 0 }, "rework_limit", `rework.maxRounds ${reworkMax} reached: ${review.names.join(", ")}`), note("rework_limit", { round }));
					else await host.commit(still, (cur) => ({ ...moveTo(cur, "REWORK_REQUIRED"), rework: cur.rework + 1, autoFix: 0 }), note("rework_required", { round, names: review.names }));
					break;
				case "none":
					await host.commit(still, (cur) => blocking(cur, "check_environment", review.reason), note("review_unavailable", { reason: review.reason }));
					break;
				case "blocked":
					await host.commit(still, (cur) => blocking({ ...cur, autoFix: 0 }, "check_environment", `checks not run: ${review.names.join(", ")}`), note("checks_not_run", { names: review.names }));
					break;
			}
			return true;
		} catch (error) {
			return onMachineFailure(s, error, "verification");
		}
	};

	const waitForDependencies = async (s: TaskState, parkOnOpen: boolean): Promise<boolean> => {
		let open: readonly string[] = [];
		let failed: readonly string[] = [];
		try {
			({ open, failed } = await control.ResolveDependencies({ taskId }));
		} catch (error) {
			if (failureType(error) !== DEPENDENCY_CYCLE) throw error;
			await host.commit((cur) => cur.status === s.status, (cur) => (s.status === "READY" ? moveTo(cur, "WAITING_DEPENDENCIES") : blockTask(cur, "dependency", "dependency cycle")), note("dependency_cycle"));
			return true;
		}
		const here = (cur: TaskState): boolean => cur.status === s.status;
		if (s.status === "READY") {
			await host.commit(here, (cur) => moveTo(cur, open.length === 0 && failed.length === 0 ? "ASSIGNED" : "WAITING_DEPENDENCIES"), note("dependencies_resolved", { open: open.length, failed: failed.length }));
			return true;
		}
		if (failed.length > 0) {
			await host.commit(here, (cur) => blockTask(cur, "dependency", `failed dependencies: ${failed.join(", ")}`), note("dependency_failed", { failed }));
			return true;
		}
		if (open.length === 0) {
			await host.commit(here, (cur) => moveTo(cur, "ASSIGNED"), note("dependencies_satisfied"));
			return true;
		}
		return !parkOnOpen;
	};

	const step = async (s: TaskState): Promise<boolean> => {
		switch (s.status) {
			case "CREATED":
				await control.PrepareTask({ taskId, revision: s.revision });
				await host.commit((cur) => cur.status === "CREATED", (cur) => moveTo(cur, "PLANNING"), note("prepared"));
				return true;
			case "PLANNING":
				// The planner run is not driven yet: a task is planned as soon as it is prepared.
				await host.commit((cur) => cur.status === "PLANNING", (cur) => moveTo(cur, "READY"), note("planned"));
				return true;
			case "READY":
				return waitForDependencies(s, false);
			case "WAITING_DEPENDENCIES":
				return waitForDependencies(s, true);
			case "ASSIGNED":
				return assign(s);
			case "RUNNING":
				return s.stopping ? stopRun(s) : monitorRun(s);
			case "CANCELLING":
				return stopRun(s);
			case "VERIFYING":
				return verify(s);
			case "REWORK_REQUIRED":
				await host.commit((cur) => cur.status === "REWORK_REQUIRED", (cur) => ({ ...moveTo(cur, "ASSIGNED"), attemptsInRound: 0 }), note("rework_assigned", { rework: s.rework }));
				return true;
			default:
				// AWAITING_APPROVAL, BLOCKED, PAUSED: the owner or a decision moves the task on.
				return false;
		}
	};

	for (;;) {
		const s = host.state();
		if (isFinal(s.status)) break;
		const seen = host.nudges();
		if (await step(s)) continue;
		host.parked(true);
		try {
			await condition(() => host.nudges() !== seen || isFinal(host.state().status));
		} finally {
			host.parked(false);
		}
	}

	// The accepted work leaves through the machine that ran it (temporal.md, PublishArtifacts after ACCEPTED).
	const done = host.state();
	if (done.status === "ACCEPTED" && done.machineId !== null && done.lastExecutionId !== null) {
		const branch = `task/${taskId}`;
		try {
			const task = await control.PrepareTask({ taskId, revision: done.revision });
			const published = await call("PublishArtifacts", done.machineId, { branch, target: task.defaultBranch ?? "main" });
			await host.commit(() => true, null, note("published", { branch, url: published.url, created: published.created }));
		} catch (error) {
			if (!(error instanceof ActivityFailure)) throw error;
			const message = error.cause?.message ?? error.message;
			// A failed publish is a run row of its own (temporal.md: external class), not only an event.
			await control.RecordRun({ taskId, executionId: uuid4(), role: "implementer", agentId: done.agentId, attemptNumber: done.attempt + 1, status: "failed", errorCode: failureType(error) ?? undefined, errorMessage: message });
			await host.commit(() => true, null, note("publish_failed", { branch, type: failureType(error), message }));
		}
	}
};
