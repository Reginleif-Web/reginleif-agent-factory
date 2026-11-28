// Activity contracts of the machine lane (temporal.md: "Activities" table, "Retry classes", "Heartbeat details", "Migrations").
// The zod schemas are the contract: every fake parses its input and output with them at run time, and the TypeScript types
// are inferred from them. The production activity modules do not exist yet; when they do, this file moves next to them.
import { z } from "zod";

import { CONTRACT_VERSION } from "../../contracts/contract-version.ts";

/** Version of these contracts, carried in every activity input (temporal.md, Migrations): one constant, shared with the workflow. */
export { CONTRACT_VERSION };

/**
 * Error kinds of temporal.md "Retry classes", with their class. `infra`, `workerLost` and `external` are retried by Temporal
 * (`external` only after the target is checked by its key); `invalid` is non-retryable.
 */
export const ERROR_CLASS_OF = {
	DbUnavailable: "infra",
	ApiUnavailable: "infra",
	NetworkError: "infra",
	HeartbeatTimeout: "workerLost",
	ValidationError: "invalid",
	PermissionDenied: "invalid",
	BudgetExceeded: "invalid",
	BadInput: "invalid",
	PublishFailed: "external",
} as const;

export type ActivityErrorKind = keyof typeof ERROR_CLASS_OF;
export type ErrorClass = (typeof ERROR_CLASS_OF)[ActivityErrorKind];

/** A failure of an activity, as Temporal sees it: its kind, its class and whether Temporal retries it. */
export class ActivityError extends Error {
	readonly kind: ActivityErrorKind;
	readonly errorClass: ErrorClass;
	readonly retryable: boolean;

	constructor(kind: ActivityErrorKind, message: string) {
		super(message);
		this.name = "ActivityError";
		this.kind = kind;
		this.errorClass = ERROR_CLASS_OF[kind];
		this.retryable = this.errorClass !== "invalid";
	}
}

/**
 * Machine activities: every activity of the catalog whose place is not `local` (`machine` queue or the `gpu` queue). Written
 * out as literals because the catalog's helpers widen `place` to ActivityPlace (control/activity-catalog.ts), so a type cannot
 * be derived from the catalog. contracts.test.ts checks this list against the catalog at run time.
 */
export const MACHINE_ACTIVITY_LIST = [
	"PrepareWorkspace",
	"InspectWorkspace",
	"StartAgentExecution",
	"MonitorExecution",
	"StopExecution",
	"CollectResults",
	"UploadResults",
	"RunAutomatedChecks",
	"PublishArtifacts",
	"CleanupWorkspace",
	"RunGpuJob",
	"ReconcileMachine",
] as const;
export type MachineActivityName = (typeof MACHINE_ACTIVITY_LIST)[number];

const ref = z.string().min(1);
const nonNegInt = z.number().int().nonnegative();
const version = z.literal(CONTRACT_VERSION);
const checkStatus = z.enum(["passed", "failed", "not_run"]);

/** The agent's final report (spec 10.1) as `collectResult` validates it; changed files and commits are recomputed by the worker. */
export const AgentReport = z.strictObject({
	actions: z.array(z.string()),
	changedFiles: z.array(z.string()),
	commits: z.array(z.strictObject({ hash: ref, subject: z.string() })),
	testResults: z.array(z.strictObject({ checkId: ref, command: ref, exitCode: z.number().int().nullable(), status: checkStatus, logRef: ref })),
	artifacts: z.array(z.strictObject({ kind: ref, path: ref, hash: ref })),
	problems: z.array(z.string()),
	uncheckedScenarios: z.array(z.string()),
	requirementDeviations: z.array(z.string()),
	finalStatus: z.enum(["done", "partial", "failed"]),
	/** Only a planner run fills it: the child task proposals CreateChildTasks takes (temporal.md "Planning"). */
	proposals: z.array(z.strictObject({ id: ref, title: z.string(), requirements: z.string(), covers: z.array(z.string()) })).optional(),
	/** Only an implementer's rework run fills it: one answer per defect the review found (temporal.md "Review findings", D25). */
	findingAnswers: z.array(z.strictObject({ key: ref, answer: z.enum(["accepted", "disputed"]), scope: z.enum(["technical", "requirements"]), reason: z.string() })).optional(),
});

/** A finding of a reviewer run: `key` is stable within the review, `kind` decides whether it blocks (D25). */
export const ReviewFinding = z.strictObject({ key: ref, kind: z.enum(["defect", "requirement_change", "suggestion"]), text: z.string() });

/**
 * Result of CollectResults and of the adapter's collectResult (contract 2). A report or a schema finding, and the typed outcomes
 * of a run that did not simply finish; every one is data, not an error (temporal.md "Retry classes"):
 * died: the agent process ended without a report; limit: the provider's usage limit, `waitSeconds` until its reset is computed by
 * the machine (null: unknown), so the workflow needs no clock; question: the agent asked the owner (the inquiry is the registry's, G9);
 * unclear: the run ended and the worker cannot tell how; review and settled: the results of a reviewer run and of a settling reviewer run.
 */
export const CollectOutcome = z.discriminatedUnion("status", [
	z.strictObject({ status: z.literal("collected"), report: AgentReport }),
	z.strictObject({ status: z.literal("schemaFinding"), findings: z.array(z.string()) }),
	z.strictObject({ status: z.literal("died"), reason: z.string() }),
	z.strictObject({ status: z.literal("limit"), waitSeconds: nonNegInt.nullable() }),
	z.strictObject({ status: z.literal("question"), question: ref, inquiryId: ref, tokenId: ref, sessionRef: ref }),
	z.strictObject({ status: z.literal("unclear"), reason: z.string() }),
	z.strictObject({ status: z.literal("review"), verdict: z.enum(["pass", "fail", "inconclusive"]), findings: z.array(ReviewFinding) }),
	z.strictObject({ status: z.literal("settled"), outcomes: z.array(z.strictObject({ key: ref, outcome: z.enum(["confirmed", "dismissed"]), reason: z.string() })) }),
]);
export type CollectOutcomeValue = z.infer<typeof CollectOutcome>;

type Schema = z.ZodType;

/** Each contract: input and output schema, heartbeat details schema (null: no heartbeat), error kinds the table allows, idempotency key. */
const contract = <I extends Schema, O extends Schema, B extends Schema | null>(c: {
	readonly input: I;
	readonly output: O;
	readonly heartbeat: B;
	readonly errors: readonly ActivityErrorKind[];
	/** Idempotency key of temporal.md's Key column; undefined for a read only activity without a key. */
	readonly key: (input: z.output<I>) => string | undefined;
	/** Whether the output of a call is stored under its key, so a repeat returns it without a step. Null for reads. */
	readonly remember: ((output: z.output<O>) => boolean) | null;
}) => c;

export const MACHINE_CONTRACTS = {
	PrepareWorkspace: contract({
		input: z.strictObject({ contractVersion: version, executionId: ref, repo: ref, baseRef: ref }),
		output: z.strictObject({ worktreePath: ref, baseCommit: ref }),
		heartbeat: null,
		errors: ["HeartbeatTimeout", "ValidationError"],
		key: (i) => i.executionId,
		remember: () => true,
	}),
	InspectWorkspace: contract({
		input: z.strictObject({ contractVersion: version, worktreePath: ref }),
		output: z.strictObject({ clean: z.boolean(), head: ref }),
		heartbeat: null,
		errors: ["HeartbeatTimeout"],
		key: () => undefined,
		remember: null,
	}),
	StartAgentExecution: contract({
		input: z.strictObject({ contractVersion: version, executionId: ref, worktreePath: ref }),
		output: z.strictObject({ launchId: ref }),
		heartbeat: null,
		errors: ["HeartbeatTimeout", "ValidationError", "PermissionDenied", "BudgetExceeded", "BadInput"],
		key: (i) => i.executionId,
		remember: () => true,
	}),
	MonitorExecution: contract({
		input: z.strictObject({ contractVersion: version, executionId: ref, logOffset: nonNegInt }),
		output: z.strictObject({ state: z.enum(["starting", "running", "exited", "lost"]), logOffset: nonNegInt }),
		heartbeat: z.strictObject({ executionId: ref, logOffset: nonNegInt }),
		errors: ["HeartbeatTimeout"],
		key: (i) => i.executionId,
		remember: null,
	}),
	StopExecution: contract({
		input: z.strictObject({ contractVersion: version, executionId: ref }),
		output: z.strictObject({ confirmed: z.boolean() }),
		heartbeat: null,
		errors: ["HeartbeatTimeout"],
		key: (i) => i.executionId,
		// Only a confirmed end is final; "not confirmed" is asked again (agent-adapters.md, cancel).
		remember: (o) => o.confirmed,
	}),
	CollectResults: contract({
		input: z.strictObject({ contractVersion: version, executionId: ref }),
		output: CollectOutcome,
		heartbeat: null,
		errors: ["HeartbeatTimeout"],
		key: (i) => i.executionId,
		remember: null,
	}),
	UploadResults: contract({
		input: z.strictObject({ contractVersion: version, executionId: ref, fileHash: ref }),
		output: z.strictObject({ uploadedHash: ref }),
		heartbeat: null,
		errors: ["ApiUnavailable", "NetworkError"],
		key: (i) => `${i.executionId}:${i.fileHash}`,
		remember: () => true,
	}),
	RunAutomatedChecks: contract({
		input: z.strictObject({ contractVersion: version, executionId: ref, checkId: ref }),
		output: z.strictObject({ checkId: ref, status: checkStatus, exitCode: z.number().int().nullable(), logRef: ref }),
		heartbeat: z.strictObject({ executionId: ref, checkId: ref }),
		errors: ["HeartbeatTimeout"],
		key: (i) => `${i.executionId}:${i.checkId}`,
		remember: () => true,
	}),
	PublishArtifacts: contract({
		input: z.strictObject({ contractVersion: version, branch: ref, target: ref }),
		output: z.strictObject({ url: ref, created: z.boolean() }),
		heartbeat: null,
		errors: ["PublishFailed", "ValidationError"],
		key: (i) => `${i.branch}@${i.target}`,
		remember: () => true,
	}),
	CleanupWorkspace: contract({
		input: z.strictObject({ contractVersion: version, worktreePath: ref }),
		output: z.strictObject({ status: z.enum(["removed", "kept"]), reason: z.literal("unpublishedChanges").optional() }),
		heartbeat: null,
		errors: ["HeartbeatTimeout"],
		key: (i) => i.worktreePath,
		remember: () => true,
	}),
	RunGpuJob: contract({
		input: z.strictObject({ contractVersion: version, jobKey: ref, executionId: ref }),
		output: z.strictObject({ status: z.enum(["succeeded", "failed"]), outputRef: ref }),
		heartbeat: z.strictObject({ executionId: ref, logOffset: nonNegInt }),
		errors: ["HeartbeatTimeout", "ValidationError"],
		key: (i) => i.jobKey,
		remember: () => true,
	}),
	ReconcileMachine: contract({
		input: z.strictObject({ contractVersion: version }),
		output: z.strictObject({ registered: z.boolean() }),
		heartbeat: null,
		errors: ["ApiUnavailable", "NetworkError"],
		key: () => undefined,
		remember: null,
	}),
} satisfies Record<MachineActivityName, unknown>;

type OutputOf<S> = S extends Schema ? z.output<S> : never;
export type ActivityInput<K extends MachineActivityName> = OutputOf<(typeof MACHINE_CONTRACTS)[K]["input"]>;
export type ActivityOutput<K extends MachineActivityName> = OutputOf<(typeof MACHINE_CONTRACTS)[K]["output"]>;
export type ActivityHeartbeat<K extends MachineActivityName> = OutputOf<(typeof MACHINE_CONTRACTS)[K]["heartbeat"]>;

/** One valid input per machine activity; the contract test runs every fake with it. Typed, so a schema change breaks here. */
export const SAMPLE_INPUTS: { readonly [K in MachineActivityName]: ActivityInput<K> } = {
	PrepareWorkspace: { contractVersion: CONTRACT_VERSION, executionId: "exec-1", repo: "repo", baseRef: "main" },
	InspectWorkspace: { contractVersion: CONTRACT_VERSION, worktreePath: "/work/exec-1" },
	StartAgentExecution: { contractVersion: CONTRACT_VERSION, executionId: "exec-1", worktreePath: "/work/exec-1" },
	MonitorExecution: { contractVersion: CONTRACT_VERSION, executionId: "exec-1", logOffset: 0 },
	StopExecution: { contractVersion: CONTRACT_VERSION, executionId: "exec-1" },
	CollectResults: { contractVersion: CONTRACT_VERSION, executionId: "exec-1" },
	UploadResults: { contractVersion: CONTRACT_VERSION, executionId: "exec-1", fileHash: "sha256-a" },
	RunAutomatedChecks: { contractVersion: CONTRACT_VERSION, executionId: "exec-1", checkId: "unit" },
	PublishArtifacts: { contractVersion: CONTRACT_VERSION, branch: "task/1", target: "origin" },
	CleanupWorkspace: { contractVersion: CONTRACT_VERSION, worktreePath: "/work/exec-1" },
	RunGpuJob: { contractVersion: CONTRACT_VERSION, jobKey: "gpu-1", executionId: "exec-1" },
	ReconcileMachine: { contractVersion: CONTRACT_VERSION },
};

/** A valid output per machine activity: what a fake returns when a test scripts no behavior. */
export const SAMPLE_OUTPUTS: { readonly [K in MachineActivityName]: ActivityOutput<K> } = {
	PrepareWorkspace: { worktreePath: "/work/exec-1", baseCommit: "abc123" },
	InspectWorkspace: { clean: true, head: "abc123" },
	StartAgentExecution: { launchId: "launch-1" },
	MonitorExecution: { state: "running", logOffset: 0 },
	StopExecution: { confirmed: true },
	CollectResults: { status: "collected", report: { actions: [], changedFiles: [], commits: [], testResults: [], artifacts: [], problems: [], uncheckedScenarios: [], requirementDeviations: [], finalStatus: "done" } },
	UploadResults: { uploadedHash: "sha256-a" },
	RunAutomatedChecks: { checkId: "unit", status: "passed", exitCode: 0, logRef: "log-1" },
	PublishArtifacts: { url: "https://example.invalid/pr/1", created: true },
	CleanupWorkspace: { status: "removed" },
	RunGpuJob: { status: "succeeded", outputRef: "out-1" },
	ReconcileMachine: { registered: true },
};

export const MACHINE_ACTIVITY_NAMES = Object.keys(MACHINE_CONTRACTS) as MachineActivityName[];

/** The contract fields without their per-activity types, for code that handles every machine activity the same way. */
export interface LooseContract {
	readonly input: z.ZodType;
	readonly output: z.ZodType;
	readonly heartbeat: z.ZodType | null;
	readonly errors: readonly ActivityErrorKind[];
	readonly key: (input: unknown) => string | undefined;
	readonly remember: ((output: unknown) => boolean) | null;
}

export const contractOf = (name: MachineActivityName): LooseContract => MACHINE_CONTRACTS[name] as unknown as LooseContract;
