// ActivityCatalog (temporal.md "Activities"): activity names, where each runs and its timeouts. Shared by the Control
// Worker, the Machine Worker and the workflows. Timeouts are settings `act.<Name>.<kind>`; the value is a number of
// seconds (the unit of the other timing settings such as `stop.orderSeconds`, database.md settings row). No default is
// set here: numbers are fixed in the measuring phase, an absent setting is an error, not a guess.
import type { SettingsReader } from "../db/repo/settings.ts";

/** S start-to-close, Q schedule-to-start, H heartbeat, C schedule-to-close (temporal.md). */
export type TimeoutKind = "S" | "Q" | "H" | "C";
/** `local`: proxyLocalActivities in the Control Worker (D28). `machine`: queue `machine-<id>`. `gpu`: `machine-<id>-gpu`. */
export type ActivityPlace = "local" | "machine" | "gpu";

export interface ActivityDef {
	readonly place: ActivityPlace;
	/** Timeout kinds the table of temporal.md gives this activity; only these are read and required. */
	readonly kinds: readonly TimeoutKind[];
}

const local = (kinds: readonly TimeoutKind[] = ["S", "C"]): ActivityDef => ({ place: "local", kinds });
const machine = (kinds: readonly TimeoutKind[]): ActivityDef => ({ place: "machine", kinds });

/** The table of temporal.md "Activities", phase 3 and 4 rows. Later phases (personnel, robotics) add their rows. */
export const ACTIVITIES = {
	PrepareTask: local(),
	ResolveDependencies: local(),
	SelectAgent: local(),
	ReadDecision: local(),
	RecordDecisionOutcome: local(),
	PersistTaskEvent: local(),
	NotifyUser: local(),
	CreateChildTasks: local(),
	RequestReview: local(),
	RecordRun: local(),
	LoadSettings: local(["S"]),
	RecordPrecedent: local(),
	// Package 53: registry-first gate, block handler choice, regulation version captured at a run's start (temporal.md block rule, G9).
	AskQuestionGate: local(),
	ChooseBlockHandler: local(["S"]),
	CaptureRegulationVersion: local(),
	PrepareWorkspace: machine(["S", "Q", "C"]),
	InspectWorkspace: machine(["S", "Q"]),
	StartAgentExecution: machine(["S", "Q", "C"]),
	MonitorExecution: machine(["S", "Q", "H"]),
	StopExecution: machine(["S", "Q", "C"]),
	CollectResults: machine(["S", "Q", "C"]),
	UploadResults: machine(["S", "Q", "C"]),
	RunAutomatedChecks: machine(["S", "Q", "H"]),
	PublishArtifacts: machine(["S", "Q", "C"]),
	CleanupWorkspace: machine(["S", "Q"]),
	RunGpuJob: { place: "gpu", kinds: ["S", "Q", "H"] },
	ReconcileMachine: machine(["S"]),
	// Package 57: one pass of MachineLivenessWorkflow over the `workers` rows (local, S only, as LoadSettings).
	MachineLiveness: local(["S"]),
} as const satisfies Record<string, ActivityDef>;

export type ActivityName = keyof typeof ACTIVITIES;

export const settingKey = (name: ActivityName, kind: TimeoutKind): string => `act.${name}.${kind}`;

/** Every setting key the catalog reads; the Control Worker loads them with `LoadSettings` at start. */
export const allTimeoutKeys = (): string[] =>
	(Object.keys(ACTIVITIES) as ActivityName[]).flatMap((name) => ACTIVITIES[name].kinds.map((kind) => settingKey(name, kind)));

/** Every `act.*` key of the catalog whose value is absent or not a positive number of seconds (all of them, not the first). */
export const invalidTimeoutKeys = (snapshot: Readonly<Record<string, unknown>>): string[] =>
	allTimeoutKeys().filter((key) => {
		const value = snapshot[key];
		return typeof value !== "number" || !Number.isFinite(value) || value <= 0;
	});

export interface ActivityCatalog {
	readonly def: (name: ActivityName) => ActivityDef;
	/** Task queue for a machine activity; `local` activities have none (no activity task exists). */
	readonly queue: (name: ActivityName, machineId: string) => string | undefined;
	/** Timeouts in seconds, one entry per kind of the activity's row. */
	readonly timeouts: (name: ActivityName) => Partial<Record<TimeoutKind, number>>;
	/** Names of the `local` activities: what the Control Worker registers and `proxyLocalActivities` may call. */
	readonly localNames: () => ActivityName[];
}

/** Builds the catalog from a settings snapshot (`snapshot(allTimeoutKeys())`). Throws on a missing or invalid value. */
export const createActivityCatalog = (snapshot: Readonly<Record<string, unknown>>): ActivityCatalog => {
	const timeouts = (name: ActivityName): Partial<Record<TimeoutKind, number>> => {
		const out: Partial<Record<TimeoutKind, number>> = {};
		for (const kind of ACTIVITIES[name].kinds) {
			const key = settingKey(name, kind);
			const value = snapshot[key];
			if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) throw new Error(`setting ${key} must be a positive number of seconds`);
			out[kind] = value;
		}
		return out;
	};
	return {
		def: (name) => ACTIVITIES[name],
		queue: (name, machineId) => {
			const { place } = ACTIVITIES[name];
			return place === "local" ? undefined : place === "gpu" ? `machine-${machineId}-gpu` : `machine-${machineId}`;
		},
		timeouts,
		localNames: () => (Object.keys(ACTIVITIES) as ActivityName[]).filter((name) => ACTIVITIES[name].place === "local"),
	};
};

/**
 * The workflow task timeout of TaskWorkflow, seconds. Source: the Temporal server default (10 s); `startTask` sets none.
 * A local activity runs inside a workflow task, so its start-to-close must stay below this (temporal.md, D28).
 */
export const WORKFLOW_TASK_TIMEOUT_SECONDS = 10;

/** The local activities whose `S` is not below the workflow task timeout (the bound check of the Control Worker and the tests). */
export const localTimeoutViolations = (catalog: ActivityCatalog, bound: number = WORKFLOW_TASK_TIMEOUT_SECONDS): string[] =>
	catalog.localNames().filter((name) => {
		const s = catalog.timeouts(name).S;
		return s !== undefined && s >= bound;
	}).map((name) => `${settingKey(name, "S")} must be below ${bound}`);

export const loadActivityCatalog =async (settings: SettingsReader): Promise<ActivityCatalog> => createActivityCatalog(await settings.snapshot(allTimeoutKeys()));
