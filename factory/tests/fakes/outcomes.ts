import { CollectOutcome, SAMPLE_OUTPUTS, type CollectOutcomeValue } from "./contracts.ts";

/**
 * Sample CollectResults outcomes for the scripted fake: one helper per typed outcome of the machine run contract (contract 2).
 * Each result is parsed by the contract, so a helper that drifts from the schema fails where it is built, not in the workflow.
 */
type Collected = Extract<CollectOutcomeValue, { status: "collected" }>;
type Report = Collected["report"];
type Finding = Extract<CollectOutcomeValue, { status: "review" }>["findings"][number];
type SettledOutcome = Extract<CollectOutcomeValue, { status: "settled" }>["outcomes"][number];

const checked = (value: CollectOutcomeValue): CollectOutcomeValue => CollectOutcome.parse(value);

/** A finished run's report; `over` changes single report fields (finalStatus, findingAnswers). */
export const collected = (over: Partial<Report> = {}): CollectOutcomeValue =>
	checked({ status: "collected", report: { ...(SAMPLE_OUTPUTS.CollectResults as Collected).report, ...over } });

export const reviewPass = (): CollectOutcomeValue => checked({ status: "review", verdict: "pass", findings: [] });

export const reviewFail = (findings: Finding[]): CollectOutcomeValue => checked({ status: "review", verdict: "fail", findings });

export const reviewInconclusive = (): CollectOutcomeValue => checked({ status: "review", verdict: "inconclusive", findings: [] });

export const settled = (outcomes: SettledOutcome[]): CollectOutcomeValue => checked({ status: "settled", outcomes });

export const limited = (waitSeconds: number | null): CollectOutcomeValue => checked({ status: "limit", waitSeconds });

export const questioned = (question = "which database should the task use"): CollectOutcomeValue =>
	checked({ status: "question", question, inquiryId: "inquiry-1", tokenId: "token-1", sessionRef: "session-1" });

export const died = (reason = "the agent process ended without a report"): CollectOutcomeValue => checked({ status: "died", reason });

export const unclear = (reason = "the run ended and the worker cannot tell how"): CollectOutcomeValue => checked({ status: "unclear", reason });
