// The contract version the server speaks (the Worker contract, api.md). One constant: the API defaults and the workflow
// state both read it. Workflow code imports this file, so it stays a plain constant with no imports of any kind.
// 2: the run result (CollectResults) carries a typed outcome (died, limit, question, unclear, review, settled) besides
// collected and schemaFinding. A worker on 1 stays supported (contract-range.ts: current and current-1).
export const CONTRACT_VERSION = 2;
