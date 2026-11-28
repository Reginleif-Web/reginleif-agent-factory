import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { test } from 'node:test';
import { z } from 'zod';
import {
  blockKinds,
  decisionKinds,
  taskStates,
  taskTransitions,
  blockedKindTargets,
  phaseTwoBlockKinds,
  roboticsBlockKinds,
  selectTransitionsForMigration,
  selectBlockTargetsForMigration,
  selectTransitionDataForMigration,
  canTransition,
} from '../contracts/states.ts';
import {
  contractSchemas,
  createTaskRequestSchema,
  decisionRequestSchemas,
  executorReportSchema,
  artifactUploadRequestSchema,
  childTaskProposalRequestSchema,
  physicalDecisionRequestSchema,
  problemDetailsSchema,
  taskDecisionResponseSchema,
  taskRevisionRequestSchema,
} from '../contracts/schemas/index.ts';

test('task states match the database enum exactly', async () => {
  const database = await readFile(new URL('../../docs/agent-factory/database.md', import.meta.url), 'utf8');
  const taskRow = database.match(/\| `tasks` \|[^\n]+/u)?.[0];
  assert.ok(taskRow, 'database.md:42 defines the task status enum');
  const documented = taskRow.match(/status` enum \(the 16 states of the architecture: ([^)]+)\)/u)?.[1];
  assert.ok(documented);
  assert.deepEqual(taskStates, documented.split(', '));
  assert.equal(taskStates.length, 16);
});

test('transition data covers the documented task transition rows', () => {
  // database.md:327-333 requires the architecture transitions plus kind-specific BLOCKED exits.
  // architecture.md:149-165 and temporal.md:332-343 provide the concrete rows and block targets.
  assert.equal(taskTransitions.length, 71);
  for (const row of [
    { from_status: 'ASSIGNED', to_status: 'BLOCKED' },
    { from_status: 'ASSIGNED', to_status: 'FAILED' },
    { from_status: 'CREATED', to_status: 'PLANNING' },
    { from_status: 'RUNNING', to_status: 'ASSIGNED' },
    { from_status: 'CANCELLING', to_status: 'CANCELLED' },
    { from_status: 'BLOCKED', to_status: 'PLANNING' },
    { from_status: 'BLOCKED', to_status: 'READY' },
    { from_status: 'BLOCKED', to_status: 'VERIFYING' },
    { from_status: 'BLOCKED', to_status: 'ASSIGNED' },
  ]) {
    assert.ok(taskTransitions.some((transition) => transition.from_status === row.from_status && transition.to_status === row.to_status));
  }
  assert.equal(new Set(taskTransitions.map(({ from_status, to_status }) => `${from_status}:${to_status}`)).size, taskTransitions.length);
});

test('each BLOCKED kind target is an allowed task_transitions pair', () => {
  for (const rule of blockedKindTargets) {
    assert.ok(taskTransitions.some((transition) => transition.from_status === 'BLOCKED' && transition.to_status === rule.to_status));
  }
});

test('migration selectors keep robotics block kinds out of migration 0002', () => {
  const migrationTwoPairs = selectTransitionsForMigration('0002');
  const migrationTwoBlockTargets = selectBlockTargetsForMigration('0002');
  const roboticsBlockTargets = selectBlockTargetsForMigration('robotics');
  const migrationTwoData = selectTransitionDataForMigration('0002');
  const roboticsData = selectTransitionDataForMigration('robotics');

  assert.equal(migrationTwoPairs.length + selectTransitionsForMigration('0011').length, taskTransitions.length);
  assert.equal(migrationTwoBlockTargets.length, 7);
  assert.equal(roboticsBlockTargets.length, 4);
  assert.deepEqual(migrationTwoData, { taskTransitions: migrationTwoPairs, blockKindTargets: migrationTwoBlockTargets });
  assert.deepEqual(roboticsData.blockKindTargets, roboticsBlockTargets);
  assert.equal(roboticsData.taskTransitions.length, 0);
  assert.ok(migrationTwoPairs.every((row) => row.introducedBy === '0002'
    && taskStates.includes(row.from_status) && taskStates.includes(row.to_status)));
  assert.ok(migrationTwoBlockTargets.every((row) => row.introducedBy === '0002'
    && phaseTwoBlockKinds.some((kind) => kind === row.kind)
    && taskStates.includes(row.to_status)));
  assert.ok(roboticsBlockTargets.every((row) => row.introducedBy === 'robotics'));
  assert.ok(roboticsBlockTargets.every((row) => roboticsBlockKinds.some((kind) => kind === row.kind)));
});

test('canTransition is a pure lookup and checks BLOCKED kinds', () => {
  assert.equal(canTransition('CREATED', 'PLANNING'), true);
  assert.equal(canTransition('CREATED', 'ACCEPTED'), false);
  assert.equal(canTransition('BLOCKED', 'READY', 'dependency'), true);
  assert.equal(canTransition('BLOCKED', 'READY', 'execution'), false);
  assert.equal(canTransition('BLOCKED', 'READY'), false);
  assert.equal(canTransition('BLOCKED', 'ASSIGNED', 'physical_unknown'), true);
});

test('the shared state and decision enums include documented values once', () => {
  assert.equal(blockKinds.length, 11);
  assert.equal(decisionKinds.length, 13);
  assert.ok(decisionKinds.includes('physical_decision'));
});

test('every exported contract schema converts to JSON Schema', () => {
  assert.ok(Object.keys(contractSchemas).length > 0);
  for (const schema of Object.values(contractSchemas)) {
    assert.equal(typeof z.toJSONSchema(schema), 'object');
  }
});

test('task creation and revision request schemas accept documented bodies and reject invalid bodies', () => {
  const task = {
    project_id: '10000000-0000-4000-8000-000000000001',
    title: 'Implement contract tests',
    description: 'Add API contracts',
    priority: 2,
    requirements: 'Use typed schemas',
    acceptance_criteria: [{ id: 'tests-pass' }],
    constraints: {},
    depends_on: [],
  };
  assert.equal(createTaskRequestSchema.safeParse(task).success, true);
  assert.equal(createTaskRequestSchema.safeParse({ ...task, surprise: true }).success, false);
  assert.equal(createTaskRequestSchema.safeParse({ ...task, priority: 'high' }).success, false);

  const revision = {
    requirements: 'Update requirements',
    acceptance_criteria: [{ id: 'new-check' }],
    constraints: {},
    change_reason: 'Owner clarification',
    revision_number: 1,
  };
  assert.equal(taskRevisionRequestSchema.safeParse(revision).success, true);
  assert.equal(taskRevisionRequestSchema.safeParse({ ...revision, change_reason: '' }).success, false);
  assert.equal(taskRevisionRequestSchema.safeParse({ ...revision, revision_number: 0 }).success, false);
});

test('decision request schemas validate representative owner decisions', () => {
  assert.equal(decisionRequestSchemas.approve.safeParse({ revision_number: 1, comment: 'Looks good' }).success, true);
  assert.equal(decisionRequestSchemas.reject.safeParse({ revision_number: 1, reason: 'Does not meet criteria' }).success, true);
  assert.equal(decisionRequestSchemas.request_changes.safeParse({ revision_number: 1, findings: [], comment: '' }).success, true);
  assert.equal(decisionRequestSchemas.resolve_block.safeParse({ block_kind: 'dependency', resume_to: 'READY', revision_number: 1 }).success, true);
  assert.equal(decisionRequestSchemas.resolve_block.safeParse({ block_kind: 'execution', choice: 'call_owner', reason: 'Need a decision' }).success, true);
  assert.equal(decisionRequestSchemas.approve.safeParse({ revision_number: -1 }).success, false);
  assert.equal(decisionRequestSchemas.cancel.safeParse({ unexpected: true }).success, false);
  assert.equal(decisionRequestSchemas.resolve_block.safeParse({ block_kind: 'scope' }).success, false);
});

test('executor report and decision response schemas validate documented reports', () => {
  const report = {
    actions: ['implemented schema validation'],
    changedFiles: ['factory/contracts/schemas/index.ts'],
    commits: [],
    testResults: [{ id: 'contracts', command: 'node --test', exitCode: 0, status: 'passed', log: 'artifact://test-log' }],
    artifacts: [],
    problems: [],
    uncheckedScenarios: [],
    requirementDeviations: [],
    finalStatus: 'done',
  };
  assert.equal(executorReportSchema.safeParse(report).success, true);
  assert.equal(executorReportSchema.safeParse({ ...report, finalStatus: 'unknown' }).success, false);
  assert.equal(executorReportSchema.safeParse({ ...report, unexpected: true }).success, false);

  assert.equal(taskDecisionResponseSchema.safeParse({ id: '10000000-0000-4000-8000-000000000002', kind: 'approve', delivery_state: 'recorded' }).success, true);
  assert.equal(taskDecisionResponseSchema.safeParse({ id: 'bad', kind: 'approve', delivery_state: 'recorded' }).success, false);
});

test('problem details follow the API error body shape', () => {
  assert.equal(problemDetailsSchema.safeParse({
    type: 'about:blank', title: 'Invalid request', status: 422, code: 'validation_failed', detail: 'Body is invalid',
    request_id: '10000000-0000-4000-8000-000000000003', errors: [{ pointer: '/priority', code: 'invalid_type', message: 'Expected number' }],
  }).success, true);
  assert.equal(problemDetailsSchema.safeParse({ title: 'Invalid request', status: 422, code: 'validation_failed' }).success, false);
});

test('worker artifact, child proposal, and physical decision bodies use the documented names', () => {
  const artifact = {
    artifact_type: 'report', size: 128, checksum: 'a'.repeat(64),
    client_upload_id: '10000000-0000-4000-8000-000000000004', metadata: {},
  };
  assert.equal(artifactUploadRequestSchema.safeParse(artifact).success, true);
  assert.equal(artifactUploadRequestSchema.safeParse({ ...artifact, size: -1 }).success, false);

  assert.equal(childTaskProposalRequestSchema.safeParse({ title: 'Investigate', requirements: 'Check the issue', kind: 'decomposition' }).success, true);
  assert.equal(childTaskProposalRequestSchema.safeParse({ title: 'Investigate', requirements: '', kind: 'other' }).success, false);

  assert.equal(physicalDecisionRequestSchema.safeParse({ platform: 'robot-platform-1', decision: { action: 'assign' } }).success, true);
  assert.equal(physicalDecisionRequestSchema.safeParse({ platform: '', decision: {} }).success, false);
});
