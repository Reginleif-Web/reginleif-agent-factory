export const taskStates = [
  'CREATED',
  'PLANNING',
  'READY',
  'WAITING_DEPENDENCIES',
  'ASSIGNED',
  'RUNNING',
  'VERIFYING',
  'AWAITING_APPROVAL',
  'REWORK_REQUIRED',
  'BLOCKED',
  'ACCEPTED',
  'REJECTED',
  'CANCELLED',
  'FAILED',
  'PAUSED',
  'CANCELLING',
] as const;

export type TaskState = (typeof taskStates)[number];

export const phaseTwoBlockKinds = [
  'scope',
  'dependency',
  'execution',
  'usage_limit',
  'agent_question',
  'check_environment',
  'rework_limit',
] as const;

export const roboticsBlockKinds = [
  'safe_stop',
  'physical_unknown',
  'maintenance',
  'quarantine',
] as const;

export const blockKinds = [...phaseTwoBlockKinds, ...roboticsBlockKinds] as const;

export type BlockKind = (typeof blockKinds)[number];

export const decisionKinds = [
  'approve',
  'reject',
  'request_changes',
  'cancel',
  'pause',
  'resume',
  'revise_requirements',
  'add_feedback',
  'change_priority',
  'resolve_block',
  'force_close',
  'allow_agent',
  'physical_decision',
] as const;

export type DecisionKind = (typeof decisionKinds)[number];

export const decisionChannels = ['client', 'telegram', 'api'] as const;
export type DecisionChannel = (typeof decisionChannels)[number];

export const deliveryStates = ['recorded', 'delivering', 'applied', 'refused'] as const;
export type DeliveryState = (typeof deliveryStates)[number];

export const executorReportStatuses = ['done', 'partial', 'failed'] as const;
export type ExecutorReportStatus = (typeof executorReportStatuses)[number];

export const proposalKinds = ['decomposition', 'agent_proposal'] as const;
export type ProposalKind = (typeof proposalKinds)[number];

export type MigrationKey = '0002' | '0011' | 'robotics';

export type TransitionRow = {
  readonly from_status: TaskState;
  readonly to_status: TaskState;
  readonly introducedBy: MigrationKey;
};

export const taskTransitions = [
  { from_status: 'CREATED', to_status: 'PLANNING', introducedBy: '0002' },
  { from_status: 'PLANNING', to_status: 'READY', introducedBy: '0002' },
  { from_status: 'PLANNING', to_status: 'BLOCKED', introducedBy: '0002' },
  { from_status: 'PLANNING', to_status: 'FAILED', introducedBy: '0002' },
  { from_status: 'READY', to_status: 'WAITING_DEPENDENCIES', introducedBy: '0002' },
  { from_status: 'READY', to_status: 'ASSIGNED', introducedBy: '0002' },
  { from_status: 'WAITING_DEPENDENCIES', to_status: 'ASSIGNED', introducedBy: '0002' },
  { from_status: 'WAITING_DEPENDENCIES', to_status: 'BLOCKED', introducedBy: '0002' },
  { from_status: 'ASSIGNED', to_status: 'RUNNING', introducedBy: '0002' },
  { from_status: 'RUNNING', to_status: 'VERIFYING', introducedBy: '0002' },
  { from_status: 'RUNNING', to_status: 'BLOCKED', introducedBy: '0002' },
  { from_status: 'RUNNING', to_status: 'FAILED', introducedBy: '0002' },
  { from_status: 'RUNNING', to_status: 'ASSIGNED', introducedBy: '0002' },
  { from_status: 'VERIFYING', to_status: 'AWAITING_APPROVAL', introducedBy: '0002' },
  { from_status: 'VERIFYING', to_status: 'REWORK_REQUIRED', introducedBy: '0002' },
  { from_status: 'VERIFYING', to_status: 'BLOCKED', introducedBy: '0002' },
  { from_status: 'VERIFYING', to_status: 'FAILED', introducedBy: '0002' },
  { from_status: 'AWAITING_APPROVAL', to_status: 'ACCEPTED', introducedBy: '0002' },
  { from_status: 'AWAITING_APPROVAL', to_status: 'REWORK_REQUIRED', introducedBy: '0002' },
  { from_status: 'AWAITING_APPROVAL', to_status: 'REJECTED', introducedBy: '0002' },
  { from_status: 'REWORK_REQUIRED', to_status: 'ASSIGNED', introducedBy: '0002' },
  { from_status: 'CANCELLING', to_status: 'CANCELLED', introducedBy: '0002' },
  { from_status: 'BLOCKED', to_status: 'PLANNING', introducedBy: '0002' },
  { from_status: 'BLOCKED', to_status: 'READY', introducedBy: '0002' },
  { from_status: 'BLOCKED', to_status: 'ASSIGNED', introducedBy: '0002' },
  { from_status: 'BLOCKED', to_status: 'VERIFYING', introducedBy: '0002' },
  { from_status: 'BLOCKED', to_status: 'REWORK_REQUIRED', introducedBy: '0002' },
  { from_status: 'CREATED', to_status: 'CANCELLED', introducedBy: '0002' },
  { from_status: 'CREATED', to_status: 'CANCELLING', introducedBy: '0002' },
  { from_status: 'PLANNING', to_status: 'CANCELLED', introducedBy: '0002' },
  { from_status: 'PLANNING', to_status: 'CANCELLING', introducedBy: '0002' },
  { from_status: 'READY', to_status: 'CANCELLED', introducedBy: '0002' },
  { from_status: 'READY', to_status: 'CANCELLING', introducedBy: '0002' },
  { from_status: 'WAITING_DEPENDENCIES', to_status: 'CANCELLED', introducedBy: '0002' },
  { from_status: 'WAITING_DEPENDENCIES', to_status: 'CANCELLING', introducedBy: '0002' },
  { from_status: 'ASSIGNED', to_status: 'CANCELLED', introducedBy: '0002' },
  { from_status: 'ASSIGNED', to_status: 'CANCELLING', introducedBy: '0002' },
  { from_status: 'RUNNING', to_status: 'CANCELLED', introducedBy: '0002' },
  { from_status: 'RUNNING', to_status: 'CANCELLING', introducedBy: '0002' },
  { from_status: 'VERIFYING', to_status: 'CANCELLED', introducedBy: '0002' },
  { from_status: 'VERIFYING', to_status: 'CANCELLING', introducedBy: '0002' },
  { from_status: 'AWAITING_APPROVAL', to_status: 'CANCELLED', introducedBy: '0002' },
  { from_status: 'AWAITING_APPROVAL', to_status: 'CANCELLING', introducedBy: '0002' },
  { from_status: 'REWORK_REQUIRED', to_status: 'CANCELLED', introducedBy: '0002' },
  { from_status: 'REWORK_REQUIRED', to_status: 'CANCELLING', introducedBy: '0002' },
  { from_status: 'BLOCKED', to_status: 'CANCELLED', introducedBy: '0002' },
  { from_status: 'BLOCKED', to_status: 'CANCELLING', introducedBy: '0002' },
  { from_status: 'CREATED', to_status: 'PAUSED', introducedBy: '0002' },
  { from_status: 'PLANNING', to_status: 'PAUSED', introducedBy: '0002' },
  { from_status: 'READY', to_status: 'PAUSED', introducedBy: '0002' },
  { from_status: 'WAITING_DEPENDENCIES', to_status: 'PAUSED', introducedBy: '0002' },
  { from_status: 'ASSIGNED', to_status: 'PAUSED', introducedBy: '0002' },
  { from_status: 'RUNNING', to_status: 'PAUSED', introducedBy: '0002' },
  { from_status: 'VERIFYING', to_status: 'PAUSED', introducedBy: '0002' },
  { from_status: 'AWAITING_APPROVAL', to_status: 'PAUSED', introducedBy: '0002' },
  { from_status: 'REWORK_REQUIRED', to_status: 'PAUSED', introducedBy: '0002' },
  { from_status: 'BLOCKED', to_status: 'PAUSED', introducedBy: '0002' },
  { from_status: 'PAUSED', to_status: 'CREATED', introducedBy: '0002' },
  { from_status: 'PAUSED', to_status: 'PLANNING', introducedBy: '0002' },
  { from_status: 'PAUSED', to_status: 'READY', introducedBy: '0002' },
  { from_status: 'PAUSED', to_status: 'WAITING_DEPENDENCIES', introducedBy: '0002' },
  { from_status: 'PAUSED', to_status: 'ASSIGNED', introducedBy: '0002' },
  { from_status: 'PAUSED', to_status: 'RUNNING', introducedBy: '0002' },
  { from_status: 'PAUSED', to_status: 'VERIFYING', introducedBy: '0002' },
  { from_status: 'PAUSED', to_status: 'AWAITING_APPROVAL', introducedBy: '0002' },
  { from_status: 'PAUSED', to_status: 'REWORK_REQUIRED', introducedBy: '0002' },
  { from_status: 'PAUSED', to_status: 'BLOCKED', introducedBy: '0002' },
  { from_status: 'PAUSED', to_status: 'CANCELLED', introducedBy: '0002' },
  { from_status: 'PAUSED', to_status: 'CANCELLING', introducedBy: '0002' },
  // 0011: ASSIGNED had no exit but RUNNING, so a run that could not be prepared had nowhere to go (state-exit rule).
  { from_status: 'ASSIGNED', to_status: 'BLOCKED', introducedBy: '0011' },
  { from_status: 'ASSIGNED', to_status: 'FAILED', introducedBy: '0011' },
] as const satisfies readonly TransitionRow[];

export type BlockTargetRule = {
  readonly kind: BlockKind;
  readonly to_status: TaskState;
  readonly introducedBy: MigrationKey;
};

export const blockedKindTargets = [
  { kind: 'scope', to_status: 'PLANNING', introducedBy: '0002' },
  { kind: 'dependency', to_status: 'READY', introducedBy: '0002' },
  { kind: 'execution', to_status: 'ASSIGNED', introducedBy: '0002' },
  { kind: 'usage_limit', to_status: 'ASSIGNED', introducedBy: '0002' },
  { kind: 'agent_question', to_status: 'ASSIGNED', introducedBy: '0002' },
  { kind: 'check_environment', to_status: 'VERIFYING', introducedBy: '0002' },
  { kind: 'rework_limit', to_status: 'REWORK_REQUIRED', introducedBy: '0002' },
  { kind: 'safe_stop', to_status: 'ASSIGNED', introducedBy: 'robotics' },
  { kind: 'physical_unknown', to_status: 'ASSIGNED', introducedBy: 'robotics' },
  { kind: 'maintenance', to_status: 'ASSIGNED', introducedBy: 'robotics' },
  { kind: 'quarantine', to_status: 'ASSIGNED', introducedBy: 'robotics' },
] as const satisfies readonly BlockTargetRule[];

// Exits that leave BLOCKED whatever its kind (cancel, pause, revise); only the progress exit is decided by the kind (0011).
export const blockedFreeExits: readonly TaskState[] = ['CANCELLED', 'CANCELLING', 'PAUSED', 'PLANNING'];

export function selectTransitionsForMigration(migration: MigrationKey): readonly TransitionRow[] {
  return taskTransitions.filter((transition) => transition.introducedBy === migration);
}

export function selectBlockTargetsForMigration(migration: MigrationKey): readonly BlockTargetRule[] {
  return blockedKindTargets.filter((rule) => rule.introducedBy === migration);
}

export function selectTransitionDataForMigration(migration: MigrationKey) {
  return {
    taskTransitions: selectTransitionsForMigration(migration),
    blockKindTargets: selectBlockTargetsForMigration(migration),
  };
}

export function canTransition(from: TaskState, to: TaskState, kind?: BlockKind): boolean {
  if (from === 'BLOCKED' && !blockedFreeExits.includes(to)) {
    return kind !== undefined
      && blockedKindTargets.some((rule) => rule.kind === kind && rule.to_status === to)
      && taskTransitions.some((transition) => transition.from_status === from && transition.to_status === to);
  }
  return taskTransitions.some((transition) => transition.from_status === from && transition.to_status === to);
}
