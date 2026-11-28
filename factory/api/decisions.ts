import { createHash, randomUUID } from 'node:crypto';
import { confirmationId as checkedConfirmationId, issueConfirmation, verifyConfirmation } from './second-press.ts';
import type { Repo } from '../db/repo/index.ts';
import { databaseRefusal } from '../db/repo/errors.ts';
import type { AuditPrincipal } from '../db/repo/audit.ts';
import { decisionRequestSchemas } from '../contracts/schemas/index.ts';
import { routes as contractRoutes } from '../contracts/routes.ts';

export type DecisionKind = keyof typeof decisionRequestSchemas;
export const decisionKinds = Object.keys(decisionRequestSchemas) as DecisionKind[];
type Channel = 'client' | 'telegram' | 'api';
export type DecisionInput = {
  taskId: string; kind: DecisionKind; channel: Channel; body: Record<string, unknown>;
  idempotencyKey: string; actorId?: string; engineerRunId?: string;
  deviceSessionId?: string | null; pressId?: string | null; confirm?: string; now?: Date;
  /** The actual request path, when it differs from the standard task decision route. */
  path?: string;
  wireBody?: unknown;
  http?: { requestId: string; principal: AuditPrincipal | null; workerId?: string };
  poll?: { id: string; answerKey: string };
};
export type PollResult = { answered_via: string | null; answered_at: string | null };
export class DecisionProblem extends Error {
  code: string;
  status: number;
  currentRowVersion: number | undefined;
  readonly pollResult?: PollResult;
  /** `detail` is the current row version (a number) or the closed poll's result (an object). */
  constructor(code: string, status = 409, detail?: number | PollResult) {
    super(code); this.code = code; this.status = status;
    if (typeof detail === 'number') this.currentRowVersion = detail; else this.pollResult = detail;
  }
}
type Recorded = { id: string; kind: string; delivery_state: string; refusal_reason: string | null };
type ConfirmationRequired = { code: 'confirmation_required'; confirmation_id: string; nonce: string; expires_at: string };
export type DecisionResult = { status: 202; body: Recorded; location: string; replayed: boolean } | { status: 409; body: ConfirmationRequired; replayed: boolean };
export const hash = (value: string) => createHash('sha256').update(value).digest('hex');
const canonical = (value: unknown): string => {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  return `{${Object.entries(value).sort(([a], [b]) => a.localeCompare(b)).map(([k, v]) => `${JSON.stringify(k)}:${canonical(v)}`).join(',')}}`;
};
export const requestFingerprint = (method: string, path: string, body: unknown) => hash(canonical([method, path, body ?? null]));
const routes: Record<DecisionKind, string> = { approve: 'approve', reject: 'reject', request_changes: 'request-changes', cancel: 'cancel', pause: 'pause', resume: 'resume', add_feedback: 'feedback', change_priority: 'priority', revise_requirements: 'revisions', resolve_block: 'resolve-block', force_close: 'force-close', allow_agent: 'allow-agent' };
const finalStates = new Set(['ACCEPTED', 'REJECTED', 'CANCELLED', 'FAILED']);
export const mutationHash = (method: string, path: string, body: unknown) => hash(canonical([method, path, body]));
const answer = (row: { id: string; kind: string | null; deliveryState: string | null; refusalReason: string | null }, replayed: boolean): DecisionResult => ({ status: 202, body: { id: row.id, kind: row.kind!, delivery_state: row.deliveryState!, refusal_reason: row.refusalReason }, location: `/decisions/${row.id}`, replayed });

export async function recordDecision(repo: Repo, input: DecisionInput): Promise<DecisionResult> {
  if (!/^[A-Za-z0-9_-]{8,128}$/.test(input.idempotencyKey) || !decisionRequestSchemas[input.kind]?.safeParse(input.body).success || Boolean(input.actorId) === Boolean(input.engineerRunId)) throw new DecisionProblem('validation_failed', 422);
  const path = input.path ?? `/tasks/${input.taskId}/${routes[input.kind]}`;
  const pollBody = input.poll ? { answer_key: input.poll.answerKey } : undefined;
  const requestHash = requestFingerprint('POST', path, pollBody ?? input.wireBody ?? input.body);
  const confirmationHash = requestFingerprint('POST', path, pollBody ?? input.body);
  const now = input.now ?? new Date();
  const run = async (): Promise<DecisionResult> => repo.transaction(async tx => {
    const principal = input.actorId ?? input.engineerRunId!;
    if (!input.http) await tx.decisions.lockKey(principal, input.idempotencyKey);
    else if (!await tx.decisions.tryLockKey(principal, input.idempotencyKey)) throw new DecisionProblem('idempotency_in_progress');
    const audit = async (outcome: 'ok' | 'needs_confirmation') => {
      if (!input.http) return;
      await tx.audit.append({ id: randomUUID(), actorId: principal, requestId: input.http.requestId,
        principal: input.http.principal, targetType: 'task', targetId: input.taskId,
        action: `POST ${path}`, requestHash, outcome });
    };
    const existing = input.actorId ? await tx.decisions.byActorKey(input.actorId, input.idempotencyKey) : await tx.decisions.byEngineerKey(input.engineerRunId!, input.idempotencyKey);
    if (existing) {
      if (existing.requestHash !== requestHash) throw new DecisionProblem('idempotency_conflict');
      await audit('ok');
      return answer(existing, true);
    }
    const key = input.actorId ? await tx.idempotencyKeys.get(input.actorId, input.idempotencyKey) : undefined;
    if (key) {
      if (key.requestHash !== requestHash) throw new DecisionProblem('idempotency_conflict');
      if (key.state !== 'done') throw new DecisionProblem('idempotency_in_progress');
      const first = key.responseBody as ConfirmationRequired | null;
      if (first?.code !== 'confirmation_required' || key.responseStatus !== 409) throw new DecisionProblem('idempotency_conflict');
      if (input.confirm === undefined) {
        await audit('needs_confirmation');
        return { status: 409, body: first, replayed: true };
      }
    }
    const task = input.http || input.kind === 'resolve_block' ? await tx.tasks.get(input.taskId, { lock: true }) : undefined;
    if ((input.http || input.kind === 'resolve_block') && !task) throw new DecisionProblem('not_found', 404);
    if (task?.originType === 'standing') throw new DecisionProblem('not_allowed_in_state');
    const callOwner = input.kind === 'resolve_block' && input.body.choice === 'call_owner';
    if (callOwner && !input.engineerRunId) throw new DecisionProblem('forbidden', 403);
    if (input.kind === 'resolve_block' && task) {
      if (input.engineerRunId) {
        if (task.blockHandler !== 'chief_engineer') throw new DecisionProblem('block_handled');
        if (input.http?.workerId && !await tx.taskReads.engineerRun(task.id, input.http.workerId, input.engineerRunId)) throw new DecisionProblem('forbidden', 403);
      } else if (task.status !== 'BLOCKED' && !finalStates.has(task.status ?? '') && (await tx.decisions.listForTask(task.id)).some(row =>
        row.kind === 'resolve_block' && row.deliveryState === 'applied' && row.revisionNumber === input.body.revision_number &&
        (row.payload as { block_kind?: unknown } | null)?.block_kind === input.body.block_kind)) {
        throw new DecisionProblem('block_handled');
      }
    }
    const revisionNumber = typeof input.body.revision_number === 'number' ? input.body.revision_number : callOwner ? task?.currentRevision ?? 1 : null;
    const selectedPoll = input.poll && await tx.polls.get(input.poll.id);
    const decisionPolls = selectedPoll ? [selectedPoll] : await tx.polls.forDecision(input.taskId, revisionNumber, input.kind);
    const closed = (poll: (typeof decisionPolls)[number]) => new DecisionProblem('poll_closed', 409, { answered_via: poll.answeredVia, answered_at: poll.answeredAt?.toISOString() ?? null });
    if (input.poll && (!selectedPoll || selectedPoll.arbiter !== 'api' || selectedPoll.taskId !== input.taskId)) throw new DecisionProblem('not_found', 404);
    if (decisionPolls.length !== 0 && decisionPolls.every(poll => poll.state !== 'open')) throw closed(decisionPolls[0]!);
    let confirmationId: string | undefined;
    if (contractRoutes.find(route => route.method === 'POST' && route.path === `/tasks/{id}/${routes[input.kind]}`)?.secondPress === 'always') {
      if (!input.actorId) throw new DecisionProblem('owner_only', 422);
      if (input.confirm === undefined) {
        const claim = await tx.idempotencyKeys.claim({ actorId: input.actorId, key: input.idempotencyKey, requestHash, state: 'in_progress' });
        if (claim.replayed) {
          if (claim.row.requestHash !== requestHash) throw new DecisionProblem('idempotency_conflict');
          if (claim.row.state !== 'done') throw new DecisionProblem('idempotency_in_progress');
          return { status: 409, body: claim.row.responseBody as ConfirmationRequired, replayed: true };
        }
        const body = await issueConfirmation(tx, { actorId: input.actorId, channel: input.channel, deviceSessionId: input.deviceSessionId ?? null, operation: input.kind, targetType: 'task', targetId: input.taskId, requestHash: confirmationHash }, now);
        await tx.idempotencyKeys.complete(input.actorId, input.idempotencyKey, 409, body);
        await audit('needs_confirmation');
        return { status: 409, body, replayed: false };
      }
      confirmationId = await checkedConfirmationId(tx, input.confirm);
      if (!confirmationId) throw new DecisionProblem('confirmation_invalid');
    }
    const decisionId = randomUUID(), outboxId = randomUUID();
    const result = await tx.decisions.insertPair({ decision: { id: decisionId, taskId: input.taskId, kind: input.kind, actorId: input.actorId ?? null, engineerRunId: input.engineerRunId ?? null, channel: input.channel, deviceSessionId: input.deviceSessionId ?? null, pressId: input.pressId ?? null, revisionNumber, payload: input.body, confirmationId: confirmationId ?? null, idempotencyKey: input.idempotencyKey, requestHash, deliveryState: 'recorded', outboxId }, outbox: { id: outboxId, kind: 'deliver_decision', taskId: input.taskId, decisionId, payload: { decision_id: decisionId }, state: 'pending', attempts: 0 } });
    for (const poll of decisionPolls.filter(row => row.state === 'open').sort((a, b) => a.id.localeCompare(b.id))) {
      const updated = await tx.polls.answer(poll.id, { answerKey: input.poll?.answerKey ?? input.kind, answeredVia: input.channel, answeredSessionId: input.deviceSessionId ?? null, arbiter: 'api', decisionId, now });
      if (!updated) throw closed(await tx.polls.get(poll.id) ?? poll);
    }
    if (confirmationId && !await verifyConfirmation(tx, input.confirm!, { actorId: input.actorId!, channel: input.channel, deviceSessionId: input.deviceSessionId ?? null, operation: input.kind, targetType: 'task', targetId: input.taskId, requestHash: confirmationHash }, now, decisionId)) throw new DecisionProblem('confirmation_invalid');
    if (input.kind === 'revise_requirements') {
      const revision = decisionRequestSchemas.revise_requirements.parse(input.body);
      if (!await tx.tasks.appendRevision(input.taskId, { id: randomUUID(), decisionId, authorId: input.actorId,
        requirements: revision.requirements, acceptanceCriteria: revision.acceptance_criteria,
        constraints: revision.constraints, changeReason: revision.change_reason })) throw new DecisionProblem('not_found', 404);
    }
    if (input.http) await tx.taskEvents.persist({ id: randomUUID(), taskId: input.taskId, correlationId: input.http.requestId,
      // Not `decision:<id>`: the workflow's decision_<kind> event owns that key, and a shared key made its persist a no-op (no transition event, no projection).
      dedupeKey: `decision-recorded:${decisionId}`, actorType: input.engineerRunId ? 'worker' : 'owner', actorId: input.actorId ?? null,
      runId: input.engineerRunId, eventType: 'decision_recorded', payload: { decision_id: decisionId, kind: input.kind } });
    await audit('ok');
    return answer(result.decision, false);
  });
  try { return await run(); } catch (error) {
    const refusal = databaseRefusal(error);
    if (refusal?.reason === 'invalid standing task operation') throw new DecisionProblem('not_allowed_in_state');
    if (refusal?.reason === 'engineer decision requires chief engineer block') throw new DecisionProblem('block_handled');
    for (let cause: any = error, depth = 0; cause && depth < 8; cause = cause.cause, depth++) {
      if (cause.code === '23505' && (cause.constraint_name === 'task_decisions_press' || cause.constraint === 'task_decisions_press')) throw new DecisionProblem('press_replayed');
    }
    throw error;
  }
}
