-- Migration 0011: every state has an exit (temporal.md "Every state has an exit").
-- 1. ASSIGNED could only move to RUNNING, CANCELLED, CANCELLING and PAUSED, so a run that could not be prepared (the machine
--    refused, the agent is not installed, a non-retryable error in preparation) left the task parked in ASSIGNED. Two
--    transitions are added: ASSIGNED -> BLOCKED (the run cannot be prepared; the block kinds are the existing ones) and
--    ASSIGNED -> FAILED (a non-retryable failure). task_transitions is immutable to UPDATE and DELETE, an INSERT by the
--    migrator is allowed.
-- 2. af_task_guard refused every move out of BLOCKED except the kind's own exit, so a blocked task could not be cancelled,
--    paused or revised although task_transitions lists BLOCKED -> CANCELLED, CANCELLING, PAUSED and PLANNING. The per-kind
--    check now decides only the progress exit (resolve); cancel, pause and revise leave BLOCKED whatever its kind.
-- No schema change and no new table. Hand-written data insert and function replacement.
INSERT INTO task_transitions (from_status, to_status) VALUES ('ASSIGNED', 'BLOCKED'), ('ASSIGNED', 'FAILED');
--> statement-breakpoint
CREATE OR REPLACE FUNCTION af_task_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_row tasks%ROWTYPE; limit_depth integer; project_deleted timestamptz;
BEGIN
 IF (NEW.block_kind IS NULL) <> (NEW.block_handler IS NULL) OR
    (NEW.status IS DISTINCT FROM 'BLOCKED' AND (NEW.block_kind IS NOT NULL OR NEW.block_handler IS NOT NULL)) THEN
   RAISE EXCEPTION 'block kind and handler require BLOCKED';
 END IF;
 IF NEW.project_id IS NOT NULL THEN
   SELECT deleted_at INTO project_deleted FROM projects WHERE id = NEW.project_id FOR UPDATE;
   IF project_deleted IS NOT NULL AND NEW.deleted_at IS NULL AND NEW.origin_type IS DISTINCT FROM 'standing'
      AND (NEW.status IS NULL OR NEW.status NOT IN ('ACCEPTED','REJECTED','CANCELLED','FAILED')) THEN
     RAISE EXCEPTION 'project is deleted';
   END IF;
 END IF;
 IF NEW.parent_task_id IS NULL THEN NEW.depth := 0;
 ELSE
   SELECT * INTO parent_row FROM tasks WHERE id = NEW.parent_task_id FOR UPDATE;
   IF NOT FOUND THEN RAISE EXCEPTION 'parent task missing'; END IF;
   IF parent_row.project_id IS DISTINCT FROM NEW.project_id THEN RAISE EXCEPTION 'parent in different project'; END IF;
   NEW.depth := parent_row.depth + 1;
   SELECT max_child_depth INTO limit_depth FROM execution_policies
     WHERE project_id = NEW.project_id OR project_id IS NULL
     ORDER BY (project_id IS NOT NULL) DESC, version DESC NULLS LAST LIMIT 1;
   IF NEW.depth > limit_depth THEN RAISE EXCEPTION 'maximum child depth exceeded'; END IF;
 END IF;
 IF TG_OP = 'UPDATE' THEN
   IF OLD.status IS DISTINCT FROM NEW.status AND NOT EXISTS (SELECT 1 FROM task_transitions WHERE from_status = OLD.status AND to_status = NEW.status) THEN
     RAISE EXCEPTION 'illegal task transition';
   END IF;
   IF OLD.status = 'BLOCKED' AND NEW.status IS DISTINCT FROM 'BLOCKED'
      AND NEW.status NOT IN ('CANCELLED','CANCELLING','PAUSED','PLANNING')
      AND NEW.status IS DISTINCT FROM (CASE OLD.block_kind
       WHEN 'scope' THEN 'PLANNING' WHEN 'dependency' THEN 'READY' WHEN 'execution' THEN 'ASSIGNED'
       WHEN 'usage_limit' THEN 'ASSIGNED' WHEN 'agent_question' THEN 'ASSIGNED'
       WHEN 'check_environment' THEN 'VERIFYING' WHEN 'rework_limit' THEN 'REWORK_REQUIRED' END) THEN
     RAISE EXCEPTION 'invalid blocked target';
   END IF;
   IF OLD.status = 'BLOCKED' AND NEW.status = 'BLOCKED' AND
      (NEW.block_kind IS DISTINCT FROM OLD.block_kind OR (NEW.block_handler IS DISTINCT FROM OLD.block_handler AND NOT (OLD.block_handler = 'chief_engineer' AND NEW.block_handler = 'owner'))) THEN
     RAISE EXCEPTION 'invalid block handler change';
   END IF;
 END IF;
 IF NEW.status = 'BLOCKED' AND (TG_OP = 'INSERT' OR OLD.status IS DISTINCT FROM 'BLOCKED') AND NEW.block_kind IN ('scope','agent_question') AND NEW.block_handler IS DISTINCT FROM 'owner' THEN
   RAISE EXCEPTION 'block requires owner';
 END IF;
 IF NEW.deleted_at IS NOT NULL AND (NEW.origin_type = 'standing' OR NEW.status IS NULL OR NEW.status NOT IN ('ACCEPTED','REJECTED','CANCELLED','FAILED')) THEN RAISE EXCEPTION 'task cannot be deleted'; END IF;
 RETURN NEW;
END $$;
