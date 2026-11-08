-- Phase 3: cancellation is a terminal job state; attempt outcomes retain
-- the reason the running lease ended.
ALTER TABLE workflow_jobs DROP CONSTRAINT IF EXISTS workflow_jobs_state_check;
ALTER TABLE workflow_jobs ADD CONSTRAINT workflow_jobs_state_check
    CHECK (state IN ('queued','running','succeeded','dead','canceled'));
ALTER TABLE workflow_attempts DROP CONSTRAINT IF EXISTS workflow_attempts_outcome_check;
ALTER TABLE workflow_attempts ADD CONSTRAINT workflow_attempts_outcome_check
    CHECK (outcome IN ('succeeded','failed','expired','canceled'));
