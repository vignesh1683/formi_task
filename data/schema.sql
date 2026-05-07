-- VoiceBot Post-Call Processing — Database Schema
-- This schema represents the CURRENT state of the system.
-- Candidates should propose schema changes as part of their solution.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE leads (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    campaign_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    name VARCHAR(255),
    phone VARCHAR(50),
    email VARCHAR(255),
    stage VARCHAR(100) DEFAULT 'new',
    lead_data JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_leads_campaign ON leads(campaign_id);
CREATE INDEX idx_leads_customer ON leads(customer_id);

CREATE TABLE sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lead_id UUID NOT NULL REFERENCES leads(id),
    campaign_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    agent_id UUID NOT NULL,
    status VARCHAR(20) DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_sessions_lead ON sessions(lead_id);
CREATE INDEX idx_sessions_campaign ON sessions(campaign_id);

CREATE TABLE interactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID NOT NULL REFERENCES sessions(id),
    lead_id UUID NOT NULL REFERENCES leads(id),
    campaign_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    agent_id UUID NOT NULL,

    status VARCHAR(20) DEFAULT 'INITIATED',
    call_sid VARCHAR(255),
    call_provider VARCHAR(50) DEFAULT 'exotel',

    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    duration_seconds INTEGER,

    -- Transcript stored here: conversation_data->'transcript' is a JSON array
    -- of {"role": "agent"|"customer", "content": "..."}
    conversation_data JSONB DEFAULT '{}',

    -- Hot cache for dashboard. Contains extracted entities, analysis status,
    -- call_stage, and other dashboard-facing fields.
    -- Structure: {"entities": {...}, "call_stage": "...", "analysis_status": "..."}
    interaction_metadata JSONB DEFAULT '{}',

    recording_url TEXT,
    recording_s3_key VARCHAR(512),

    -- Current Celery task tracking (no workflow visibility)
    postcall_celery_task_id VARCHAR(255),

    retry_count INTEGER DEFAULT 0,
    error_log JSONB DEFAULT '[]',

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_interactions_session ON interactions(session_id);
CREATE INDEX idx_interactions_lead ON interactions(lead_id);
CREATE INDEX idx_interactions_campaign ON interactions(campaign_id);
CREATE INDEX idx_interactions_customer ON interactions(customer_id);
CREATE INDEX idx_interactions_call_sid ON interactions(call_sid);
CREATE INDEX idx_interactions_status ON interactions(status);

-- Seed data: sample interactions for testing
-- (Uses fixed UUIDs for reproducibility)

INSERT INTO leads (id, campaign_id, customer_id, name, phone, stage) VALUES
    ('a0000000-0000-0000-0000-000000000001', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'Rahul Sharma', '+919876543210', 'contacted'),
    ('a0000000-0000-0000-0000-000000000002', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'Priya Gupta', '+919876543211', 'new'),
    ('a0000000-0000-0000-0000-000000000003', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'Amit Verma', '+919876543212', 'contacted'),
    ('a0000000-0000-0000-0000-000000000004', 'c0000000-0000-0000-0000-000000000002', 'd0000000-0000-0000-0000-000000000002', 'Neha Patel', '+919876543213', 'new'),
    ('a0000000-0000-0000-0000-000000000005', 'c0000000-0000-0000-0000-000000000002', 'd0000000-0000-0000-0000-000000000002', 'Rajesh Kumar', '+919876543214', 'contacted');

INSERT INTO sessions (id, lead_id, campaign_id, customer_id, agent_id, status) VALUES
    ('b0000000-0000-0000-0000-000000000001', 'a0000000-0000-0000-0000-000000000001', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'e0000000-0000-0000-0000-000000000001', 'COMPLETED'),
    ('b0000000-0000-0000-0000-000000000002', 'a0000000-0000-0000-0000-000000000002', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'e0000000-0000-0000-0000-000000000001', 'COMPLETED'),
    ('b0000000-0000-0000-0000-000000000003', 'a0000000-0000-0000-0000-000000000003', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'e0000000-0000-0000-0000-000000000001', 'COMPLETED');

INSERT INTO interactions (id, session_id, lead_id, campaign_id, customer_id, agent_id, status, call_sid, duration_seconds, started_at, ended_at, conversation_data, interaction_metadata) VALUES
    (
        'f0000000-0000-0000-0000-000000000001',
        'b0000000-0000-0000-0000-000000000001',
        'a0000000-0000-0000-0000-000000000001',
        'c0000000-0000-0000-0000-000000000001',
        'd0000000-0000-0000-0000-000000000001',
        'e0000000-0000-0000-0000-000000000001',
        'ENDED',
        'exotel-call-001',
        180,
        NOW() - INTERVAL '10 minutes',
        NOW() - INTERVAL '7 minutes',
        '{"transcript": [{"role": "agent", "content": "Hello, am I speaking with Mr. Sharma?"}, {"role": "customer", "content": "Haan ji"}, {"role": "agent", "content": "I am calling from Cashify regarding your phone evaluation. Can we reschedule?"}, {"role": "customer", "content": "Tomorrow 3:30 PM works"}, {"role": "agent", "content": "Confirmed, our executive will visit tomorrow at 3:30 PM"}, {"role": "customer", "content": "Okay, confirmed. Bye."}]}',
        '{"analysis_status": "pending"}'
    ),
    (
        'f0000000-0000-0000-0000-000000000002',
        'b0000000-0000-0000-0000-000000000002',
        'a0000000-0000-0000-0000-000000000002',
        'c0000000-0000-0000-0000-000000000001',
        'd0000000-0000-0000-0000-000000000001',
        'e0000000-0000-0000-0000-000000000001',
        'ENDED',
        'exotel-call-002',
        45,
        NOW() - INTERVAL '15 minutes',
        NOW() - INTERVAL '14 minutes',
        '{"transcript": [{"role": "agent", "content": "Hello, am I speaking with Ms. Gupta?"}, {"role": "customer", "content": "Not interested, dont call again"}, {"role": "agent", "content": "Sorry for the inconvenience. Have a good day."}]}',
        '{"analysis_status": "pending"}'
    ),
    (
        'f0000000-0000-0000-0000-000000000003',
        'b0000000-0000-0000-0000-000000000003',
        'a0000000-0000-0000-0000-000000000003',
        'c0000000-0000-0000-0000-000000000001',
        'd0000000-0000-0000-0000-000000000001',
        'e0000000-0000-0000-0000-000000000001',
        'ENDED',
        'exotel-call-003',
        15,
        NOW() - INTERVAL '20 minutes',
        NOW() - INTERVAL '19 minutes',
        '{"transcript": [{"role": "agent", "content": "Hello—"}, {"role": "customer", "content": "Wrong number"}]}',
        '{"analysis_status": "pending"}'
    );

-- ─────────────────────────────────────────────────────────────────────────────
-- NEW TABLES: Added as part of the scalable post-call pipeline redesign.
-- ─────────────────────────────────────────────────────────────────────────────

-- Durable task tracker. Replaces the Celery-only task tracking that is lost
-- on Redis restarts. Every interaction that enters post-call processing gets
-- a row here. Status transitions are the authoritative record of what happened.
--
-- Statuses: queued → processing → completed | failed | exhausted
--   queued     : Celery task enqueued, not yet picked up by a worker
--   processing : Worker has started processing
--   completed  : All steps finished successfully
--   failed     : One step failed, within retry budget
--   exhausted  : Max retries consumed; needs manual intervention
CREATE TABLE analysis_tasks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id UUID NOT NULL UNIQUE REFERENCES interactions(id) ON DELETE CASCADE,
    customer_id UUID NOT NULL,
    campaign_id UUID NOT NULL,

    -- Processing lane determines priority queue routing:
    --   hot  : rebook_confirmed, demo_booked, escalation_needed → high-priority queue
    --   cold : not_interested, callback_requested, already_done  → standard queue
    --   skip : short_call (<4 turns)                            → no LLM, fast path
    lane VARCHAR(10) NOT NULL DEFAULT 'cold' CHECK (lane IN ('hot', 'cold', 'skip')),

    status VARCHAR(20) NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'processing', 'completed', 'failed', 'exhausted')),

    -- Which Celery task is currently handling this interaction.
    -- If the worker dies, we can detect orphaned tasks (no heartbeat for > N min).
    celery_task_id VARCHAR(255),

    -- Retry accounting
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 4,
    last_error TEXT,
    next_retry_at TIMESTAMPTZ,

    -- Token usage — written back after each LLM call for budget tracking.
    tokens_used INTEGER DEFAULT 0,

    -- Correlation ID threaded from the inbound webhook through every step.
    -- Lets an on-call engineer grep logs for a single interaction end-to-end.
    correlation_id UUID NOT NULL DEFAULT uuid_generate_v4(),

    -- Recording status tracked independently from LLM analysis
    recording_status VARCHAR(20) DEFAULT 'pending'
        CHECK (recording_status IN ('pending', 'uploaded', 'failed', 'skipped')),
    recording_s3_key VARCHAR(512),

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_analysis_tasks_interaction ON analysis_tasks(interaction_id);
CREATE INDEX idx_analysis_tasks_customer ON analysis_tasks(customer_id);
CREATE INDEX idx_analysis_tasks_status ON analysis_tasks(status);
CREATE INDEX idx_analysis_tasks_lane_status ON analysis_tasks(lane, status);
CREATE INDEX idx_analysis_tasks_correlation ON analysis_tasks(correlation_id);
CREATE INDEX idx_analysis_tasks_next_retry ON analysis_tasks(next_retry_at)
    WHERE status = 'failed';


-- Per-customer token budget configuration and real-time usage tracking.
--
-- Allocation model:
--   allocated_tpm  : Pre-reserved tokens/min for this customer (guaranteed floor)
--   burst_factor   : Multiplier over allocation when global headroom exists
--   used_tpm_current : Rolling 60-second window counter (written by budget_manager)
--
-- A customer with allocated_tpm=20 always gets ≥20 TPM even if others are active.
-- Unallocated headroom (global_tpm − sum(allocated_tpm)) is shared fairly among
-- customers that want to burst above their allocation.
CREATE TABLE customer_quotas (
    customer_id UUID PRIMARY KEY,

    -- Token budget
    allocated_tpm INTEGER NOT NULL DEFAULT 1000,  -- guaranteed tokens per minute
    burst_factor FLOAT NOT NULL DEFAULT 1.5,      -- max burst = allocated_tpm × burst_factor
    daily_token_limit BIGINT,                     -- optional hard cap; NULL = unlimited

    -- Priority within the hot lane when multiple customers compete
    priority_tier INTEGER NOT NULL DEFAULT 2      -- 1=highest, 3=lowest
        CHECK (priority_tier BETWEEN 1 AND 3),

    -- Processing preferences (no deployment needed to change per-customer behaviour)
    skip_llm_on_short_calls BOOLEAN NOT NULL DEFAULT TRUE,
    short_call_threshold INTEGER NOT NULL DEFAULT 4,   -- turns
    enable_crm_push BOOLEAN NOT NULL DEFAULT FALSE,
    crm_webhook_url TEXT,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed default quota rows for the test customer IDs used in fixtures
INSERT INTO customer_quotas (customer_id, allocated_tpm, burst_factor, priority_tier) VALUES
    ('d0000000-0000-0000-0000-000000000001', 2000, 1.5, 1),
    ('d0000000-0000-0000-0000-000000000002', 1000, 1.2, 2);


-- Structured audit log — one row per stage-transition per interaction.
-- Every step in the pipeline (enqueue, triage, rate_limit_wait, llm_call,
-- recording_upload, signal_job, lead_stage_update) writes a row here.
--
-- This makes it possible to answer "what happened to interaction X at 2am on Tuesday?"
-- without relying on log aggregation.
CREATE TABLE interaction_audit_log (
    id BIGSERIAL PRIMARY KEY,
    interaction_id UUID NOT NULL REFERENCES interactions(id) ON DELETE CASCADE,
    correlation_id UUID NOT NULL,
    customer_id UUID NOT NULL,

    -- Which pipeline step emitted this event
    stage VARCHAR(50) NOT NULL,
    -- e.g.: webhook_received, triage, rate_limit_wait, llm_call_start,
    --       llm_call_end, recording_poll_attempt, recording_uploaded,
    --       recording_failed, signal_job_triggered, lead_stage_updated,
    --       task_failed, task_completed

    status VARCHAR(20) NOT NULL CHECK (status IN ('started', 'completed', 'failed', 'skipped')),

    -- Stage-specific payload (token counts, retry attempts, error details, etc.)
    metadata JSONB DEFAULT '{}',

    -- Wall-clock duration of this stage (NULL if event is a point-in-time marker)
    duration_ms INTEGER,

    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_audit_log_interaction ON interaction_audit_log(interaction_id);
CREATE INDEX idx_audit_log_correlation ON interaction_audit_log(correlation_id);
CREATE INDEX idx_audit_log_customer_created ON interaction_audit_log(customer_id, created_at);
CREATE INDEX idx_audit_log_stage_status ON interaction_audit_log(stage, status);
