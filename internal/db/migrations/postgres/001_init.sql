-- +goose Up
CREATE TABLE users (
    id          BIGSERIAL PRIMARY KEY,
    google_id   TEXT NOT NULL UNIQUE,
    email       TEXT NOT NULL,
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE api_keys (
    id           BIGSERIAL PRIMARY KEY,
    user_id      BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    label        TEXT NOT NULL,
    key_hash     TEXT NOT NULL UNIQUE,
    key_prefix   TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMPTZ
);

CREATE INDEX idx_api_keys_user_id ON api_keys(user_id);
CREATE INDEX idx_api_keys_key_hash ON api_keys(key_hash);

CREATE TABLE jobs (
    id           TEXT PRIMARY KEY,
    queue        TEXT NOT NULL,
    kind         TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'available',
    priority     INT  NOT NULL DEFAULT 1,
    attempt      INT  NOT NULL DEFAULT 0,
    max_attempts INT  NOT NULL DEFAULT 25,
    progress     INT  NOT NULL DEFAULT 0 CHECK (progress >= 0 AND progress <= 100),
    payload      JSONB NOT NULL DEFAULT '{}',
    metadata     JSONB NOT NULL DEFAULT '{}',
    result       JSONB,
    errors       JSONB NOT NULL DEFAULT '[]',
    attempted_by TEXT[] NOT NULL DEFAULT '{}',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempted_at   TIMESTAMPTZ,
    finalized_at   TIMESTAMPTZ,
    lock_expires_at TIMESTAMPTZ
);

CREATE INDEX jobs_pickup_idx ON jobs(queue, priority, scheduled_at) WHERE status = 'available';
CREATE INDEX jobs_status_idx ON jobs(status);

CREATE TABLE queues (
    name       TEXT PRIMARY KEY,
    paused_at  TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE workers (
    id           BIGSERIAL PRIMARY KEY,
    name         TEXT NOT NULL UNIQUE,
    key_hash     TEXT NOT NULL UNIQUE,
    key_prefix   TEXT NOT NULL,
    created_by   BIGINT REFERENCES users(id) ON DELETE SET NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ
);

-- +goose Down
DROP TABLE IF EXISTS workers;
DROP TABLE IF EXISTS queues;
DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS api_keys;
DROP TABLE IF EXISTS users;
