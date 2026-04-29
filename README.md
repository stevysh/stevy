# Stevy

Self-hosted job queue service with built-in dashboard and multi-protocol API.

## Quick start

```bash
export SESSION_SECRET="$(openssl rand -base64 32)"
export GOOGLE_CLIENT_ID="your-client-id"
export GOOGLE_CLIENT_SECRET="your-client-secret"
export DATABASE_URL="sqlite://./stevy.db"

go run ./cmd/stevy serve --migrate
```

Visit `http://localhost:8080` to sign in and manage jobs, queues, workers, and API keys.

## Environment variables

| Variable | Description | Default |
|---|---|---|
| `PORT` | Server listen port | `3000` |
| `DATABASE_URL` | Database connection string (see below) | required |
| `SESSION_SECRET` | HMAC key for signing session cookies | required |
| `GOOGLE_CLIENT_ID` | Google OAuth 2.0 client ID | required |
| `GOOGLE_CLIENT_SECRET` | Google OAuth 2.0 client secret | required |
| `HOSTNAME` | Public base URL, used to build the OAuth callback URL | `http://localhost:8080` |
| `ALLOWED_DOMAINS` | Comma-separated list of allowed email domains | — (all allowed) |
| `SCHEDULER_INTERVAL` | How often the scheduler runs | `1s` |
| `JOB_LOCK_DURATION` | How long a claimed job lock lasts; workers must heartbeat before it expires | `30s` |
| `MIGRATE` | Run migrations on startup when `true` | — |

## Database

Driver is auto-detected from `DATABASE_URL`:

| DSN | Database |
|---|---|
| `sqlite://./stevy.db` or `file:stevy.db` | SQLite (good for dev) |
| `postgres://user:pass@host/db` | PostgreSQL (production) |

SQLite serialises all writers — use PostgreSQL for any concurrent workload.

## CLI

```
stevy serve [--migrate]   # start the server, optionally run migrations first
stevy scheduler           # run the scheduler process
stevy migrate             # run database migrations and exit
stevy version             # print version
stevy help                # print usage
```

## Authentication

### Web UI (Google OAuth)

Sign in at `/` — the dashboard lets you manage jobs, queues, workers, and API keys.
Restrict login to specific email domains with `ALLOWED_DOMAINS`.

### API (Bearer token)

All RPC endpoints require `Authorization: Bearer <key>`. Two key types:

| Prefix | Issued from | Allowed RPCs |
|---|---|---|
| `stv_` | Dashboard → API Keys | All job and queue RPCs |
| `stw_` | Dashboard → Workers | `ClaimJob`, `CompleteJob`, `FailJob`, `HeartbeatJob`, `SetJobProgress` |

Plaintext key shown once on creation — only the SHA-256 hash is stored.

## Protocols

A single endpoint serves all protocols via [vanguard-go](https://github.com/connectrpc/vanguard-go) transcoding:

| Protocol | Transport |
|---|---|
| gRPC | HTTP/2 (h2c) |
| gRPC-Web | HTTP/1.1 or HTTP/2 |
| Connect | HTTP/1.1 or HTTP/2 |
| REST | HTTP/1.1 or HTTP/2 |

## Worker flow

```
CreateJob  →  ClaimJob  →  (HeartbeatJob loop)  →  CompleteJob | FailJob
                               (SetJobProgress)
```

Workers must call `HeartbeatJob` at least once every `JOB_LOCK_DURATION` (default `30s`) to
keep their lock alive. If a worker dies, the scheduler will fail the job automatically on the
next tick and reschedule it (up to `max_attempts`).

```bash
# Create a job — REST (client key)
curl -X POST http://localhost:8080/v1/jobs \
  -H "Authorization: Bearer stv_XXXXXX" \
  -H "Content-Type: application/json" \
  -d '{"queue": "translate", "kind": "TranslateDoc", "payload": {"doc_id": "abc"}}'

# Create a job — Connect
curl -X POST http://localhost:8080/stevy.v1.JobService/CreateJob \
  -H "Authorization: Bearer stv_XXXXXX" \
  -H "Content-Type: application/json" \
  -H "Connect-Protocol-Version: 1" \
  -d '{"queue": "translate", "kind": "TranslateDoc", "payload": {"doc_id": "abc"}}'

# Claim next job — REST (worker key)
curl -X POST http://localhost:8080/v1/queues/translate/claim \
  -H "Authorization: Bearer stw_XXXXXX"
```

## Scheduler

Jobs with a future `scheduled_at`, or that failed and need a retry, sit in `scheduled` /
`retryable` status. The scheduler promotes them to `available` when their time arrives.

Deploy as a long-lived process alongside the server:

```bash
stevy scheduler
```

On request-based platforms (Cloud Run, etc.) where a persistent process isn't practical,
trigger a single pass over HTTP instead:

```bash
curl -X POST http://localhost:8080/scheduler/run
```

## Job statuses

| Status | Meaning |
|---|---|
| `available` | Ready to be claimed |
| `pending` | Inserted but not yet made available (promote manually) |
| `scheduled` | Delayed, waiting for `scheduled_at` |
| `running` | Currently being worked |
| `retryable` | Failed an attempt, waiting for backoff |
| `completed` | Finished successfully |
| `discarded` | Permanently failed (exhausted retries) |
| `cancelled` | Explicitly cancelled |

## License

Apache 2.0
