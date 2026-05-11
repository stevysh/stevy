# Stevy

Self-hosted job queue service with multi-protocol API (gRPC, gRPC-Web, Connect, REST).

## Services

### `stevy.v1.JobService`

| RPC | Method | Path |
|---|---|---|
| `CreateJob` | `POST` | `/v1/jobs` |
| `ListJobs` | `GET` | `/v1/jobs` |
| `GetJob` | `GET` | `/v1/jobs/{id}` |
| `GetJobStatus` | `GET` | `/v1/jobs/{id}/status` |
| `ClaimJob` | `POST` | `/v1/jobs/claim` |
| `CompleteJob` | `POST` | `/v1/jobs/{id}/complete` |
| `FailJob` | `POST` | `/v1/jobs/{id}/fail` |
| `HeartbeatJob` | `POST` | `/v1/jobs/{id}/heartbeat` |
| `CancelJob` | `POST` | `/v1/jobs/{id}/cancel` |
| `PromoteJob` | `POST` | `/v1/jobs/{id}/promote` |

### `stevy.v1.QueueService`

| RPC | Method | Path |
|---|---|---|
| `GetQueue` | `GET` | `/v1/queues/{name}` |
| `PauseQueue` | `POST` | `/v1/queues/{name}/pause` |
| `ResumeQueue` | `POST` | `/v1/queues/{name}/resume` |

## Links

- [GitHub](https://github.com/stevysh/stevy)
