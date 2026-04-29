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
| `BatchGetJobStatuses` | `GET` | `/v1/jobs/statuses` |
| `SetJobProgress` | `PATCH` | `/v1/jobs/{id}/progress` |
| `ClaimJob` | `POST` | `/v1/queues/{queue}/claim` |
| `CompleteJob` | `POST` | `/v1/jobs/{id}/complete` |
| `FailJob` | `POST` | `/v1/jobs/{id}/fail` |
| `HeartbeatJob` | `POST` | `/v1/jobs/{id}/heartbeat` |
| `CancelJob` | `POST` | `/v1/jobs/{id}/cancel` |
| `PromoteJob` | `POST` | `/v1/jobs/{id}/promote` |

### `stevy.v1.QueueService`

| RPC | Method | Path |
|---|---|---|
| `GetQueue` | `GET` | `/v1/queues/{name}` |

## Links

- [GitHub](https://github.com/stevysh/stevy)
