package service

import (
	"context"
	"time"
)

type JobRow struct {
	ID           string
	Queue        string
	Kind         string
	Status       string
	Priority     int
	Attempt      int
	MaxAttempts  int
	Progress     int
	PayloadJSON  []byte
	MetadataJSON []byte
	ResultJSON   []byte
	ErrorsJSON   []byte
	CreatedAt    time.Time
	ScheduledAt  time.Time
	AttemptedAt  *time.Time
	FinalizedAt  *time.Time
}

type JobStatusRow struct {
	ID         string
	Status     string
	Progress   int
	ErrorsJSON []byte
}

type CreateOpts struct {
	Queue       string
	Kind        string
	PayloadJSON []byte
	Metadata    []byte
	MaxAttempts int
	Priority    int
	ScheduledAt *time.Time
	Pending     bool // insert in 'pending' status; caller must PromoteJob to release
}

type QueueInfo struct {
	CountsByStatus map[string]int32
	Paused         bool
}

type QueueSummary struct {
	Name           string
	Paused         bool
	CountsByStatus map[string]int32
}

type Driver interface {
	CreateJob(ctx context.Context, opts CreateOpts) (*JobRow, error)
	ClaimJob(ctx context.Context, queueName string, workerID int64) (*JobRow, error)
	CompleteJob(ctx context.Context, id string, resultJSON []byte) error
	FailJob(ctx context.Context, id string, errMsg string, backoffMs int) error
	HeartbeatJob(ctx context.Context, id string, progress *int) error
	CancelJob(ctx context.Context, id string) error
	PromoteJob(ctx context.Context, id string) error
	GetJob(ctx context.Context, id string) (*JobRow, error)
	GetJobStatus(ctx context.Context, id string) (*JobStatusRow, error)
	QueueInfo(ctx context.Context, queueName string) (*QueueInfo, error)
	ListQueues(ctx context.Context) ([]QueueSummary, error)
	PauseQueue(ctx context.Context, name string) error
	ResumeQueue(ctx context.Context, name string) error
	ListJobs(ctx context.Context, queue, status string, pageSize int, afterID string, afterCreatedAt *time.Time) ([]JobRow, error)
	JobCountsByStatus(ctx context.Context) (map[string]int32, error)
	PromoteScheduledJobs(ctx context.Context, limit int) (int64, error)
	FailExpiredJobs(ctx context.Context, limit int) (int64, error)
}
