package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PGDriver struct {
	pool         *pgxpool.Pool
	lockDuration time.Duration
}

func NewPGDriver(pool *pgxpool.Pool, lockDuration time.Duration) *PGDriver {
	return &PGDriver{pool: pool, lockDuration: lockDuration}
}

const workerActiveWindow = 60 * time.Second

func (d *PGDriver) CreateJob(ctx context.Context, o CreateOpts) (*JobRow, error) {
	if _, err := d.pool.Exec(ctx, `
		INSERT INTO queues (name, updated_at) VALUES ($1, NOW())
		ON CONFLICT (name) DO UPDATE SET updated_at = NOW()
	`, o.Queue); err != nil {
		return nil, err
	}

	scheduled := time.Now()
	if o.ScheduledAt != nil {
		scheduled = *o.ScheduledAt
	}
	priority := o.Priority
	if priority == 0 {
		priority = 1
	}
	maxAttempts := o.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 25
	}
	status := "available"
	if scheduled.After(time.Now()) {
		status = "scheduled"
	}
	if o.Pending {
		status = "pending"
	}

	idV7, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	id := idV7.String()

	metadata := o.Metadata
	if len(metadata) == 0 {
		metadata = []byte("{}")
	}

	row, err := scanJobRowPG(d.pool.QueryRow(ctx, `
		INSERT INTO jobs (id, queue, kind, status, priority, max_attempts, payload, metadata, scheduled_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9)
		RETURNING id, queue, kind, status, priority, attempt, max_attempts, progress,
		          payload, metadata, result, errors,
		          created_at, scheduled_at, attempted_at, finalized_at
	`, id, o.Queue, o.Kind, status, priority, maxAttempts, o.PayloadJSON, metadata, scheduled))
	return row, err
}

func (d *PGDriver) ClaimJob(ctx context.Context, queue string, workerID int64) (*JobRow, error) {
	if _, err := d.pool.Exec(ctx, `
		UPDATE workers SET last_seen_at = NOW() WHERE id = $1
	`, workerID); err != nil {
		return nil, err
	}

	lockMs := d.lockDuration.Milliseconds()
	const q = `
WITH paused AS (
    SELECT 1 FROM queues WHERE name = $1 AND paused_at IS NOT NULL
), claimed AS (
    UPDATE jobs
    SET status          = 'running',
        attempt         = attempt + 1,
        attempted_at    = NOW(),
        attempted_by    = array_append(attempted_by, $2),
        lock_expires_at = NOW() + ($3::bigint || ' milliseconds')::interval
    WHERE NOT EXISTS (SELECT 1 FROM paused)
      AND id = (
        SELECT id FROM jobs
        WHERE queue = $1
          AND status = 'available'
          AND scheduled_at <= NOW()
        ORDER BY priority ASC, scheduled_at ASC, id ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    )
    RETURNING *
)
SELECT id, queue, kind, status, priority, attempt, max_attempts, progress,
       payload, metadata, result, errors,
       created_at, scheduled_at, attempted_at, finalized_at
FROM claimed
`
	row, err := scanJobRowPG(d.pool.QueryRow(ctx, q, queue, workerID, lockMs))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return row, err
}

func (d *PGDriver) CompleteJob(ctx context.Context, id string, resultJSON []byte) error {
	tag, err := d.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'completed', finalized_at = NOW(), result = $2::jsonb
		WHERE id = $1 AND status = 'running'
	`, id, resultJSON)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrWrongState
	}
	return nil
}

func (d *PGDriver) FailJob(ctx context.Context, id string, errMsg string, backoffMs int) error {
	var newStatus string
	err := d.pool.QueryRow(ctx, `
		UPDATE jobs
		SET status = CASE WHEN attempt >= max_attempts THEN 'discarded' ELSE 'retryable' END,
		    scheduled_at = CASE
		        WHEN attempt >= max_attempts THEN scheduled_at
		        ELSE NOW() + ($3::int || ' milliseconds')::interval
		    END,
		    errors = errors || jsonb_build_array(jsonb_build_object(
		        'at', NOW(), 'message', $2::text
		    )),
		    finalized_at = CASE WHEN attempt >= max_attempts THEN NOW() ELSE NULL END
		WHERE id = $1 AND status = 'running'
		RETURNING status
	`, id, errMsg, backoffMs).Scan(&newStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrWrongState
	}
	return err
}

func (d *PGDriver) HeartbeatJob(ctx context.Context, id string, progress *int) error {
	lockMs := d.lockDuration.Milliseconds()
	q := `
		UPDATE jobs
		SET lock_expires_at = NOW() + ($2::bigint || ' milliseconds')::interval`
	args := []any{id, lockMs}
	if progress != nil {
		q += `, progress = $3`
		args = append(args, *progress)
	}
	q += ` WHERE id = $1 AND status = 'running'`
	tag, err := d.pool.Exec(ctx, q, args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrWrongState
	}
	return nil
}

func (d *PGDriver) FailExpiredJobs(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		limit = 1000
	}
	tag, err := d.pool.Exec(ctx, `
		UPDATE jobs
		SET status = CASE WHEN attempt >= max_attempts THEN 'discarded' ELSE 'retryable' END,
		    scheduled_at = CASE WHEN attempt >= max_attempts THEN scheduled_at ELSE NOW() END,
		    errors = errors || jsonb_build_array(jsonb_build_object(
		        'at', NOW(), 'message', 'job lock expired'
		    )),
		    finalized_at    = CASE WHEN attempt >= max_attempts THEN NOW() ELSE NULL END,
		    lock_expires_at = NULL
		WHERE id IN (
		    SELECT id FROM jobs
		    WHERE status = 'running'
		      AND lock_expires_at < NOW()
		    ORDER BY lock_expires_at ASC
		    LIMIT $1
		    FOR UPDATE SKIP LOCKED
		)
	`, limit)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (d *PGDriver) PromoteJob(ctx context.Context, id string) error {
	tag, err := d.pool.Exec(ctx, `
		UPDATE jobs
		SET status = CASE WHEN scheduled_at > NOW() THEN 'scheduled' ELSE 'available' END
		WHERE id = $1 AND status = 'pending'
	`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrWrongState
	}
	return nil
}

func (d *PGDriver) CancelJob(ctx context.Context, id string) error {
	tag, err := d.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'cancelled',
		    finalized_at = COALESCE(finalized_at, NOW())
		WHERE id = $1
		  AND status NOT IN ('completed', 'discarded', 'cancelled')
	`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrWrongState
	}
	return nil
}

func (d *PGDriver) GetJob(ctx context.Context, id string) (*JobRow, error) {
	row, err := scanJobRowPG(d.pool.QueryRow(ctx, `
		SELECT id, queue, kind, status, priority, attempt, max_attempts, progress,
		       payload, metadata, result, errors,
		       created_at, scheduled_at, attempted_at, finalized_at
		FROM jobs WHERE id = $1
	`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return row, err
}

func (d *PGDriver) GetJobStatus(ctx context.Context, id string) (*JobStatusRow, error) {
	var s JobStatusRow
	err := d.pool.QueryRow(ctx, `
		SELECT id, status, progress, errors FROM jobs WHERE id = $1
	`, id).Scan(&s.ID, &s.Status, &s.Progress, &s.ErrorsJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &s, err
}

func (d *PGDriver) QueueInfo(ctx context.Context, name string) (*QueueInfo, error) {
	info := &QueueInfo{CountsByStatus: map[string]int32{}}

	rows, err := d.pool.Query(ctx, `
		SELECT status, COUNT(*)::int FROM jobs WHERE queue = $1 GROUP BY status
	`, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var count int32
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		info.CountsByStatus[status] = count
	}

	var pausedAt *time.Time
	_ = d.pool.QueryRow(ctx, `SELECT paused_at FROM queues WHERE name = $1`, name).Scan(&pausedAt)
	info.Paused = pausedAt != nil

	return info, nil
}

func (d *PGDriver) ListQueues(ctx context.Context) ([]QueueSummary, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT q.name, q.paused_at IS NOT NULL FROM queues q ORDER BY q.name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []QueueSummary
	for rows.Next() {
		var s QueueSummary
		if err := rows.Scan(&s.Name, &s.Paused); err != nil {
			return nil, err
		}
		s.CountsByStatus = map[string]int32{}
		out = append(out, s)
	}

	for i := range out {
		crows, err := d.pool.Query(ctx, `
			SELECT status, COUNT(*)::int FROM jobs WHERE queue = $1 GROUP BY status
		`, out[i].Name)
		if err != nil {
			return nil, err
		}
		for crows.Next() {
			var status string
			var count int32
			if err := crows.Scan(&status, &count); err != nil {
				crows.Close()
				return nil, err
			}
			out[i].CountsByStatus[status] = count
		}
		crows.Close()
	}
	return out, nil
}

func (d *PGDriver) PauseQueue(ctx context.Context, name string) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO queues (name, paused_at, updated_at) VALUES ($1, NOW(), NOW())
		ON CONFLICT (name) DO UPDATE SET paused_at = NOW(), updated_at = NOW()
	`, name)
	return err
}

func (d *PGDriver) ResumeQueue(ctx context.Context, name string) error {
	_, err := d.pool.Exec(ctx, `
		UPDATE queues SET paused_at = NULL, updated_at = NOW() WHERE name = $1
	`, name)
	return err
}

// PromoteScheduledJobs flips scheduled/retryable rows whose time has come to
// 'available'. Bounded LIMIT + SKIP LOCKED so a backlog doesn't block writers.
func (d *PGDriver) PromoteScheduledJobs(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		limit = 1000
	}
	tag, err := d.pool.Exec(ctx, `
		UPDATE jobs SET status = 'available'
		WHERE id IN (
		    SELECT id FROM jobs
		    WHERE status IN ('scheduled', 'retryable')
		      AND scheduled_at <= NOW()
		    ORDER BY scheduled_at ASC
		    LIMIT $1
		    FOR UPDATE SKIP LOCKED
		)
	`, limit)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (d *PGDriver) ListJobs(ctx context.Context, queue, status string, limit int, afterID string) ([]JobRow, error) {
	if limit <= 0 {
		limit = 50
	}
	var args []any
	var conds []string
	if queue != "" {
		args = append(args, queue)
		conds = append(conds, fmt.Sprintf("queue = $%d", len(args)))
	}
	if status != "" {
		args = append(args, status)
		conds = append(conds, fmt.Sprintf("status = $%d", len(args)))
	}
	if afterID != "" {
		args = append(args, afterID)
		conds = append(conds, fmt.Sprintf("id < $%d", len(args)))
	}
	q := `SELECT id, queue, kind, status, priority, attempt, max_attempts, progress,
		       payload, metadata, result, errors,
		       created_at, scheduled_at, attempted_at, finalized_at
		FROM jobs`
	if len(conds) > 0 {
		q += " WHERE " + strings.Join(conds, " AND ")
	}
	args = append(args, limit)
	q += fmt.Sprintf(` ORDER BY id DESC LIMIT $%d`, len(args))

	rows, err := d.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []JobRow
	for rows.Next() {
		row, err := scanJobRowPG(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *row)
	}
	return out, nil
}

func (d *PGDriver) JobCountsByStatus(ctx context.Context) (map[string]int32, error) {
	rows, err := d.pool.Query(ctx, `SELECT status, COUNT(*)::int FROM jobs GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := map[string]int32{}
	for rows.Next() {
		var status string
		var count int32
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		counts[status] = count
	}
	return counts, nil
}

type pgScannable interface {
	Scan(dest ...any) error
}

func scanJobRowPG(s pgScannable) (*JobRow, error) {
	var r JobRow
	err := s.Scan(
		&r.ID, &r.Queue, &r.Kind, &r.Status,
		&r.Priority, &r.Attempt, &r.MaxAttempts, &r.Progress,
		&r.PayloadJSON, &r.MetadataJSON, &r.ResultJSON, &r.ErrorsJSON,
		&r.CreatedAt, &r.ScheduledAt, &r.AttemptedAt, &r.FinalizedAt,
	)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

var (
	ErrNotFound   = fmt.Errorf("not found")
	ErrWrongState = fmt.Errorf("wrong state")
)
