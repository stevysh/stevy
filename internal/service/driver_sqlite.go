package service

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	nanoid "github.com/matoous/go-nanoid/v2"
)

// SQLiteDriver is the SQLite implementation of Driver.
//
// SQLite serialises writers at the DB level, so concurrent ClaimJob calls
// queue but never race — no FOR UPDATE SKIP LOCKED needed.
type SQLiteDriver struct {
	db           *sql.DB
	lockDuration time.Duration
}

func NewSQLiteDriver(db *sql.DB, lockDuration time.Duration) *SQLiteDriver {
	return &SQLiteDriver{db: db, lockDuration: lockDuration}
}

const sqliteTimeFmt = "2006-01-02 15:04:05"

func parseTime(s string) time.Time {
	t, _ := time.ParseInLocation(sqliteTimeFmt, s, time.UTC)
	return t
}

func parseTimePtr(s sql.NullString) *time.Time {
	if !s.Valid || s.String == "" {
		return nil
	}
	t, err := time.ParseInLocation(sqliteTimeFmt, s.String, time.UTC)
	if err != nil {
		return nil
	}
	return &t
}

func (d *SQLiteDriver) CreateJob(ctx context.Context, o CreateOpts) (*JobRow, error) {
	if _, err := d.db.ExecContext(ctx, `
		INSERT INTO queues (name, updated_at) VALUES (?, CURRENT_TIMESTAMP)
		ON CONFLICT (name) DO UPDATE SET updated_at = CURRENT_TIMESTAMP
	`, o.Queue); err != nil {
		return nil, err
	}

	scheduled := time.Now().UTC()
	if o.ScheduledAt != nil {
		scheduled = o.ScheduledAt.UTC()
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

	id, err := nanoid.New()
	if err != nil {
		return nil, err
	}
	metadata := o.Metadata
	if len(metadata) == 0 {
		metadata = []byte("{}")
	}

	row, err := scanJobRowSqlite(d.db.QueryRowContext(ctx, `
		INSERT INTO jobs (id, queue, kind, status, priority, max_attempts, payload, metadata, scheduled_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING id, queue, kind, status, priority, attempt, max_attempts, progress,
		          payload, metadata, result, errors,
		          strftime('%Y-%m-%d %H:%M:%S', created_at),
		          strftime('%Y-%m-%d %H:%M:%S', scheduled_at),
		          strftime('%Y-%m-%d %H:%M:%S', attempted_at),
		          strftime('%Y-%m-%d %H:%M:%S', finalized_at)
	`, id, o.Queue, o.Kind, status, priority, maxAttempts, string(o.PayloadJSON), string(metadata), scheduled.Format(sqliteTimeFmt)))
	return row, err
}

func (d *SQLiteDriver) ClaimJob(ctx context.Context, queue string, workerID int64) (*JobRow, error) {
	if _, err := d.db.ExecContext(ctx, `
		UPDATE workers SET last_seen_at = CURRENT_TIMESTAMP WHERE id = ?
	`, workerID); err != nil {
		return nil, err
	}

	lockSec := int64(d.lockDuration.Seconds())
	const q = `
WITH paused AS (
    SELECT 1 FROM queues WHERE name = ? AND paused_at IS NOT NULL
), claimed AS (
    UPDATE jobs
    SET status          = 'running',
        attempt         = attempt + 1,
        attempted_at    = CURRENT_TIMESTAMP,
        attempted_by    = json_insert(COALESCE(attempted_by, '[]'), '$[#]', ?),
        lock_expires_at = datetime('now', '+' || ? || ' seconds')
    WHERE NOT EXISTS (SELECT 1 FROM paused)
      AND id = (
        SELECT id FROM jobs
        WHERE queue = ?
          AND status = 'available'
          AND scheduled_at <= CURRENT_TIMESTAMP
        ORDER BY priority ASC, scheduled_at ASC, id ASC
        LIMIT 1
    )
    RETURNING *
)
SELECT id, queue, kind, status, priority, attempt, max_attempts, progress,
       payload, metadata, result, errors,
       strftime('%Y-%m-%d %H:%M:%S', created_at),
       strftime('%Y-%m-%d %H:%M:%S', scheduled_at),
       strftime('%Y-%m-%d %H:%M:%S', attempted_at),
       strftime('%Y-%m-%d %H:%M:%S', finalized_at)
FROM claimed
`
	row, err := scanJobRowSqlite(d.db.QueryRowContext(ctx, q, queue, workerID, lockSec, queue))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return row, err
}

func (d *SQLiteDriver) CompleteJob(ctx context.Context, id string, resultJSON []byte) error {
	res, err := d.db.ExecContext(ctx, `
		UPDATE jobs
		SET status = 'completed', finalized_at = CURRENT_TIMESTAMP, result = ?
		WHERE id = ? AND status = 'running'
	`, string(resultJSON), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrWrongState
	}
	return nil
}

func (d *SQLiteDriver) FailJob(ctx context.Context, id string, errMsg string, backoffMs int) error {
	backoffSec := (backoffMs + 999) / 1000
	errorEntry := buildErrorEntry(errMsg)

	var newStatus string
	err := d.db.QueryRowContext(ctx, `
		UPDATE jobs
		SET status = CASE WHEN attempt >= max_attempts THEN 'discarded' ELSE 'retryable' END,
		    scheduled_at = CASE
		        WHEN attempt >= max_attempts THEN scheduled_at
		        ELSE datetime('now', '+' || ? || ' seconds')
		    END,
		    errors = json_insert(COALESCE(errors, '[]'), '$[#]', json(?)),
		    finalized_at = CASE WHEN attempt >= max_attempts THEN CURRENT_TIMESTAMP ELSE NULL END
		WHERE id = ? AND status = 'running'
		RETURNING status
	`, backoffSec, errorEntry, id).Scan(&newStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrWrongState
	}
	return err
}

func (d *SQLiteDriver) HeartbeatJob(ctx context.Context, id string, progress *int) error {
	lockSec := int64(d.lockDuration.Seconds())
	q := `
		UPDATE jobs
		SET lock_expires_at = datetime('now', '+' || ? || ' seconds')`
	args := []any{lockSec}
	if progress != nil {
		q += `, progress = ?`
		args = append(args, *progress)
	}
	q += ` WHERE id = ? AND status = 'running'`
	args = append(args, id)

	res, err := d.db.ExecContext(ctx, q, args...)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrWrongState
	}
	return nil
}

func (d *SQLiteDriver) FailExpiredJobs(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		limit = 1000
	}
	errorEntry := buildErrorEntry("job lock expired")
	res, err := d.db.ExecContext(ctx, `
		UPDATE jobs
		SET status = CASE WHEN attempt >= max_attempts THEN 'discarded' ELSE 'retryable' END,
		    scheduled_at = CASE WHEN attempt >= max_attempts THEN scheduled_at ELSE CURRENT_TIMESTAMP END,
		    errors = json_insert(COALESCE(errors, '[]'), '$[#]', json(?)),
		    finalized_at    = CASE WHEN attempt >= max_attempts THEN CURRENT_TIMESTAMP ELSE NULL END,
		    lock_expires_at = NULL
		WHERE id IN (
		    SELECT id FROM jobs
		    WHERE status = 'running'
		      AND lock_expires_at < CURRENT_TIMESTAMP
		    ORDER BY lock_expires_at ASC
		    LIMIT ?
		)
	`, errorEntry, limit)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (d *SQLiteDriver) PromoteJob(ctx context.Context, id string) error {
	res, err := d.db.ExecContext(ctx, `
		UPDATE jobs
		SET status = CASE WHEN scheduled_at > CURRENT_TIMESTAMP THEN 'scheduled' ELSE 'available' END
		WHERE id = ? AND status = 'pending'
	`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrWrongState
	}
	return nil
}

func (d *SQLiteDriver) CancelJob(ctx context.Context, id string) error {
	res, err := d.db.ExecContext(ctx, `
		UPDATE jobs
		SET status = 'cancelled',
		    finalized_at = COALESCE(finalized_at, CURRENT_TIMESTAMP)
		WHERE id = ? AND status NOT IN ('completed', 'discarded', 'cancelled')
	`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrWrongState
	}
	return nil
}

func (d *SQLiteDriver) GetJob(ctx context.Context, id string) (*JobRow, error) {
	row, err := scanJobRowSqlite(d.db.QueryRowContext(ctx, `
		SELECT id, queue, kind, status, priority, attempt, max_attempts, progress,
		       payload, metadata, result, errors,
		       strftime('%Y-%m-%d %H:%M:%S', created_at),
		       strftime('%Y-%m-%d %H:%M:%S', scheduled_at),
		       strftime('%Y-%m-%d %H:%M:%S', attempted_at),
		       strftime('%Y-%m-%d %H:%M:%S', finalized_at)
		FROM jobs WHERE id = ?
	`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return row, err
}

func (d *SQLiteDriver) GetJobStatus(ctx context.Context, id string) (*JobStatusRow, error) {
	var s JobStatusRow
	var errorsJSON sql.NullString
	err := d.db.QueryRowContext(ctx, `
		SELECT id, status, progress, errors FROM jobs WHERE id = ?
	`, id).Scan(&s.ID, &s.Status, &s.Progress, &errorsJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if errorsJSON.Valid {
		s.ErrorsJSON = []byte(errorsJSON.String)
	}
	return &s, nil
}

func (d *SQLiteDriver) QueueInfo(ctx context.Context, name string) (*QueueInfo, error) {
	info := &QueueInfo{CountsByStatus: map[string]int32{}}

	rows, err := d.db.QueryContext(ctx, `
		SELECT status, COUNT(*) FROM jobs WHERE queue = ? GROUP BY status
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

	var pausedAt sql.NullString
	_ = d.db.QueryRowContext(ctx, `SELECT paused_at FROM queues WHERE name = ?`, name).Scan(&pausedAt)
	info.Paused = pausedAt.Valid

	return info, nil
}

func (d *SQLiteDriver) ListQueues(ctx context.Context) ([]QueueSummary, error) {
	rows, err := d.db.QueryContext(ctx, `
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
		crows, err := d.db.QueryContext(ctx, `
			SELECT status, COUNT(*) FROM jobs WHERE queue = ? GROUP BY status
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

func (d *SQLiteDriver) PauseQueue(ctx context.Context, name string) error {
	_, err := d.db.ExecContext(ctx, `
		INSERT INTO queues (name, paused_at, updated_at) VALUES (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		ON CONFLICT (name) DO UPDATE SET paused_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
	`, name)
	return err
}

func (d *SQLiteDriver) ResumeQueue(ctx context.Context, name string) error {
	_, err := d.db.ExecContext(ctx, `
		UPDATE queues SET paused_at = NULL, updated_at = CURRENT_TIMESTAMP WHERE name = ?
	`, name)
	return err
}

// PromoteScheduledJobs flips scheduled/retryable rows whose time has come to
// 'available'. SQLite serialises writers, so no SKIP LOCKED needed.
func (d *SQLiteDriver) PromoteScheduledJobs(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		limit = 1000
	}
	res, err := d.db.ExecContext(ctx, `
		UPDATE jobs SET status = 'available'
		WHERE id IN (
		    SELECT id FROM jobs
		    WHERE status IN ('scheduled', 'retryable')
		      AND scheduled_at <= CURRENT_TIMESTAMP
		    ORDER BY scheduled_at ASC
		    LIMIT ?
		)
	`, limit)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (d *SQLiteDriver) ListJobs(ctx context.Context, queue, status string, pageSize int, afterID string, afterCreatedAt *time.Time) ([]JobRow, error) {
	if pageSize <= 0 {
		pageSize = 50
	}
	var args []any
	var conds []string
	if queue != "" {
		conds = append(conds, "queue = ?")
		args = append(args, queue)
	}
	if status != "" {
		conds = append(conds, "status = ?")
		args = append(args, status)
	}
	if afterCreatedAt != nil {
		conds = append(conds, "(created_at < ? OR (created_at = ? AND id < ?))")
		t := afterCreatedAt.UTC().Format(sqliteTimeFmt)
		args = append(args, t, t, afterID)
	}
	q := `SELECT id, queue, kind, status, priority, attempt, max_attempts, progress,
		       payload, metadata, result, errors,
		       strftime('%Y-%m-%d %H:%M:%S', created_at),
		       strftime('%Y-%m-%d %H:%M:%S', scheduled_at),
		       strftime('%Y-%m-%d %H:%M:%S', attempted_at),
		       strftime('%Y-%m-%d %H:%M:%S', finalized_at)
		FROM jobs`
	if len(conds) > 0 {
		q += " WHERE " + strings.Join(conds, " AND ")
	}
	q += ` ORDER BY created_at DESC, id DESC LIMIT ?`
	args = append(args, pageSize)

	rows, err := d.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []JobRow
	for rows.Next() {
		row, err := scanJobRowSqlite(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *row)
	}
	return out, nil
}

func (d *SQLiteDriver) JobCountsByStatus(ctx context.Context) (map[string]int32, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT status, COUNT(*) FROM jobs GROUP BY status`)
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

type sqliteScannable interface {
	Scan(dest ...any) error
}

func scanJobRowSqlite(s sqliteScannable) (*JobRow, error) {
	var r JobRow
	var payload, metadata, errs string
	var result sql.NullString
	var createdAt, scheduledAt string
	var attemptedAt, finalizedAt sql.NullString
	err := s.Scan(
		&r.ID, &r.Queue, &r.Kind, &r.Status,
		&r.Priority, &r.Attempt, &r.MaxAttempts, &r.Progress,
		&payload, &metadata, &result, &errs,
		&createdAt, &scheduledAt, &attemptedAt, &finalizedAt,
	)
	if err != nil {
		return nil, err
	}
	r.PayloadJSON = []byte(payload)
	r.MetadataJSON = []byte(metadata)
	r.ErrorsJSON = []byte(errs)
	if result.Valid {
		r.ResultJSON = []byte(result.String)
	}
	r.CreatedAt = parseTime(createdAt)
	r.ScheduledAt = parseTime(scheduledAt)
	r.AttemptedAt = parseTimePtr(attemptedAt)
	r.FinalizedAt = parseTimePtr(finalizedAt)
	return &r, nil
}

func buildErrorEntry(msg string) string {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return `{"at":"` + now + `","message":` + jsonString(msg) + `}`
}

func jsonString(s string) string {
	var sb strings.Builder
	sb.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			sb.WriteString(`\"`)
		case '\\':
			sb.WriteString(`\\`)
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\t':
			sb.WriteString(`\t`)
		default:
			sb.WriteRune(r)
		}
	}
	sb.WriteByte('"')
	return sb.String()
}
