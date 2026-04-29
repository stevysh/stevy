package db

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"time"

	"github.com/stevysh/stevy/internal/dialect"
)

//go:embed migrations/postgres/*.sql migrations/sqlite/*.sql
var MigrationsFS embed.FS

// ─────────────────────────── Models ───────────────────────────

type User struct {
	ID        int64
	GoogleID  string
	Email     string
	Name      string
	CreatedAt time.Time
}

type APIKey struct {
	ID         int64
	UserID     int64
	Label      string
	KeyPrefix  string
	CreatedAt  time.Time
	LastUsedAt *time.Time
}

type APIKeyLookup struct {
	ID     int64
	UserID int64
}

type WorkerSummary struct {
	ID         int64
	Name       string
	CreatedBy  string
	CreatedAt  time.Time
	LastSeenAt *time.Time
}

// ─────────────────────────── DB wrapper ───────────────────────────

type DB struct {
	SQL     *sql.DB
	Dialect dialect.Dialect
}

func New(sqlDB *sql.DB, d dialect.Dialect) *DB {
	return &DB{SQL: sqlDB, Dialect: d}
}

func (d *DB) q(query string) string { return d.Dialect.Q(query) }

// ─────────────────────────── Users ───────────────────────────

func (d *DB) UpsertUser(ctx context.Context, googleID, email, name string) (*User, error) {
	var u User
	err := d.SQL.QueryRowContext(ctx, d.q(`
		INSERT INTO users (google_id, email, name)
		VALUES (?, ?, ?)
		ON CONFLICT (google_id) DO UPDATE SET email = excluded.email, name = excluded.name
		RETURNING id, google_id, email, name, created_at
	`), googleID, email, name).Scan(&u.ID, &u.GoogleID, &u.Email, &u.Name, &u.CreatedAt)
	return &u, err
}

func (d *DB) GetUserByID(ctx context.Context, id int64) (*User, error) {
	var u User
	err := d.SQL.QueryRowContext(ctx, d.q(`
		SELECT id, google_id, email, name, created_at FROM users WHERE id = ?
	`), id).Scan(&u.ID, &u.GoogleID, &u.Email, &u.Name, &u.CreatedAt)
	return &u, err
}

// ─────────────────────────── API Keys (client) ───────────────────────────

func (d *DB) CreateAPIKey(ctx context.Context, userID int64, label string) (plaintext string, key *APIKey, err error) {
	buf := make([]byte, 32)
	if _, err = rand.Read(buf); err != nil {
		return "", nil, err
	}
	plaintext = "stv_" + base64.RawURLEncoding.EncodeToString(buf)
	hash := sha256Hex(plaintext)
	prefix := plaintext[:12]

	var k APIKey
	err = d.SQL.QueryRowContext(ctx, d.q(`
		INSERT INTO api_keys (user_id, label, key_hash, key_prefix)
		VALUES (?, ?, ?, ?)
		RETURNING id, user_id, label, key_prefix, created_at, last_used_at
	`), userID, label, hash, prefix).Scan(
		&k.ID, &k.UserID, &k.Label, &k.KeyPrefix, &k.CreatedAt, &k.LastUsedAt,
	)
	return plaintext, &k, err
}

func (d *DB) ListAPIKeys(ctx context.Context, userID int64) ([]APIKey, error) {
	rows, err := d.SQL.QueryContext(ctx, d.q(`
		SELECT id, user_id, label, key_prefix, created_at, last_used_at
		FROM api_keys WHERE user_id = ? ORDER BY created_at DESC
	`), userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []APIKey
	for rows.Next() {
		var k APIKey
		if err := rows.Scan(&k.ID, &k.UserID, &k.Label, &k.KeyPrefix, &k.CreatedAt, &k.LastUsedAt); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, nil
}

func (d *DB) DeleteAPIKey(ctx context.Context, userID, keyID int64) error {
	res, err := d.SQL.ExecContext(ctx, d.q(`DELETE FROM api_keys WHERE id = ? AND user_id = ?`), keyID, userID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errors.New("key not found")
	}
	return nil
}

// LookupAPIKey hashes plaintext, looks it up in api_keys, bumps last_used_at.
func (d *DB) LookupAPIKey(ctx context.Context, plaintext string) (*APIKeyLookup, error) {
	hash := sha256Hex(plaintext)
	var update string
	switch d.Dialect {
	case dialect.Postgres:
		update = `UPDATE api_keys SET last_used_at = NOW() WHERE key_hash = ? RETURNING id, user_id`
	default:
		update = `UPDATE api_keys SET last_used_at = CURRENT_TIMESTAMP WHERE key_hash = ? RETURNING id, user_id`
	}
	var l APIKeyLookup
	err := d.SQL.QueryRowContext(ctx, d.q(update), hash).Scan(&l.ID, &l.UserID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errors.New("invalid key")
	}
	return &l, err
}

// ─────────────────────────── Workers ───────────────────────────

func (d *DB) CreateWorkerKey(ctx context.Context, userID int64, name string) (id int64, plaintext string, err error) {
	buf := make([]byte, 32)
	if _, err = rand.Read(buf); err != nil {
		return 0, "", err
	}
	plaintext = "stw_" + base64.RawURLEncoding.EncodeToString(buf)
	hash := sha256Hex(plaintext)
	prefix := plaintext[:12]
	err = d.SQL.QueryRowContext(ctx, d.q(`
		INSERT INTO workers (name, key_hash, key_prefix, created_by)
		VALUES (?, ?, ?, ?)
		RETURNING id
	`), name, hash, prefix, userID).Scan(&id)
	return id, plaintext, err
}

func (d *DB) LookupWorkerKey(ctx context.Context, plaintext string) (int64, error) {
	hash := sha256Hex(plaintext)
	var id int64
	err := d.SQL.QueryRowContext(ctx, d.q(`SELECT id FROM workers WHERE key_hash = ?`), hash).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, errors.New("invalid key")
	}
	return id, err
}

func (d *DB) ListWorkers(ctx context.Context) ([]WorkerSummary, error) {
	rows, err := d.SQL.QueryContext(ctx, d.q(`
		SELECT w.id, w.name, u.email, w.created_at, w.last_seen_at
		FROM workers w
		LEFT JOIN users u ON w.created_by = u.id
		ORDER BY w.created_at DESC
	`))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WorkerSummary
	for rows.Next() {
		var s WorkerSummary
		if err := rows.Scan(&s.ID, &s.Name, &s.CreatedBy, &s.CreatedAt, &s.LastSeenAt); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

func (d *DB) DeleteWorker(ctx context.Context, userID, workerID int64) error {
	res, err := d.SQL.ExecContext(ctx, d.q(`
		DELETE FROM workers WHERE id = ? AND created_by = ?
	`), workerID, userID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errors.New("worker not found")
	}
	return nil
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
