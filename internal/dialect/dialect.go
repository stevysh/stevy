package dialect

import (
	"fmt"
	"strings"
)

type Dialect string

const (
	Postgres Dialect = "postgres"
	SQLite   Dialect = "sqlite"
)

// FromDSN returns the dialect for a connection URL.
// Recognised prefixes: postgres://, postgresql:// → Postgres; sqlite://, sqlite3://, file: → SQLite.
func FromDSN(dsn string) (Dialect, error) {
	switch {
	case strings.HasPrefix(dsn, "postgres://"), strings.HasPrefix(dsn, "postgresql://"):
		return Postgres, nil
	case strings.HasPrefix(dsn, "sqlite://"), strings.HasPrefix(dsn, "sqlite3://"), strings.HasPrefix(dsn, "file:"):
		return SQLite, nil
	}
	return "", fmt.Errorf("unknown database dialect for DSN: %s", dsn)
}

// Q rewrites ? placeholders to $1, $2, ... for Postgres. SQLite keeps ?.
func (d Dialect) Q(query string) string {
	if d != Postgres {
		return query
	}
	var sb strings.Builder
	n := 1
	for _, r := range query {
		if r == '?' {
			fmt.Fprintf(&sb, "$%d", n)
			n++
		} else {
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

// StripSQLitePrefix converts "sqlite://path" or "sqlite3://path" to just "path"
// for use with modernc.org/sqlite which takes a filesystem path.
func StripSQLitePrefix(dsn string) string {
	for _, p := range []string{"sqlite://", "sqlite3://"} {
		if strings.HasPrefix(dsn, p) {
			return strings.TrimPrefix(dsn, p)
		}
	}
	return dsn
}
