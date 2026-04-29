package db

import (
	"database/sql"

	"github.com/stevysh/stevy/internal/dialect"
	"github.com/pressly/goose/v3"
)

// Migrate runs app migrations from the embedded migrations/ directory.
func Migrate(sqlDB *sql.DB, d dialect.Dialect) error {
	goose.SetBaseFS(MigrationsFS)

	var dir, gooseDialect string
	switch d {
	case dialect.Postgres:
		dir = "migrations/postgres"
		gooseDialect = "postgres"
	case dialect.SQLite:
		dir = "migrations/sqlite"
		gooseDialect = "sqlite3"
	}

	if err := goose.SetDialect(gooseDialect); err != nil {
		return err
	}
	return goose.Up(sqlDB, dir)
}
