package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
)

// ErrMigrationDrift means an already-recorded migration has different source
// bytes. The runner refuses to guess how to repair a changed schema.
var ErrMigrationDrift = errors.New("sgsp postgres: migration checksum mismatch")

//go:embed migrations/001_group_assignments.sql
var migration001 string

type migration struct {
	version string
	sql     string
}

var migrations = []migration{{version: "001_group_assignments", sql: migration001}}

// ApplyMigrations applies this package's schema changes in the caller's
// current PostgreSQL schema. It is deliberately separate from New: callers
// choose when and in which schema migration is authorized. Configure a
// schema-local search_path on every pooled connection before calling it.
func ApplyMigrations(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("apply migrations: %w", ErrInvalidDatabase)
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext(current_schema() || ':sgsp-migrations'))`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS sgsp_schema_migrations (
        version text PRIMARY KEY,
        checksum bytea NOT NULL,
        applied_at timestamptz NOT NULL DEFAULT now()
    )`); err != nil {
		return err
	}
	for _, migration := range migrations {
		checksum := sha256.Sum256([]byte(migration.sql))
		var stored []byte
		err := tx.QueryRowContext(ctx, `SELECT checksum FROM sgsp_schema_migrations WHERE version=$1`, migration.version).Scan(&stored)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			if _, err := tx.ExecContext(ctx, migration.sql); err != nil {
				return fmt.Errorf("apply migration %s: %w", migration.version, err)
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO sgsp_schema_migrations (version, checksum) VALUES ($1,$2)`, migration.version, checksum[:]); err != nil {
				return err
			}
		case err != nil:
			return err
		case string(stored) != string(checksum[:]):
			return fmt.Errorf("migration %s: %w", migration.version, ErrMigrationDrift)
		}
	}
	return tx.Commit()
}

// ErrInvalidDatabase keeps invalid runner input distinct from a database-side
// migration failure while retaining an ordinary Go error for host setup code.
var ErrInvalidDatabase = errors.New("sgsp postgres: nil database")
