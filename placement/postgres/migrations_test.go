package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestApplyMigrationsRejectsNilDatabase(t *testing.T) {
	if err := ApplyMigrations(context.Background(), nil); !errors.Is(err, ErrInvalidDatabase) {
		t.Fatalf("ApplyMigrations(nil) = %v", err)
	}
}

func TestEmbeddedMigrationsHaveStableDefinitions(t *testing.T) {
	seen := make(map[string]struct{}, len(migrations))
	for _, migration := range migrations {
		if migration.version == "" || strings.TrimSpace(migration.sql) == "" {
			t.Fatalf("invalid migration: %#v", migration)
		}
		if _, exists := seen[migration.version]; exists {
			t.Fatalf("duplicate migration version %q", migration.version)
		}
		seen[migration.version] = struct{}{}
	}
	if !strings.Contains(migration001, "CREATE TABLE sgsp_group_assignments") {
		t.Fatal("assignment migration is not embedded")
	}
}
