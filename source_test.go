package migrations_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	migrations "github.com/ulexxander/go-db-migrations"
)

const migration1 = `CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`
const migration2 = `CREATE TABLE users (...);`
const migration3 = `CREATE TABLE user_sessions (...);`

var migrationContents = []string{
	migration1,
	migration2,
	migration3,
}

func createMigrationFiles(dir string) error {
	for i, content := range migrationContents {
		filename := fmt.Sprintf("%d.migration.sql", i)
		f, err := os.Create(filepath.Join(dir, filename))
		if err != nil {
			return fmt.Errorf("could not create file: %s", err)
		}
		defer f.Close()
		if _, err := f.Write([]byte(content)); err != nil {
			return fmt.Errorf("could not write into file: %s", err)
		}
	}
	return nil
}

func TestSourceDir(t *testing.T) {
	dir := t.TempDir()

	if err := createMigrationFiles(dir); err != nil {
		t.Fatalf("could not create temp migration files: %s", err)
	}

	src := migrations.SourceDir{dir}

	migrations, err := src.Migrations()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if len(migrations) < 3 {
		t.Fatalf("expected to get 3 migrations, got: %d", len(migrations))
	}

	migrationEquals(t, migrations[0], "0.migration.sql", migration1)
	migrationEquals(t, migrations[1], "1.migration.sql", migration2)
	migrationEquals(t, migrations[2], "2.migration.sql", migration3)
}

func migrationEquals(t *testing.T, m migrations.Migration, id, content string) {
	if m.ID != id {
		t.Errorf("expected id is %s, got: %s", id, m.ID)
	}
	if m.Content != content {
		t.Errorf("expected content is %s, got: %s", content, m.Content)
	}
}
