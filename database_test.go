package migrations_test

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	migrations "github.com/ulexxander/go-db-migrations"
)

func envWithDefault(key string, def string) string {
	val, ok := os.LookupEnv(key)
	if ok {
		return val
	}
	return def
}

var pgHost = envWithDefault("TEST_POSTGRES_HOST", "localhost")
var pgPort = envWithDefault("TEST_POSTGRES_PORT", "5433")
var pgUser = envWithDefault("TEST_POSTGRES_USER", "test")
var pgPass = envWithDefault("TEST_POSTGRES_PASS", "test")
var pgDBName = envWithDefault("TEST_POSTGRES_DBNAME", "test")
var pgSSLMode = envWithDefault("TEST_POSTGRES_SSLMODE", "disable")

func openDB() (*sql.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		pgHost,
		pgPort,
		pgUser,
		pgPass,
		pgDBName,
		pgSSLMode,
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres connection: %s", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %s", err)
	}

	return db, nil
}

const migrationsExecutedTable = "migrations_executed"
const table1 = "first"
const table2 = "second"
const table3 = "third"

func resetDB(db *sql.DB) error {
	var tables = []string{
		migrationsExecutedTable, table1, table2, table3,
	}

	for _, table := range tables {
		if _, err := db.Exec("DROP TABLE IF EXISTS " + table); err != nil {
			return fmt.Errorf("could not drop table %s: %s", table, err)
		}
	}

	return nil
}

func TestReturnsAndUpdatesExecutedMigrations(t *testing.T) {
	db, err := openDB()
	if err != nil {
		t.Fatalf("could not setup db: %s", err)
	}
	defer db.Close()
	if err := resetDB(db); err != nil {
		t.Fatalf("could not reset db: %s", err)
	}

	dbp := migrations.DatabasePostgres{DB: db}

	executed, err := dbp.ExecutedMigrations()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if executed != nil {
		t.Fatalf("expected no executed migrations yet")
	}

	migID := "1.sql"
	if err := dbp.RecordMigration(migID, time.Millisecond); err != nil {
		t.Fatalf("failed to record migration %s: %s", migID, err)
	}

	executed, err = dbp.ExecutedMigrations()
	if err != nil {
		t.Fatalf("unexpected error after migrations is recorded: %s", err)
	}
	if len(executed) != 1 {
		t.Fatalf("expected to get 1 executed migration, got: %d", len(executed))
	}
	executedEquals(t, executed[0], migID)
}

func TestExecutesMigrations(t *testing.T) {
	db, err := openDB()
	if err != nil {
		t.Fatalf("could not setup db: %s", err)
	}
	defer db.Close()
	if err := resetDB(db); err != nil {
		t.Fatalf("could not reset db: %s", err)
	}

	dbp := migrations.DatabasePostgres{DB: db}

	err = dbp.Migrate(migrations.Migration{
		ID:      "some.sql",
		Content: "CREATE TABLE first ( somefield TEXT NOT NULL )",
	})
	if err != nil {
		t.Fatalf("unexpected error during first migrations execution: %s", err)
	}

	executed, err := dbp.ExecutedMigrations()
	if err != nil {
		t.Fatalf("unexpected error after migrations are run: %s", err)
	}
	if executed != nil {
		// Migrate by itself does not record a migration
		t.Fatalf("expected no executed migration recorded, got: %d", len(executed))
	}
}

func TestNotAllowingToRecordSameMigrationAgain(t *testing.T) {
	db, err := openDB()
	if err != nil {
		t.Fatalf("could not setup db: %s", err)
	}
	defer db.Close()
	if err := resetDB(db); err != nil {
		t.Fatalf("could not reset db: %s", err)
	}

	dbp := migrations.DatabasePostgres{DB: db}

	// needed here to estabilish migrations_executed table initially
	dbp.ExecutedMigrations()

	migID := "1.sql"
	if err := dbp.RecordMigration(migID, time.Millisecond); err != nil {
		t.Fatalf("failed to execute migration %s for the first time: %s", migID, err)
	}

	err = dbp.RecordMigration(migID, time.Millisecond)
	if err == nil {
		t.Fatalf("expected to get error when executing migration %s again, got nil", migID)
	}
	if !strings.Contains(err.Error(), "already executed") {
		t.Fatalf("expected error to contain already executed message, got: %s", err.Error())
	}
}

func TestIsAlreadyExecuted(t *testing.T) {
	db, err := openDB()
	if err != nil {
		t.Fatalf("could not setup db: %s", err)
	}
	defer db.Close()
	if err := resetDB(db); err != nil {
		t.Fatalf("could not reset db: %s", err)
	}

	dbp := migrations.DatabasePostgres{DB: db}

	// needed here to estabilish migrations_executed table initially
	dbp.ExecutedMigrations()

	migID := "abc.sql"
	if err := dbp.RecordMigration(migID, time.Millisecond); err != nil {
		t.Fatalf("unexpected error when executing migration: %s", err)
	}

	isExecuted, err := dbp.IsAlreadyExecuted(migID)
	if err != nil {
		t.Fatalf("unexpected error when checking if migration is executed: %s", err)
	}
	if !isExecuted {
		t.Fatalf("migration should have been executed")
	}
}

func executedEquals(t *testing.T, e migrations.Executed, id string) {
	t.Helper()
	if e.ID != id {
		t.Fatalf("expected ID to be %s, got: %s", id, e.ID)
	}
	if e.ExecutedAt.IsZero() {
		t.Fatal("ExecutedAt must not be zero")
	}
	if e.DurationMS == 0 {
		t.Error("DurationMS is zero which is probably incorrect")
	}
}

func executedSliceEquals(t *testing.T, e []migrations.Executed, ids []string) {
	t.Helper()
	if len(e) != len(ids) {
		t.Fatalf("expected to have %d migrations executed, got: %d", len(ids), len(e))
	}
	for i, mig := range e {
		id := ids[i]
		executedEquals(t, mig, id)
	}
}
