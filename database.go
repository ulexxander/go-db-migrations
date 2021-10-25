package migrations

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
)

type Executed struct {
	ID         string
	DurationMS int
	ExecutedAt time.Time
}

type Database interface {
	// ExecutedMigrations should return all executed migrations in DESC order
	ExecutedMigrations() ([]Executed, error)
	RecordMigration(id string, duration time.Duration) error
	Migrate(mig Migration) error
	IsAlreadyExecuted(id string) (bool, error)
}

type DatabasePostgres struct {
	DB *sql.DB
}

const migrationsExecutedTable = "migrations_executed"

var createTableQuery = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	id text PRIMARY KEY,
	duration_ms int NOT NULL,
	executed_at timestamptz NOT NULL DEFAULT NOW()
)`, migrationsExecutedTable)

var selectExecutedMigrationsAllQuery = fmt.Sprintf(`SELECT * FROM %s
ORDER BY executed_at DESC`, migrationsExecutedTable)

var selectExecutedMigrationQuery = fmt.Sprintf(`SELECT * FROM %s
WHERE id = $1`, migrationsExecutedTable)

var insertMigrationQuery = fmt.Sprintf(`INSERT INTO %s (id, duration_ms)
VALUES ($1, $2)`, migrationsExecutedTable)

func (dp *DatabasePostgres) ExecutedMigrations() ([]Executed, error) {
	_, err := dp.DB.Exec(createTableQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to create %s if not exists: %s", migrationsExecutedTable, err)
	}

	rows, err := dp.DB.Query(selectExecutedMigrationsAllQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to select %s: %s", migrationsExecutedTable, err)
	}
	defer rows.Close()

	var result []Executed
	for rows.Next() {
		var item Executed
		if err := rows.Scan(
			&item.ID,
			&item.DurationMS,
			&item.ExecutedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan executed migration: %s", err)
		}
		result = append(result, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to select %s (rows containing error): %s", migrationsExecutedTable, err)
	}

	return result, nil
}

func (dp *DatabasePostgres) RecordMigration(id string, duration time.Duration) error {
	_, err := dp.DB.Exec(insertMigrationQuery, id, duration.Milliseconds())
	if err != nil {
		if isPrimaryKeyErr(err) {
			return fmt.Errorf("migration %s is already executed", id)
		}
		return fmt.Errorf("could not insert record in migrations_executed: %s", err)
	}
	return nil
}

func (dp *DatabasePostgres) Migrate(mig Migration) error {
	tx, err := dp.DB.Begin()
	if err != nil {
		return fmt.Errorf("could not begin transaction: %s", err)
	}

	if _, err := tx.Exec(mig.Content); err != nil {
		tx.Rollback()
		return err
	}

	tx.Commit()
	return nil
}

func (dp *DatabasePostgres) IsAlreadyExecuted(id string) (bool, error) {
	row := dp.DB.QueryRow(selectExecutedMigrationQuery, id)
	var executed Executed
	err := row.Scan(
		&executed.ID,
		&executed.DurationMS,
		&executed.ExecutedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func isPrimaryKeyErr(err error) bool {
	switch err := err.(type) {
	case *pq.Error:
		if err.Code == "23505" {
			return true
		}
	}
	return false
}
