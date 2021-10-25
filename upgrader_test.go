package migrations_test

import (
	"errors"
	"testing"
	"time"

	migrations "github.com/ulexxander/go-db-migrations"
)

type DatabaseMock struct {
	executed []migrations.Executed

	migrateOverride   bool
	migrateFailsAfter int
}

var someDBError = "some db error"

func (dm *DatabaseMock) ExecutedMigrations() ([]migrations.Executed, error) {
	return dm.executed, nil
}

func (dm *DatabaseMock) RecordMigration(id string, duration time.Duration) error {
	dm.executed = append(dm.executed, migrations.Executed{
		ID:         id,
		DurationMS: int(time.Millisecond),
		ExecutedAt: time.Now(),
	})
	return nil
}

func (dm *DatabaseMock) Migrate(mig migrations.Migration) error {
	if dm.migrateOverride {
		if dm.migrateFailsAfter == 0 {
			return errors.New(someDBError)
		}
		dm.migrateFailsAfter--
	}
	return nil
}

func (dm *DatabaseMock) IsAlreadyExecuted(id string) (bool, error) {
	panic("not implemented")
}

func TestUpgrader(t *testing.T) {
	db := DatabaseMock{}

	mig1ID := "first.sql"
	mig2ID := "second.sql"
	migrations1 := []migrations.Migration{
		{ID: mig1ID},
		{ID: mig2ID},
	}

	t.Run("first two migrations", func(t *testing.T) {
		src := migrations.DirectSource(migrations1)
		u := migrations.Upgrader{
			Source:   &src,
			Database: &db,
		}

		result, err := u.Do()
		if err != nil {
			t.Fatalf("unexpected upgrader error: %s", err)
		}
		resultEquals(t, result, migrations1)
		executedSliceEquals(t, db.executed, []string{
			mig1ID,
			mig2ID,
		})
	})

	mig3ID := "third.sql"
	mig4ID := "forth.sql"
	migrations2 := []migrations.Migration{
		{ID: mig3ID},
		{ID: mig4ID},
	}

	t.Run("another two migrations", func(t *testing.T) {
		src := migrations.DirectSource(append(migrations1, migrations2...))
		u := migrations.Upgrader{
			Source:   &src,
			Database: &db,
		}

		result, err := u.Do()
		if err != nil {
			t.Fatalf("unexpected upgrader error: %s", err)
		}
		resultEquals(t, result, migrations2)
		executedSliceEquals(t, db.executed, []string{
			mig1ID,
			mig2ID,
			mig3ID,
			mig4ID,
		})
	})

	t.Run("no migrations needed", func(t *testing.T) {
		src := migrations.DirectSource(append(migrations1, migrations2...))
		u := migrations.Upgrader{
			Source:   &src,
			Database: &db,
		}

		result, err := u.Do()
		if err != nil {
			t.Fatalf("unexpected upgrader error: %s", err)
		}
		resultEquals(t, result, nil)
		executedSliceEquals(t, db.executed, []string{
			mig1ID,
			mig2ID,
			mig3ID,
			mig4ID,
		})
	})

	mig5ID := "last.sql"
	migrations3 := []migrations.Migration{
		{ID: mig5ID},
	}

	t.Run("last one", func(t *testing.T) {
		src := migrations.DirectSource(append(migrations1, append(migrations2, migrations3...)...))
		u := migrations.Upgrader{
			Source:   &src,
			Database: &db,
		}

		result, err := u.Do()
		if err != nil {
			t.Fatalf("unexpected upgrader error: %s", err)
		}
		resultEquals(t, result, migrations3)
		executedSliceEquals(t, db.executed, []string{
			mig1ID,
			mig2ID,
			mig3ID,
			mig4ID,
			mig5ID,
		})
	})
}

func TestErrorsDuringMigrations(t *testing.T) {
	db := DatabaseMock{}

	migSuccessID := "first_successful.sql"
	migrations1 := []migrations.Migration{
		{ID: migSuccessID},
	}

	t.Run("first successful", func(t *testing.T) {
		src := migrations.DirectSource(migrations1)
		u := migrations.Upgrader{
			Source:   &src,
			Database: &db,
		}

		result, err := u.Do()
		if err != nil {
			t.Fatalf("unexpected upgrader error: %s", err)
		}
		resultEquals(t, result, migrations1)
		executedSliceEquals(t, db.executed, []string{
			migSuccessID,
		})
	})

	migFailureOneID := "failure_numba_one.sql"
	migrations2 := []migrations.Migration{
		{ID: migFailureOneID},
	}

	t.Run("second fails", func(t *testing.T) {
		db.migrateOverride = true

		src := migrations.DirectSource(append(migrations1, migrations2...))
		u := migrations.Upgrader{
			Source:   &src,
			Database: &db,
		}

		result, err := u.Do()
		if err == nil {
			t.Errorf("expected to get error, got nil")
		}
		resultEquals(t, result, nil)
		executedSliceEquals(t, db.executed, []string{
			migSuccessID,
		})
	})

	t.Run("restores failed migration", func(t *testing.T) {
		db.migrateOverride = false

		src := migrations.DirectSource(append(migrations1, migrations2...))
		u := migrations.Upgrader{
			Source:   &src,
			Database: &db,
		}

		result, err := u.Do()
		if err != nil {
			t.Fatalf("unexpected upgrader error: %s", err)
		}
		resultEquals(t, result, migrations2)
		executedSliceEquals(t, db.executed, []string{
			migSuccessID,
			migFailureOneID,
		})
	})

	migOK1ID := "ok_1.sql"
	migOK2ID := "ok_2.sql"
	migOK3ID := "ok_3.sql"
	migNOTOK1ID := "not_ok_1.sql"
	migNOTOK2ID := "not_ok_2.sql"
	migNOTOK3ID := "not_ok_3.sql"
	migNOTOK4ID := "not_ok_4.sql"
	migrations3 := []migrations.Migration{
		{ID: migOK1ID},
		{ID: migOK2ID},
		{ID: migOK3ID},
		{ID: migNOTOK1ID},
		{ID: migNOTOK2ID},
		{ID: migNOTOK3ID},
		{ID: migNOTOK4ID},
	}

	t.Run("4/7 migrations have failed", func(t *testing.T) {
		db.migrateOverride = true
		db.migrateFailsAfter = 3

		src := migrations.DirectSource(append(migrations1, append(migrations2, migrations3...)...))
		u := migrations.Upgrader{
			Source:   &src,
			Database: &db,
		}

		result, err := u.Do()
		if err == nil {
			t.Errorf("expected to get error, got nil")
		}
		resultEquals(t, result, migrations3[:3])
		executedSliceEquals(t, db.executed, []string{
			migSuccessID,
			migFailureOneID,
			migOK1ID,
			migOK2ID,
			migOK3ID,
		})
	})

	migFinalID := "final.sql"
	migrations4 := []migrations.Migration{
		{ID: migFinalID},
	}

	t.Run("restores 4 failed migrations and executes final one", func(t *testing.T) {
		db.migrateOverride = false

		src := migrations.DirectSource(append(migrations1, append(migrations2, append(migrations3, migrations4...)...)...))
		u := migrations.Upgrader{
			Source:   &src,
			Database: &db,
		}

		result, err := u.Do()
		if err != nil {
			t.Fatalf("unexpected upgrader error: %s", err)
		}
		failedOnes := migrations3[3:]
		resultEquals(t, result, append(failedOnes, migrations4...))
		executedSliceEquals(t, db.executed, []string{
			migSuccessID,
			migFailureOneID,
			migOK1ID,
			migOK2ID,
			migOK3ID,
			migNOTOK1ID,
			migNOTOK2ID,
			migNOTOK3ID,
			migNOTOK4ID,
			migFinalID,
		})
	})
}

func resultEquals(t *testing.T, r *migrations.UpgradeResult, executedNow []migrations.Migration) {
	t.Helper()
	if r == nil {
		t.Fatalf("did not expect result to be nil")
	}
	if len(r.Executed) != len(executedNow) {
		t.Fatalf("executed migrations count should be %d, got: %d", len(executedNow), len(r.Executed))
	}
	for i, e := range r.Executed {
		expected := executedNow[i]
		if e.Content != expected.Content {
			t.Errorf("executed migration %d content should be %s, got: %s", i, expected.Content, e.Content)
		}
	}
}
