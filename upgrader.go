package migrations

import (
	"errors"
	"fmt"
	"time"
)

type Logger interface {
	Println(v ...interface{})
}

type Upgrader struct {
	Logger   Logger
	Source   Source
	Database Database
}

type UpgradeResult struct {
	Executed []Migration
}

func (u *Upgrader) Do() (*UpgradeResult, error) {
	migrationsSrc, err := u.Source.Migrations()
	if err != nil {
		return nil, fmt.Errorf("could not read source migrations: %s", err)
	}

	if len(migrationsSrc) == 0 {
		return nil, errors.New("no migrations to run")
	}

	executedAlready, err := u.Database.ExecutedMigrations()
	if err != nil {
		return nil, fmt.Errorf("could not get already executed migrations: %s", err)
	}

	executedByID := map[string]Executed{}
	for _, mig := range executedAlready {
		executedByID[mig.ID] = mig
	}

	executedNow := make([]Migration, 0, len(migrationsSrc))
	for _, mig := range migrationsSrc {
		if _, ok := executedByID[mig.ID]; ok {
			continue
		}

		u.Println("Executing migration", mig.ID)

		start := time.Now()

		err = u.Database.Migrate(mig)
		if err != nil {
			break
		}

		duration := time.Since(start)
		u.Println("Done,", duration)

		if err := u.Database.RecordMigration(mig.ID, duration); err != nil {
			break
		}

		executedNow = append(executedNow, mig)
	}

	result := UpgradeResult{
		Executed: executedNow,
	}

	if err != nil {
		return &result, fmt.Errorf("failure during migrations execution: %s", err)
	}

	return &result, nil
}

func (u *Upgrader) Println(v ...interface{}) {
	if u.Logger != nil {
		u.Logger.Println(v...)
	}
}
