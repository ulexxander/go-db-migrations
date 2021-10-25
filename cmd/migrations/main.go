package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"

	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	migrations "github.com/ulexxander/go-db-migrations"
)

type args struct {
	// common args
	cmd string
	dir string
	dsn string

	// record command args
	migrationID string
}

func main() {
	logger := logrus.New()
	fl := parseArgs()
	if err := run(fl, logger); err != nil {
		logger.Fatalf("error: %s", err)
	}
}

func parseArgs() args {
	flagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	dir := flagSet.String("dir", "", "migrations source directory")
	dsn := flagSet.String("dsn", "", "postgres connection string (dsn)")
	migrationID := flagSet.String("migration-id", "", "migration id to force add in record command")

	err := flagSet.Parse(os.Args[1:])
	if err == flag.ErrHelp {
		flagSet.Usage()
	}

	return args{
		cmd:         flagSet.Arg(0),
		dir:         *dir,
		dsn:         *dsn,
		migrationID: *migrationID,
	}
}

func run(a args, logger *logrus.Logger) error {
	if a.dir == "" {
		return fmt.Errorf("dir flag can not be empty")
	}
	if a.dsn == "" {
		return fmt.Errorf("dsn flag can not be empty")
	}

	pgdb, err := openPostgres(a.dsn)
	if pgdb != nil {
		defer pgdb.Close()
	}
	if err != nil {
		return fmt.Errorf("failed to setup database: %s", err)
	}

	src := migrations.SourceDir{Dir: a.dir}
	dbp := migrations.DatabasePostgres{DB: pgdb}

	switch a.cmd {
	case "status":
		return cmdStatus(&dbp, logger)
	case "upgrade":
		return cmdUpgrade(&src, &dbp, logger)
	case "record":
		return cmdRecord(a.migrationID, &dbp, logger)
	case "":
		return fmt.Errorf("command is required, available are: state, upgrade, force")
	default:
		return fmt.Errorf("unknown command: %s", a.cmd)
	}
}

func cmdStatus(db migrations.Database, logger *logrus.Logger) error {
	logger.Println("Loading database status...")

	executed, err := db.ExecutedMigrations()
	if err != nil {
		return fmt.Errorf("could not get database executed migrations: %s", err)
	}

	logger.Println("Successfully loaded database last state")

	if len(executed) == 0 {
		logger.Println("No migrations executed yet")
		return nil
	}

	last := executed[0]
	logger.Println("Last migration executed:", last.ID, "at", last.ExecutedAt)

	return nil
}

func cmdUpgrade(src migrations.Source, db migrations.Database, logger *logrus.Logger) error {
	logger.Println("Performing upgrade...")

	u := migrations.Upgrader{
		Logger:   logger,
		Source:   src,
		Database: db,
	}

	result, err := u.Do()
	if result != nil {
		logger.WithField("count", len(result.Executed)).Println("Migrations executed")
	}
	if err != nil {
		return err
	}

	if len(result.Executed) == 0 {
		logger.Println("No upgrade was needed")
	}

	logger.Println("No errors occurred")
	return nil
}

func cmdRecord(migID string, db migrations.Database, logger *logrus.Logger) error {
	if migID == "" {
		return errors.New("migration id cannot be empty")
	}

	log := logger.WithField("migrationID", migID)

	log.Println("Force adding already executed migration record...")

	alreadyExecuted, err := db.IsAlreadyExecuted(migID)
	if err != nil {
		return fmt.Errorf("could not check if migration is already executed: %s", err)
	}

	if alreadyExecuted {
		return fmt.Errorf("migration %s is already executed", migID)
	}

	if err := db.RecordMigration(migID, 0); err != nil {
		return fmt.Errorf("failed to record migration: %s", err)
	}

	log.Println("Force added executed migration")
	return nil
}

func openPostgres(dsn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("could not open postgres: %s", err)
	}

	if err := db.Ping(); err != nil {
		return db, fmt.Errorf("failed to ping postgres: %s", err)
	}

	return db, nil
}
