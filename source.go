package migrations

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

type Migration struct {
	ID      string
	Content string
}

type Source interface {
	Migrations() ([]Migration, error)
}

type SourceDir struct {
	Dir string
}

func (sr *SourceDir) Migrations() ([]Migration, error) {
	paths, err := sr.filepaths()
	if err != nil {
		return nil, fmt.Errorf("could get migration files paths: %s", err)
	}

	migrations := make([]Migration, 0, len(paths))
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("could not open file %s: %s", path, err)
		}
		defer f.Close()

		content, err := io.ReadAll(f)
		if err != nil {
			return nil, fmt.Errorf("could not read contents of file %s: %s", path, err)
		}

		migrations = append(migrations, Migration{
			ID:      filepath.Base(path),
			Content: string(content),
		})
	}

	return migrations, nil
}

func (sr *SourceDir) filepaths() ([]string, error) {
	var paths []string
	err := filepath.Walk(sr.Dir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		paths = append(paths, path)
		return err
	})
	return paths, err
}

type SourceDirect []Migration

func (sd SourceDirect) Migrations() ([]Migration, error) {
	return sd, nil
}
