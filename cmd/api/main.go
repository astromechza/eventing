package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/astromechza/eventing/internal/model/sqlmodel"
)

func main() {
	if err := mainInner(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func mainInner() error {
	var connString string
	fs := flag.NewFlagSet("eventing-api", flag.ExitOnError)
	fs.StringVar(&connString, "conn", "", "postgres database connection string")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	} else if fs.NArg() > 0 {
		return errors.New("no positional arguments allowed")
	} else if connString == "" {
		return errors.New("a value is expected for the connection string")
	}

	slog.Info("Getting started..")
	model, err := sqlmodel.New(context.Background(), connString)
	if err != nil {
		return fmt.Errorf("failed to setup database: %w", err)
	}
	defer model.Close()

	return nil
}
