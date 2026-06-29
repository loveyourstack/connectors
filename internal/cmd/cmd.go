package cmd

import (
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/loveyourstack/connectors/internal/myapp"
	"github.com/loveyourstack/lys/lyslog"
)

// Application contains the fields common to all commands
type Application struct {
	Config *myapp.Config
	Logger *slog.Logger
	Db     *pgxpool.Pool
}

// NewApplication returns an Application with default settings. Not all fields get initialized.
func NewApplication(conf *myapp.Config) (app *Application) {

	return &Application{
		Config: conf,
		Logger: slog.New(lyslog.NewSplitStreamHandler(os.Stdout, os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
}
