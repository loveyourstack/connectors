package rootcli

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/loveyourstack/connectors/apiclients/ecbapi"
	appCmd "github.com/loveyourstack/connectors/internal/cmd"
	"github.com/loveyourstack/connectors/internal/cmd/connscli/cliapp"
	"github.com/loveyourstack/connectors/internal/cmd/connscli/subcmds/ecbcli"
	"github.com/loveyourstack/connectors/internal/myapp"
	"github.com/loveyourstack/lys/lyserr"
	"github.com/loveyourstack/lys/lyspgdb"
	"github.com/spf13/cobra"
)

var version = "0.0.1"
var rootCmd = &cobra.Command{
	Use:           "connscli",
	Version:       version,
	Short:         "connscli - CLI tool for Connectors",
	Long:          `connscli is a CLI tool for running Connectors admin tasks`,
	SilenceErrors: true, // subcommand errors are returned upwards via RunE and handled in Execute() below
	SilenceUsage:  true,
	// no Run function: a subcommand is always needed
}

var cliApp *cliapp.App

func addSubCommands() {
	rootCmd.AddCommand(CreateTestDbCmd(cliApp))

	rootCmd.AddCommand(ecbcli.NewCmd(cliApp))
}

func Execute() {

	// set up signal handling for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ensure that context cancelation propagates to subcommands
	rootCmd.SetContext(ctx)

	conf := myapp.Config{}
	err := conf.LoadFromFile("/usr/local/etc/conns_config.toml")
	if err != nil {
		log.Fatalf("initialization: conns_config.toml not found: %s", err.Error())
	}

	// create non-specific app
	app := appCmd.NewApplication(&conf)

	// create cli app
	cliApp = &cliapp.App{
		Application: app,
	}

	// connect to db and assign pool to cliApp
	cliApp.Db, err = lyspgdb.GetPool(ctx, conf.Db, conf.DbOwnerUser, conf.General.AppName+" Cli")
	if err != nil {
		log.Fatalf("initialization: failed to create db connection pool: %s", err.Error())
	}
	defer cliApp.Db.Close()

	cliApp.EcbClient = ecbapi.NewClient(cliApp.Db, cliApp.InfoLog, cliApp.ErrorLog)

	// note that defer db Close is also needed in subcommands or else context cancelation doesn't propagate to db

	// subcommands
	addSubCommands()

	if err := rootCmd.Execute(); err != nil {
		var userErr lyserr.User
		if errors.As(err, &userErr) {
			log.Fatal(userErr)
		}
		log.Fatal(err.Error())
	}
}
