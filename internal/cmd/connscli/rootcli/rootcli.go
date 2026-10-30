package rootcli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/loveyourstack/connectors/aws/awsapi"
	"github.com/loveyourstack/connectors/aws/awssvc"
	"github.com/loveyourstack/connectors/ecb/ecbapi"
	"github.com/loveyourstack/connectors/ecb/ecbsvc"
	appCmd "github.com/loveyourstack/connectors/internal/cmd"
	"github.com/loveyourstack/connectors/internal/cmd/connscli/cliapp"
	"github.com/loveyourstack/connectors/internal/cmd/connscli/subcmds/awspcli"
	"github.com/loveyourstack/connectors/internal/cmd/connscli/subcmds/ecbcli"
	"github.com/loveyourstack/connectors/internal/cmd/connscli/subcmds/mmcli"
	"github.com/loveyourstack/connectors/internal/cmd/connscli/subcmds/tedbcli"
	"github.com/loveyourstack/connectors/internal/myapp"
	"github.com/loveyourstack/connectors/internal/stores/sysextdatasync"
	"github.com/loveyourstack/connectors/maxmind/mmapi"
	"github.com/loveyourstack/connectors/maxmind/mmsvc"
	"github.com/loveyourstack/connectors/tedb/tedbapi"
	"github.com/loveyourstack/connectors/tedb/tedbsvc"
	"github.com/loveyourstack/lys/lyspgdb"
	"github.com/spf13/cobra"
)

var version = "0.0.1"
var rootCmd = &cobra.Command{
	Use:           "connscli",
	Version:       version,
	Short:         "connscli - CLI tool for Connectors",
	Long:          `connscli is a CLI tool for running Connectors admin tasks`,
	SilenceErrors: true, // subcommand errors are returned upwards via RunE to Run() below, then to main()
	// no Run function: a subcommand is always needed
}

var cliApp *cliapp.App

func addSubCommands() {
	rootCmd.AddCommand(awspcli.NewCmd(cliApp))
	rootCmd.AddCommand(ecbcli.NewCmd(cliApp))
	rootCmd.AddCommand(mmcli.NewCmd(cliApp))
	rootCmd.AddCommand(tedbcli.NewCmd(cliApp))
}

func Run() error {

	// set up signal handling for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ensure that context cancelation propagates to subcommands
	rootCmd.SetContext(ctx)

	conf := myapp.Config{}
	err := conf.LoadFromFile("/usr/local/etc/conns_config.toml")
	if err != nil {
		return fmt.Errorf("conf.LoadFromFile failed: %w", err)
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
		return fmt.Errorf("lyspgdb.GetPool failed: %w", err)
	}
	defer cliApp.Db.Close()

	// attach API clients
	cliApp.AwsClient, err = awsapi.NewClient(conf.Aws, cliApp.Db, cliApp.Logger)
	if err != nil {
		return fmt.Errorf("awsapi.NewClient failed: %w", err)
	}
	cliApp.EcbClient = ecbapi.NewClient(cliApp.Db, cliApp.Logger)
	cliApp.MaxMindClient, err = mmapi.NewClient(cliApp.Config.MaxMind, cliApp.Db, cliApp.Logger)
	if err != nil {
		return fmt.Errorf("mmapi.NewClient failed: %w", err)
	}
	cliApp.TedbClient = tedbapi.NewClient(cliApp.Db, cliApp.Logger)

	// attach services
	syncStore := sysextdatasync.Store{Db: cliApp.Db}
	cliApp.AwsSvc, err = awssvc.NewService(cliApp.Db, cliApp.AwsClient, cliApp.Logger)
	if err != nil {
		return fmt.Errorf("awssvc.NewService failed: %w", err)
	}
	cliApp.EcbSvc = ecbsvc.NewServiceWithSyncStore(cliApp.EcbClient, cliApp.Logger, syncStore)
	cliApp.MaxMindSvc, err = mmsvc.NewServiceWithSyncStore(cliApp.MaxMindClient, cliApp.Config.General.DownloadsPath, cliApp.Logger, syncStore)
	if err != nil {
		return fmt.Errorf("mmsvc.NewServiceWithSyncStore failed: %w", err)
	}
	cliApp.TedbSvc = tedbsvc.NewServiceWithSyncStore(cliApp.TedbClient, cliApp.Db, cliApp.Logger, syncStore)

	// subcommands
	addSubCommands()

	// execute the root command, which runs the appropriate subcommand based on user input
	return rootCmd.Execute()
}
