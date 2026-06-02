package mmcli

import (
	"fmt"
	"strconv"

	"github.com/loveyourstack/connectors/internal/cmd/connscli/cliapp"
	"github.com/spf13/cobra"
)

func WriteGeo2LiteCityCmd(cliApp *cliapp.App) *cobra.Command {
	return &cobra.Command{
		Use:  "writeGeo2LiteCity [getNewZip]", // pass 0 or 1 for getNewZip
		Long: "Writes GeoLite2 City data to the database. If getNewZip is 1, it will fetch a new zip file from the MaxMind API before writing to the database.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			ctx := cmd.Context()

			defer cliApp.Db.Close()

			// parse getNewZip argument
			getNewZip, err := strconv.ParseBool(args[0])
			if err != nil {
				return fmt.Errorf("invalid value for getNewZip: %w", err)
			}

			err = cliApp.MaxMindSvc.WriteGeo2LiteCity(ctx, cliApp.Db, getNewZip)
			if err != nil {
				return fmt.Errorf("cliApp.MaxMindSvc.WriteGeo2LiteCity failed: %w", err)
			}

			return nil
		},
	}
}
