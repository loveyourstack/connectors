package ecbsynccli

import (
	"fmt"

	"github.com/loveyourstack/connectors/csyncdb"
	"github.com/loveyourstack/connectors/internal/cmd/connscli/cliapp"
	"github.com/spf13/cobra"
)

func CurrenciesCmd(cliApp *cliapp.App) *cobra.Command {
	return &cobra.Command{
		Use:   "currencies",
		Short: "Sync currencies from ECB API into database",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) (err error) {

			defer cliApp.Db.Close()

			err = csyncdb.EcbCurrencies(cmd.Context(), cliApp.Db, cliApp.EcbClient, cliApp.InfoLog)
			if err != nil {
				return fmt.Errorf("csyncdb.EcbCurrencies failed: %w", err)
			}

			cliApp.InfoLog.Debug("done")

			return nil
		},
	}
}
