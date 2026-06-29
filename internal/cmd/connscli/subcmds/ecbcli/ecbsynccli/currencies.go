package ecbsynccli

import (
	"fmt"

	"github.com/loveyourstack/connectors/internal/cmd/connscli/cliapp"
	"github.com/spf13/cobra"
)

// example: connscli ecb sync currencies
func CurrenciesCmd(cliApp *cliapp.App) *cobra.Command {
	return &cobra.Command{
		Use:   "currencies",
		Short: "Sync currencies from ECB API into database",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			ctx := cmd.Context()

			defer cliApp.Db.Close()

			err = cliApp.EcbSvc.SyncCurrencies(ctx, cliApp.Db)
			if err != nil {
				return fmt.Errorf("ecbsvc.SyncCurrencies failed: %w", err)
			}

			cliApp.Logger.Debug("done")

			return nil
		},
	}
}
