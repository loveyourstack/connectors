package ecbsynccli

import (
	"fmt"
	"time"

	"github.com/loveyourstack/connectors/apiclients/ecbapi"
	"github.com/loveyourstack/connectors/csyncdb"
	"github.com/loveyourstack/connectors/internal/cmd/connscli/cliapp"
	"github.com/spf13/cobra"
)

// example: connscli ecb sync xr 2024-10-10 2024-10-21
func ExchangeRatesCmd(cliApp *cliapp.App) *cobra.Command {
	return &cobra.Command{
		Use:   "xr",
		Short: "Sync exchange rates with base currency EUR from ECB API into database. Arguments are from and to date, in format YYYY-MM-DD.",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) (err error) {

			defer cliApp.Db.Close()

			startDate, err := time.Parse("2006-01-02", args[0])
			if err != nil {
				return fmt.Errorf("time.Parse (start) failed: %w", err)
			}
			endDate, err := time.Parse("2006-01-02", args[1])
			if err != nil {
				return fmt.Errorf("time.Parse (end) failed: %w", err)
			}

			// daily
			err = csyncdb.EcbExchangeRates(cmd.Context(), cliApp.Db, cliApp.EcbClient, "EUR", ecbapi.Daily, startDate, endDate, cliApp.InfoLog)
			if err != nil {
				return fmt.Errorf("csyncdb.EcbExchangeRates (Daily) failed: %w", err)
			}

			// monthly
			/*err = csyncdb.EcbExchangeRates(cmd.Context(), cliApp.Db, cliApp.EcbClient, "EUR", ecbapi.Monthly, startDate, endDate)
			if err != nil {
				return fmt.Errorf("csyncdb.EcbExchangeRates (Monthly) failed: %w", err)
			}*/

			cliApp.InfoLog.Debug("done")

			return nil
		},
	}
}
