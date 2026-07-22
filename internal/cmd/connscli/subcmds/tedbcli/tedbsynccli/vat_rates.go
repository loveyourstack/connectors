package tedbsynccli

import (
	"fmt"
	"time"

	"github.com/loveyourstack/connectors/internal/cmd/connscli/cliapp"
	"github.com/loveyourstack/connectors/tedb/tedbapi"
	"github.com/spf13/cobra"
)

// example: connscli tedb sync vatRates 2026-07-01 2026-07-31
func VatRatesCmd(cliApp *cliapp.App) *cobra.Command {
	return &cobra.Command{
		Use:   "vatRates",
		Short: "Sync VAT rates from TEDB API into database. Arguments are from and to date, in format YYYY-MM-DD.",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			ctx := cmd.Context()

			defer cliApp.Db.Close()

			startDate, err := time.Parse("2006-01-02", args[0])
			if err != nil {
				return fmt.Errorf("time.Parse (start) failed: %w", err)
			}
			endDate, err := time.Parse("2006-01-02", args[1])
			if err != nil {
				return fmt.Errorf("time.Parse (end) failed: %w", err)
			}

			err = cliApp.TedbSvc.SyncVatRates(ctx, cliApp.Db, startDate, endDate)
			if err != nil {
				if err == tedbapi.ErrNoRatesFound {
					cliApp.Logger.Debug("no rates returned from TEDB API for the given date range")
					return nil
				}
				return fmt.Errorf("cliApp.TedbSvc.SyncVatRates failed: %w", err)
			}

			cliApp.Logger.Debug("done")

			return nil
		},
	}
}
