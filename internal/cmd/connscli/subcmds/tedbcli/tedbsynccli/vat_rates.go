package tedbsynccli

import (
	"fmt"
	"time"

	"github.com/loveyourstack/connectors/internal/cmd/connscli/cliapp"
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
				return fmt.Errorf("cliApp.TedbSvc.SyncVatRates failed: %w", err)
			}

			/*results, err := cliApp.TedbClient.GetApiVatRates(ctx, []string{"DE"}, startDate, endDate)
			if err != nil {
				return fmt.Errorf("cliApp.TedbClient.GetApiVatRates failed: %w", err)
			}

			for _, r := range results {
				fmt.Printf("MemberState: %s ", r.MemberState)
				fmt.Printf("Type: %s ", r.Type)
				fmt.Printf("RateType: %s ", r.Rate.Type)

				rate := "null"
				if r.Rate.Value != nil {
					rate = fmt.Sprintf("%f", *r.Rate.Value)
				}
				fmt.Printf("Rate: %s ", rate)

				fmt.Printf("SituationOn: %s ", r.SituationOn)
				if r.CNCodes != nil {
					fmt.Printf("CN Codes: ")
					for _, c := range r.CNCodes.Code {
						fmt.Printf("%s ", c.Value)
					}
				}
				if r.CPACodes != nil {
					fmt.Printf("CPA Codes: ")
					for _, c := range r.CPACodes.Code {
						fmt.Printf("%s ", c.Value)
					}
				}
				if r.Category != nil {
					fmt.Printf("Category desc: %s ", r.Category.Description)
					fmt.Printf("Category id: %s ", r.Category.Identifier)
				}
				fmt.Printf("Comment: %s ", r.Comment)
				fmt.Printf("\n")
			}*/

			cliApp.Logger.Debug("done")

			return nil
		},
	}
}
