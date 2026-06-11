package awspcli

import (
	"fmt"

	"github.com/loveyourstack/connectors/internal/cmd/connscli/cliapp"
	"github.com/spf13/cobra"
)

func ListSecGrpsCmd(cliApp *cliapp.App) *cobra.Command {
	return &cobra.Command{
		Use:   "listSecGrps",
		Short: "List security groups",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			ctx := cmd.Context()

			defer cliApp.Db.Close()

			secGrps, err := cliApp.AwsClient.GetApiEc2SecGroups(ctx)
			if err != nil {
				return fmt.Errorf("cliApp.AwsClient.GetApiEc2SecGroups failed: %w", err)
			}

			for _, secGrp := range secGrps {
				fmt.Printf("%s - %s\n", *secGrp.GroupId, *secGrp.GroupName)
			}

			return nil
		},
	}
}
