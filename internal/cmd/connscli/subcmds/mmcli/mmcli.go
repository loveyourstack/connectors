package mmcli

import (
	"github.com/loveyourstack/connectors/internal/cmd/connscli/cliapp"
	"github.com/spf13/cobra"
)

func NewCmd(cliApp *cliapp.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "maxmind",
		Short: "MaxMind commands",
	}

	cmd.AddCommand(GetGeo2LiteCityLmCmd(cliApp))
	cmd.AddCommand(WriteGeo2LiteCityCmd(cliApp))

	return cmd
}
