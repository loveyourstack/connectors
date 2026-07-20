package tedbcli

import (
	"github.com/loveyourstack/connectors/internal/cmd/connscli/cliapp"
	"github.com/loveyourstack/connectors/internal/cmd/connscli/subcmds/tedbcli/tedbsynccli"
	"github.com/spf13/cobra"
)

func NewCmd(cliApp *cliapp.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tedb",
		Short: "TEDB connector commands",
	}

	cmd.AddCommand(tedbsynccli.NewCmd(cliApp))

	return cmd
}
