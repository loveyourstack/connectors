package ecbcli

import (
	"github.com/loveyourstack/connectors/internal/cmd/connscli/cliapp"
	"github.com/loveyourstack/connectors/internal/cmd/connscli/subcmds/ecbcli/ecbsynccli"
	"github.com/spf13/cobra"
)

func NewCmd(cliApp *cliapp.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ecb",
		Short: "ECB connector commands",
	}

	cmd.AddCommand(ecbsynccli.NewCmd(cliApp))

	return cmd
}
