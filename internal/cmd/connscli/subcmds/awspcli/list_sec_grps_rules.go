package awspcli

import (
	"fmt"

	"github.com/loveyourstack/connectors/internal/cmd/connscli/cliapp"
	"github.com/spf13/cobra"
)

func ListSecGrpRulesCmd(cliApp *cliapp.App) *cobra.Command {
	return &cobra.Command{
		Use:   "listSecGrpRules [security group ID]",
		Short: "List security group rules",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			ctx := cmd.Context()

			defer cliApp.Db.Close()

			secGrpRules, err := cliApp.AwsClient.GetApiEc2SecGroupRulesByGroup(ctx, args[0])
			if err != nil {
				return fmt.Errorf("cliApp.AwsClient.GetApiEc2SecGroupRulesByGroup failed: %w", err)
			}

			for _, secGrpRule := range secGrpRules {

				var desc string
				if secGrpRule.Description != nil {
					desc = *secGrpRule.Description
				}

				direction := "inbound"
				if *secGrpRule.IsEgress {
					direction = "outbound"
				}

				ipv4 := "none"
				if secGrpRule.CidrIpv4 != nil {
					ipv4 = *secGrpRule.CidrIpv4
				}

				ipv6 := "none"
				if secGrpRule.CidrIpv6 != nil {
					ipv6 = *secGrpRule.CidrIpv6
				}

				ipProtocol := ""
				if secGrpRule.IpProtocol != nil {
					ipProtocol = *secGrpRule.IpProtocol
				}

				fmt.Printf("%s - %s - %s - v4: %s - v6: %s - %s\n", direction, *secGrpRule.SecurityGroupRuleId, desc, ipv4, ipv6, ipProtocol)
			}

			return nil
		},
	}
}
