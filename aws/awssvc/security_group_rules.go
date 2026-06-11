package awssvc

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"

	"github.com/loveyourstack/connectors/aws/awsapi"
)

var ErrNoRulesConfigured = errors.New("no security group rules configured for user")

func (svc Service) UpdateUserSecurityGroupRules(ctx context.Context, userShortname string, userIp netip.Addr) (err error) {

	// select AWS security group rule ids attached to this user
	ruleIds, err := svc.userSgRuleStore.SelectRuleIdsByUser(ctx, userShortname)
	if err != nil {
		return fmt.Errorf("svc.userSgRuleStore.SelectRuleIdsByUser failed: %w", err)
	}

	// exit with error if none found
	if len(ruleIds) == 0 {
		return ErrNoRulesConfigured
	}

	// get rules from AWS client
	rules, err := svc.client.GetApiEc2SecGroupRulesByIds(ctx, ruleIds)
	if err != nil {
		return fmt.Errorf("svc.client.GetApiEc2SecGroupRulesByIds failed: %w", err)
	}
	if len(rules) != len(ruleIds) {
		return fmt.Errorf("expected %v rules, but got %v", len(ruleIds), len(rules))
	}

	// ensure that each rule's description contains the user shortname
	for _, rule := range rules {

		// check to prevent panic below, but shouldn't happen
		if rule.SecurityGroupRuleId == nil {
			return fmt.Errorf("rule does not have a SecurityGroupRuleId")
		}

		if rule.Description == nil {
			return fmt.Errorf("rule: %v does not have a description", *rule.SecurityGroupRuleId)
		}

		desc := *rule.Description
		if !strings.Contains(strings.ToLower(desc), strings.ToLower(userShortname)) {
			return fmt.Errorf("the description (%s) of rule: %v does not contain the shortname: '%s'", desc, *rule.SecurityGroupRuleId, userShortname)
		}
	}

	// create Ec2SecGroupRules
	ec2SecGroupRules := make([]awsapi.Ec2SecGroupRule, len(rules))
	for i, rule := range rules {
		ec2SecGroupRules[i] = awsapi.Ec2SecGroupRule{
			GroupId:     rule.GroupId,
			RuleId:      rule.SecurityGroupRuleId,
			Description: rule.Description,
			IpProtocol:  rule.IpProtocol,
			FromPort:    rule.FromPort,
			ToPort:      rule.ToPort,
		}
	}

	// update rules with new IP
	err = svc.client.SetEc2SecGroupRulesIp(ctx, userIp, ec2SecGroupRules)
	if err != nil {
		return fmt.Errorf("svc.client.SetEc2SecGroupRulesIp failed: userIp: %s: %w", userIp, err)
	}

	return nil
}
