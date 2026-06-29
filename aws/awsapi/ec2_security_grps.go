package awsapi

import (
	"context"
	"fmt"
	"net/netip"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/ec2"
	awsTypes "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/loveyourstack/connectors/aws/stores/awsapicall"
)

// GetApiEc2SecGroups returns all EC2 security groups in the configured AWS account and region.
func (c *Client) GetApiEc2SecGroups(ctx context.Context) (secGroups []awsTypes.SecurityGroup, err error) {

	// make EC2 client if needed
	err = c.makeEc2Client(ctx)
	if err != nil {
		return nil, fmt.Errorf("c.makeEc2Client failed: %w", err)
	}

	// prepare call log input
	callInput := awsapicall.Input{
		Attempt:    1, // no retry mechanism yet
		DurationMs: 0, // set in defer
		Endpoint:   "ec2Client.DescribeSecurityGroups",
		Page:       0,  // set in loop below
		Result:     "", // set below depending on success or error
	}

	start := time.Now()

	// defer call log to capture duration and result
	defer func() {
		callInput.DurationMs = time.Since(start).Milliseconds()

		_, err := c.callStore.Insert(context.Background(), callInput) // use background context to ensure call log is inserted even if main context is cancelled
		if err != nil {
			c.logger.Error("c.callStore.Insert failed", "error", err, "callInput", callInput)
		}
	}()

	// page through security groups
	nextToken := new(string)
	for {
		callInput.Page++

		input := &ec2.DescribeSecurityGroupsInput{}
		if nextToken != nil && *nextToken != "" {
			input.NextToken = nextToken
		}

		secGrpsOutput, err := c.ec2Client.DescribeSecurityGroups(ctx, input)
		if err != nil {
			callInput.Result = err.Error()
			return nil, fmt.Errorf("c.ec2Client.DescribeSecurityGroups failed: %w", err)
		}
		secGroups = append(secGroups, secGrpsOutput.SecurityGroups...)

		if secGrpsOutput.NextToken == nil {
			break
		}
		nextToken = secGrpsOutput.NextToken
	}

	callInput.Result = "OK"
	return secGroups, nil
}

// getApiEc2SecGroupRules is a helper method that returns EC2 security group rules based on the provided filter.
func (c *Client) getApiEc2SecGroupRules(ctx context.Context, filter awsTypes.Filter) (secGroupRules []awsTypes.SecurityGroupRule, err error) {

	// make EC2 client if needed
	err = c.makeEc2Client(ctx)
	if err != nil {
		return nil, fmt.Errorf("c.makeEc2Client failed: %w", err)
	}

	// prepare call log input
	callInput := awsapicall.Input{
		Attempt:    1, // no retry mechanism yet
		DurationMs: 0, // set in defer
		Endpoint:   "ec2Client.DescribeSecurityGroupRules",
		Page:       0,  // set in loop below
		Result:     "", // set below depending on success or error
	}

	start := time.Now()

	// defer call log to capture duration and result
	defer func() {
		callInput.DurationMs = time.Since(start).Milliseconds()

		_, err := c.callStore.Insert(context.Background(), callInput) // use background context to ensure call log is inserted even if main context is cancelled
		if err != nil {
			c.logger.Error("c.callStore.Insert failed", "error", err, "callInput", callInput)
		}
	}()

	// page through security group rules
	nextToken := new(string)
	for {
		callInput.Page++

		input := &ec2.DescribeSecurityGroupRulesInput{
			Filters: []awsTypes.Filter{filter},
		}
		if nextToken != nil && *nextToken != "" {
			input.NextToken = nextToken
		}

		secGrpsRulesOutput, err := c.ec2Client.DescribeSecurityGroupRules(ctx, input)
		if err != nil {
			callInput.Result = err.Error()
			return nil, fmt.Errorf("c.ec2Client.DescribeSecurityGroupRules failed: %w", err)
		}
		secGroupRules = append(secGroupRules, secGrpsRulesOutput.SecurityGroupRules...)

		if secGrpsRulesOutput.NextToken == nil {
			break
		}
		nextToken = secGrpsRulesOutput.NextToken
	}

	callInput.Result = "OK"
	return secGroupRules, nil
}

// GetApiEc2SecGroupRulesByGroup returns EC2 security group rules for the specified security group ID.
func (c *Client) GetApiEc2SecGroupRulesByGroup(ctx context.Context, secGroupId string) (secGroupRules []awsTypes.SecurityGroupRule, err error) {
	return c.getApiEc2SecGroupRules(ctx, awsTypes.Filter{Name: new("group-id"), Values: []string{secGroupId}})
}

// GetApiEc2SecGroupRulesByIds returns EC2 security group rules for the specified security group rule IDs.
func (c *Client) GetApiEc2SecGroupRulesByIds(ctx context.Context, secGroupRuleIds []string) (secGroupRules []awsTypes.SecurityGroupRule, err error) {
	return c.getApiEc2SecGroupRules(ctx, awsTypes.Filter{Name: new("security-group-rule-id"), Values: secGroupRuleIds})
}

// Ec2SecGroupRule is a simplified struct for EC2 security group rules that contains only the fields needed for updating the rule's IP.
type Ec2SecGroupRule struct {
	GroupId     *string
	RuleId      *string
	Description *string
	IpProtocol  *string
	FromPort    *int32
	ToPort      *int32
}

// SetEc2SecGroupRulesIp updates the IP address for the provided EC2 security group rules to the new IP address.
// It supports both IPv4 and IPv6 addresses.
func (c *Client) SetEc2SecGroupRulesIp(ctx context.Context, newIp netip.Addr, rules []Ec2SecGroupRule) (err error) {

	if !newIp.IsValid() {
		return fmt.Errorf("newIp is not valid: %s", newIp.String())
	}

	// make EC2 client if needed
	err = c.makeEc2Client(ctx)
	if err != nil {
		return fmt.Errorf("c.makeEc2Client failed: %w", err)
	}

	// write correct ip field depending on whether newIp is v4 or v6
	var cidrIpV4, cidrIpV6 *string
	if newIp.Is4() {
		cidrIpV4 = new(string)
		*cidrIpV4 = newIp.String() + "/32"
	} else {
		cidrIpV6 = new(string)
		*cidrIpV6 = newIp.String() + "/128"
	}

	// make map of k = groupId, v = []rules
	groupRulesMap := make(map[string][]Ec2SecGroupRule)
	for _, rule := range rules {

		// check to prevent panic below, but shouldn't happen
		if rule.RuleId == nil {
			return fmt.Errorf("rule does not have a ruleId")
		}

		if rule.GroupId == nil {
			return fmt.Errorf("rule: %v does not have a groupId", *rule.RuleId)
		}
		groupRulesMap[*rule.GroupId] = append(groupRulesMap[*rule.GroupId], rule)
	}

	// for each groupId
	for groupId, rules := range groupRulesMap {

		// create rule updates
		ruleUpdates := make([]awsTypes.SecurityGroupRuleUpdate, len(rules))
		for i, rule := range rules {
			ruleUpdates[i] = awsTypes.SecurityGroupRuleUpdate{
				SecurityGroupRuleId: rule.RuleId,
				SecurityGroupRule: &awsTypes.SecurityGroupRuleRequest{
					CidrIpv4:    cidrIpV4,
					CidrIpv6:    cidrIpV6,
					Description: rule.Description,
					IpProtocol:  rule.IpProtocol,
					FromPort:    rule.FromPort,
					ToPort:      rule.ToPort,
				},
			}
		}

		// prepare call log input
		callInput := awsapicall.Input{
			Attempt:    1, // no retry mechanism yet
			DurationMs: 0, // set in defer
			Endpoint:   "ec2Client.ModifySecurityGroupRules",
			Page:       1,
			Result:     "", // set below depending on success or error
		}

		start := time.Now()

		// defer call log to capture duration and result
		defer func() {
			callInput.DurationMs = time.Since(start).Milliseconds()

			_, err := c.callStore.Insert(context.Background(), callInput) // use background context to ensure call log is inserted even if main context is cancelled
			if err != nil {
				c.logger.Error("c.callStore.Insert failed", "error", err, "callInput", callInput)
			}
		}()

		// update rules in EC2
		_, err = c.ec2Client.ModifySecurityGroupRules(ctx, &ec2.ModifySecurityGroupRulesInput{
			GroupId:            &groupId,
			SecurityGroupRules: ruleUpdates,
		})
		if err != nil {
			callInput.Result = err.Error()
			return fmt.Errorf("c.ec2Client.ModifySecurityGroupRules failed for groupId %s: %w", groupId, err)
		}

		callInput.Result = "OK"
	}

	return nil
}
