package awsusersgrule

import (
	"context"
	"fmt"
	"log"

	"github.com/go-playground/validator/v10"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/loveyourstack/lys/lysmeta"
	"github.com/loveyourstack/lys/lyspg"
	"github.com/loveyourstack/lys/lystype"
)

const (
	name           string = "AWS User Security Group Rules"
	schemaName     string = "aws"
	tableName      string = "user_security_group_rule"
	viewName       string = "user_security_group_rule"
	pkColName      string = "id"
	defaultOrderBy string = "id"
)

type Input struct {
	SecurityGroupRuleId string `db:"security_group_rule_id" json:"security_group_rule_id,omitempty" validate:"required"`
	UserShortname       string `db:"user_shortname" json:"user_shortname,omitempty" validate:"required"`
}

type Model struct {
	Id        int64            `db:"id" json:"id"`
	CreatedAt lystype.Datetime `db:"created_at" json:"created_at,omitzero"`
	Input
}

var (
	plan, inputPlan lysmeta.Plan
)

func init() {
	var err error
	plan, err = lysmeta.Analyze(Model{})
	if err != nil {
		log.Fatalf("lysmeta.Analyze failed for %s.%s: %s", schemaName, tableName, err.Error())
	}
	inputPlan, _ = lysmeta.Analyze(Input{})
}

type Store struct {
	Db *pgxpool.Pool
}

func (s Store) Delete(ctx context.Context, id int64) error {
	return lyspg.DeleteUnique(ctx, s.Db, schemaName, tableName, pkColName, id)
}

func (s Store) GetName() string {
	return name
}
func (s Store) GetPlan() lysmeta.Plan {
	return plan
}

func (s Store) Insert(ctx context.Context, input Input) (newId int64, err error) {
	return lyspg.Insert[Input, int64](ctx, s.Db, schemaName, tableName, pkColName, input)
}

func (s Store) Select(ctx context.Context, params lyspg.SelectParams) (items []Model, unpagedCount lyspg.TotalCount, err error) {
	return lyspg.Select[Model](ctx, s.Db, schemaName, tableName, viewName, defaultOrderBy, plan.DbNames(), params)
}

func (s Store) SelectRuleIdsByUser(ctx context.Context, userShortname string) (ruleIds []string, err error) {
	stmt := fmt.Sprintf("SELECT security_group_rule_id FROM %s.%s WHERE user_shortname = $1;", schemaName, tableName)
	return lyspg.SelectSlice[string](ctx, s.Db, stmt, userShortname)
}

func (s Store) SelectById(ctx context.Context, id int64) (item Model, err error) {
	return lyspg.SelectUnique[Model](ctx, s.Db, schemaName, viewName, pkColName, id)
}

func (s Store) Update(ctx context.Context, input Input, id int64) error {
	return lyspg.Update(ctx, s.Db, schemaName, tableName, pkColName, input, id)
}

func (s Store) UpdatePartial(ctx context.Context, assignmentsMap map[string]any, id int64) error {
	return lyspg.UpdatePartial(ctx, s.Db, schemaName, tableName, pkColName, inputPlan.JsonKeyDbNameMap(), assignmentsMap, id)
}

func (s Store) UserHasRules(ctx context.Context, userShortname string) (bool, error) {
	return lyspg.Exists(ctx, s.Db, schemaName, tableName, "user_shortname", userShortname)
}

func (s Store) Validate(validate *validator.Validate, input Input) error {
	return lysmeta.Validate(validate, input)
}
