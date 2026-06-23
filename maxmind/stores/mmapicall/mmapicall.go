package mmapicall

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/loveyourstack/lys/lysmeta"
	"github.com/loveyourstack/lys/lyspg"
	"github.com/loveyourstack/lys/lystype"
)

const (
	name           string = "Maxmind API calls"
	schemaName     string = "maxmind"
	tableName      string = "api_call"
	viewName       string = "api_call"
	pkColName      string = "id"
	defaultOrderBy string = "created_at DESC"
)

type Input struct {
	Attempt    int    `db:"attempt" json:"attempt,omitempty" validate:"required,min=1"`
	DurationMs int64  `db:"duration_ms" json:"duration_ms" validate:"required,min=0"`
	Endpoint   string `db:"endpoint" json:"endpoint,omitempty" validate:"required"`
	Method     string `db:"method" json:"method,omitempty" validate:"required,max=64"`
	Page       int    `db:"page" json:"page,omitempty" validate:"required,min=1"`
	Result     string `db:"result" json:"result,omitempty"`
	StatusCode int    `db:"status_code" json:"status_code,omitempty" validate:"required,min=0"`
}

type Model struct {
	Id            int64            `db:"id" json:"id,omitempty"`
	CreatedAt     lystype.Datetime `db:"created_at" json:"created_at,omitzero"`
	CreatedAtDate lystype.Date     `db:"created_at_date" json:"created_at_date,omitzero"`
	Input
}

var (
	plan lysmeta.Plan
)

func init() {
	var err error
	plan, err = lysmeta.Analyze(Model{})
	if err != nil {
		log.Fatalf("lysmeta.Analyze failed for %s.%s: %s", schemaName, tableName, err.Error())
	}
}

type Store struct {
	Db *pgxpool.Pool
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

func (s Store) SelectById(ctx context.Context, id int64) (item Model, err error) {
	return lyspg.SelectUnique[Model](ctx, s.Db, schemaName, viewName, pkColName, id)
}
