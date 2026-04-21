package ecbapicall

import (
	"context"
	"log"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/loveyourstack/lys/lysmeta"
	"github.com/loveyourstack/lys/lyspg"
	"github.com/loveyourstack/lys/lystype"
)

const (
	name           string = "ECB API calls"
	schemaName     string = "ecb"
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
	meta lysmeta.Result
)

func init() {
	var err error
	meta, err = lysmeta.AnalyzeStruct(reflect.ValueOf(&Model{}).Elem())
	if err != nil {
		log.Fatalf("lysmeta.AnalyzeStruct failed for %s.%s: %s", schemaName, tableName, err.Error())
	}
}

type Store struct {
	Db *pgxpool.Pool
}

func (s Store) GetMeta() lysmeta.Result {
	return meta
}
func (s Store) GetName() string {
	return name
}

func (s Store) Insert(ctx context.Context, input Input) (newId int64, err error) {
	return lyspg.Insert[Input, int64](ctx, s.Db, schemaName, tableName, pkColName, input)
}

func (s Store) Select(ctx context.Context, params lyspg.SelectParams) (items []Model, unpagedCount lyspg.TotalCount, err error) {
	return lyspg.Select[Model](ctx, s.Db, schemaName, tableName, viewName, defaultOrderBy, meta.DbTags, params)
}

func (s Store) SelectById(ctx context.Context, id int64) (item Model, err error) {
	return lyspg.SelectUnique[Model](ctx, s.Db, schemaName, viewName, pkColName, id)
}

func (s Store) Validate(validate *validator.Validate, input Input) error {
	return lysmeta.Validate(validate, input)
}
