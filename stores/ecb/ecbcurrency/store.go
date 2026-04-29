package ecbcurrency

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
	name           string = "ECB currencies"
	schemaName     string = "ecb"
	tableName      string = "currency"
	viewName       string = "currency"
	pkColName      string = "id"
	defaultOrderBy string = "name"
)

type Input struct {
	Code string `db:"code" json:"code,omitempty" validate:"required,min=2,max=3"`
	Name string `db:"name" json:"name,omitempty" validate:"required,max=500"`
}

type Model struct {
	Id        int64            `db:"id" json:"id"`
	CreatedAt lystype.Datetime `db:"created_at" json:"created_at,omitzero"`
	UpdatedAt lystype.Datetime `db:"updated_at" json:"updated_at,omitzero"` // assigned by trigger (assumes use of lyspgmon.CheckDb)
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

func (s Store) Equal(a, b Model) bool {
	return a.Name == b.Name
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

func (s Store) SelectMapByNaturalKey(ctx context.Context) (itemsMap map[string]Model, err error) {

	items, _, err := s.Select(ctx, lyspg.SelectParams{})
	if err != nil {
		return nil, fmt.Errorf("s.Select failed: %w", err)
	}

	// convert to map with Code as key
	itemsMap = make(map[string]Model)
	for _, dbItem := range items {
		item := Model{
			Id:    dbItem.Id,
			Input: dbItem.Input,
		}
		itemsMap[dbItem.Code] = item
	}

	return itemsMap, nil
}

func (s Store) SelectCodeIdMap(ctx context.Context) (codeIdMap map[string]int64, err error) {

	items, _, err := s.Select(ctx, lyspg.SelectParams{})
	if err != nil {
		return nil, fmt.Errorf("s.Select failed: %w", err)
	}

	codeIdMap = make(map[string]int64)
	for _, dbItem := range items {
		codeIdMap[dbItem.Code] = dbItem.Id
	}

	return codeIdMap, nil
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

func (s Store) Validate(validate *validator.Validate, input Input) error {
	return lysmeta.Validate(validate, input)
}
