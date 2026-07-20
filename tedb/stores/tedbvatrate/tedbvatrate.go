package tedbvatrate

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/loveyourstack/lys/lysmeta"
	"github.com/loveyourstack/lys/lyspg"
	"github.com/loveyourstack/lys/lysslice"
	"github.com/loveyourstack/lys/lystype"
)

const (
	name           string = "TEDB VAT rates"
	schemaName     string = "tedb"
	tableName      string = "vat_rate"
	viewName       string = "vat_rate"
	pkColName      string = "id"
	defaultOrderBy string = "situation_on"
)

type Input struct {
	CategoryDescription string       `db:"category_description" json:"category_description,omitempty"`
	CategoryIdentifier  string       `db:"category_identifier" json:"category_identifier,omitempty" validate:"max=64"`
	CnCodes             []string     `db:"cn_codes" json:"cn_codes,omitempty"`
	Comment             string       `db:"comment" json:"comment,omitempty"`
	CpaCodes            []string     `db:"cpa_codes" json:"cpa_codes,omitempty"`
	MemberState         string       `db:"member_state" json:"member_state,omitempty" validate:"required,len=2"`
	RateType            string       `db:"rate_type" json:"rate_type,omitempty" validate:"required,max=64"`
	Rate                float64      `db:"rate" json:"rate,omitempty" validate:"required,gte=0"`
	SituationOn         lystype.Date `db:"situation_on" json:"situation_on,omitzero" validate:"required"`
	Type                string       `db:"type" json:"type,omitempty" validate:"required,max=64"`
}

type Model struct {
	Id        int64            `db:"id" json:"id"`
	CreatedAt lystype.Datetime `db:"created_at" json:"created_at,omitzero"`
	UpdatedAt lystype.Datetime `db:"updated_at" json:"updated_at,omitzero"` // assigned by trigger (assumes use of lyspgmon.CheckDb)
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

func (s Store) BulkDelete(ctx context.Context, ids []int64) error {
	return lyspg.BulkDelete(ctx, s.Db, schemaName, tableName, pkColName, ids)
}

func (s Store) BulkInsert(ctx context.Context, inputs []Input) (rowsAffected int64, err error) {
	return lyspg.BulkInsert(ctx, s.Db, schemaName, tableName, inputs)
}

func (s Store) BulkUpdate(ctx context.Context, inputs []Input, ids []int64) error {
	return lyspg.BulkUpdate(ctx, s.Db, schemaName, tableName, pkColName, inputs, ids)
}

func (s Store) Delete(ctx context.Context, id int64) error {
	return lyspg.DeleteUnique(ctx, s.Db, schemaName, tableName, pkColName, id)
}

func (s Store) Equal(a, b Model) bool {
	return a.CategoryDescription == b.CategoryDescription &&
		a.CategoryIdentifier == b.CategoryIdentifier &&
		lysslice.EqualUnordered(a.CnCodes, b.CnCodes) &&
		a.Comment == b.Comment &&
		lysslice.EqualUnordered(a.CpaCodes, b.CpaCodes) &&
		a.MemberState == b.MemberState &&
		a.RateType == b.RateType &&
		a.Rate == b.Rate &&
		a.SituationOn.Format(lystype.DateFormat) == b.SituationOn.Format(lystype.DateFormat) &&
		a.Type == b.Type
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

/*
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
*/

func (s Store) SelectById(ctx context.Context, id int64) (item Model, err error) {
	return lyspg.SelectUnique[Model](ctx, s.Db, schemaName, viewName, pkColName, id)
}

func (s Store) Update(ctx context.Context, input Input, id int64) error {
	return lyspg.Update(ctx, s.Db, schemaName, tableName, pkColName, input, id)
}
