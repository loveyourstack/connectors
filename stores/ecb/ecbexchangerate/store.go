package ecbexchangerate

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/loveyourstack/lys/lysmeta"
	"github.com/loveyourstack/lys/lyspg"
	"github.com/loveyourstack/lys/lystype"
)

const (
	name           string = "Exchange rates"
	schemaName     string = "ecb"
	tableName      string = "exchange_rate"
	viewName       string = "v_exchange_rate"
	pkColName      string = "id"
	defaultOrderBy string = "id"
)

type Input struct {
	Day            lystype.Date     `db:"day" json:"day,omitempty" validate:"required"`
	Frequency      string           `db:"frequency" json:"frequency,omitempty" validate:"required"`
	FromCurrencyFk int64            `db:"from_currency_fk" json:"from_currency_fk,omitempty" validate:"required"`
	LastModifiedAt lystype.Datetime `db:"last_modified_at" json:"last_modified_at,omitempty"` // assigned in Update funcs
	Rate           float32          `db:"rate" json:"rate,omitempty" validate:"required"`
	ToCurrencyFk   int64            `db:"to_currency_fk" json:"to_currency_fk,omitempty" validate:"required"`
}

type Model struct {
	Id           int64            `db:"id" json:"id"`
	FromCurrency string           `db:"from_currency" json:"from_currency"`
	EntryAt      lystype.Datetime `db:"entry_at" json:"entry_at,omitempty"`
	ToCurrency   string           `db:"to_currency" json:"to_currency"`
	Input
}

var (
	meta, inputMeta lysmeta.Result
)

func init() {
	var err error
	meta, err = lysmeta.AnalyzeStructs(reflect.ValueOf(&Input{}).Elem(), reflect.ValueOf(&Model{}).Elem())
	if err != nil {
		log.Fatalf("lysmeta.AnalyzeStructs failed for %s.%s: %s", schemaName, tableName, err.Error())
	}
	inputMeta, _ = lysmeta.AnalyzeStructs(reflect.ValueOf(&Input{}).Elem())
}

type Store struct {
	Db *pgxpool.Pool
}

func (s Store) BulkInsert(ctx context.Context, inputs []Input) (rowsAffected int64, err error) {
	return lyspg.BulkInsert[Input](ctx, s.Db, schemaName, tableName, inputs)
}

func (s Store) Delete(ctx context.Context, id int64) (stmt string, err error) {
	return lyspg.DeleteUnique(ctx, s.Db, schemaName, tableName, pkColName, id)
}

func (s Store) Equal(a, b Model) bool {
	return fmt.Sprintf("%.4f", a.Rate) == fmt.Sprintf("%.4f", b.Rate)
}

func (s Store) GetMeta() lysmeta.Result {
	return meta
}
func (s Store) GetName() string {
	return name
}

func (s Store) Insert(ctx context.Context, input Input) (newItem Model, stmt string, err error) {
	return lyspg.Insert[Input, Model](ctx, s.Db, schemaName, tableName, viewName, pkColName, meta.DbTags, input)
}

func (s Store) Select(ctx context.Context, params lyspg.SelectParams) (items []Model, unpagedCount lyspg.TotalCount, stmt string, err error) {
	return lyspg.Select[Model](ctx, s.Db, schemaName, tableName, viewName, defaultOrderBy, meta.DbTags, params)
}

func (s Store) SelectMapByNaturalKey(ctx context.Context, baseCurr, freq string, startDate, endDate time.Time) (itemsMap map[string]Model, stmt string, err error) {

	items, _, stmt, err := s.Select(ctx, lyspg.SelectParams{
		Conditions: []lyspg.Condition{
			{Field: "from_currency", Operator: lyspg.OpEquals, Value: baseCurr},
			{Field: "frequency", Operator: lyspg.OpEquals, Value: freq},
			{Field: "day", Operator: lyspg.OpGreaterThanEquals, Value: startDate.Format(lystype.DateFormat)},
			{Field: "day", Operator: lyspg.OpLessThanEquals, Value: endDate.Format(lystype.DateFormat)},
		},
	})
	if err != nil {
		return nil, stmt, fmt.Errorf("s.Select failed: %w", err)
	}

	// convert to map with day+toCurrFk as key
	itemsMap = make(map[string]Model)
	for _, dbItem := range items {
		item := Model{
			Id:    dbItem.Id,
			Input: dbItem.Input,
		}
		itemsMap[dbItem.Day.Format(lystype.DateFormat)+"+"+fmt.Sprintf("%v", dbItem.ToCurrencyFk)] = item
	}

	return itemsMap, "", nil
}

func (s Store) SelectById(ctx context.Context, fields []string, id int64) (item Model, stmt string, err error) {
	return lyspg.SelectUnique[Model](ctx, s.Db, schemaName, viewName, pkColName, fields, meta.DbTags, id)
}

func (s Store) Update(ctx context.Context, input Input, id int64) (stmt string, err error) {
	input.LastModifiedAt = lystype.Datetime(time.Now())
	return lyspg.Update[Input](ctx, s.Db, schemaName, tableName, pkColName, input, id)
}

func (s Store) UpdatePartial(ctx context.Context, assignmentsMap map[string]any, id int64) (stmt string, err error) {
	assignmentsMap["last_modified_at"] = lystype.Datetime(time.Now())
	return lyspg.UpdatePartial(ctx, s.Db, schemaName, tableName, pkColName, inputMeta.DbTags, assignmentsMap, id)
}

func (s Store) Validate(validate *validator.Validate, input Input) error {
	return lysmeta.Validate[Input](validate, input)
}
