package ecbexchangerate

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/jackc/pgx/v5"
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
	Day            lystype.Date `db:"day" json:"day,omitzero" validate:"required"`
	Frequency      string       `db:"frequency" json:"frequency,omitempty" validate:"required,len=1"`
	FromCurrencyFk int64        `db:"from_currency_fk" json:"from_currency_fk,omitempty" validate:"required"`
	Rate           float32      `db:"rate" json:"rate,omitempty" validate:"required"`
	ToCurrencyFk   int64        `db:"to_currency_fk" json:"to_currency_fk,omitempty" validate:"required"`
}

type Model struct {
	Id           int64            `db:"id" json:"id"`
	CreatedAt    lystype.Datetime `db:"created_at" json:"created_at,omitzero"`
	FromCurrency string           `db:"from_currency" json:"from_currency"`
	ToCurrency   string           `db:"to_currency" json:"to_currency"`
	UpdatedAt    lystype.Datetime `db:"updated_at" json:"updated_at,omitzero"` // assigned by trigger (assumes use of lyspgmon.CheckDDL)
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
	return lyspg.BulkInsert(ctx, s.Db, schemaName, tableName, inputs)
}

func (s Store) Delete(ctx context.Context, id int64) error {
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

func (s Store) Insert(ctx context.Context, input Input) (newId int64, err error) {
	return lyspg.Insert[Input, int64](ctx, s.Db, schemaName, tableName, pkColName, input)
}

func (s Store) Select(ctx context.Context, params lyspg.SelectParams) (items []Model, unpagedCount lyspg.TotalCount, err error) {
	return lyspg.Select[Model](ctx, s.Db, schemaName, tableName, viewName, defaultOrderBy, meta.DbTags, params)
}

func (s Store) SelectMapByNaturalKey(ctx context.Context, baseCurr, freq string, startDate, endDate time.Time) (itemsMap map[string]Model, err error) {

	items, _, err := s.Select(ctx, lyspg.SelectParams{
		Conditions: []lyspg.Condition{
			{Field: "from_currency", Operator: lyspg.OpEquals, Value: baseCurr},
			{Field: "frequency", Operator: lyspg.OpEquals, Value: freq},
			{Field: "day", Operator: lyspg.OpGreaterThanEquals, Value: startDate.Format(lystype.DateFormat)},
			{Field: "day", Operator: lyspg.OpLessThanEquals, Value: endDate.Format(lystype.DateFormat)},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("s.Select failed: %w", err)
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

	return itemsMap, nil
}

func (s Store) SelectById(ctx context.Context, id int64) (item Model, err error) {
	return lyspg.SelectUnique[Model](ctx, s.Db, schemaName, viewName, pkColName, id)
}

// SelectLatestDaily returns the latest available daily record for the requested day.
// if the requested day has no rate, the latest rate before that day is used, up to a maximum of 5 days prior
func (s Store) SelectLatestDaily(ctx context.Context, fromCurr, toCurr string, day time.Time) (item Model, err error) {

	stmt := fmt.Sprintf("SELECT * from %s.%s WHERE frequency = 'D' AND from_currency = $1 AND to_currency = $2 AND day <= $3 ORDER BY day DESC LIMIT 1", schemaName, viewName)

	items, err := lyspg.SelectT[Model](ctx, s.Db, stmt, fromCurr, toCurr, day.Format(lystype.DateFormat))
	if err != nil {
		return Model{}, fmt.Errorf("lyspg.SelectT failed: %w", err)
	}
	if len(items) == 0 {
		return Model{}, pgx.ErrNoRows
	}
	if len(items) > 1 {
		return Model{}, pgx.ErrTooManyRows
	}

	diff := day.Sub(time.Time(items[0].Day))
	diffDays := int(diff.Hours() / 24)
	maxDiffDays := 5
	if diffDays > maxDiffDays {
		return Model{}, fmt.Errorf("returned rate is for %v. This is %v days before the requested day, which exceeds the max of %v", items[0].Day.Format(lystype.DateFormat), diffDays, maxDiffDays)
	}

	return items[0], nil
}

// SelectLatestDailyRangeMap returns a map of k = day in YYYY-MM-DD, v = rate for all days between start and end, inclusive
// if a day has no rate, the latest rate before that day is used, up to a maximum of 5 days prior
func (s Store) SelectLatestDailyRangeMap(ctx context.Context, fromCurr, toCurr string, startDay, endDay time.Time) (rangeMap map[string]float32, err error) {

	maxDiffDays := 5

	stmt := fmt.Sprintf("SELECT * from %s.%s WHERE frequency = 'D' AND from_currency = $1 AND to_currency = $2 AND day >= $3 AND day <= $4 ORDER BY day DESC", schemaName, viewName)

	items, err := lyspg.SelectT[Model](ctx, s.Db, stmt, fromCurr, toCurr, startDay.Add(-time.Duration(maxDiffDays)*24*time.Hour).Format(lystype.DateFormat), endDay.Format(lystype.DateFormat))
	if err != nil {
		return nil, fmt.Errorf("lyspg.SelectT failed: %w", err)
	}
	if len(items) == 0 {
		return nil, pgx.ErrNoRows
	}

	// the latest record may not be more than 5 days before endDay param
	diff := endDay.Sub(time.Time(items[0].Day))
	diffDays := int(diff.Hours() / 24)
	if diffDays > maxDiffDays {
		return nil, fmt.Errorf("latest rate is for %v. This is %v days before the endDay, which exceeds the max of %v", items[0].Day.Format(lystype.DateFormat), diffDays, maxDiffDays)
	}

	// the earliest record must be before startDay param
	if startDay.Before(time.Time(items[len(items)-1].Day)) {
		return nil, fmt.Errorf("earliest returned rate is for %v. This is after startDay", items[len(items)-1].Day.Format(lystype.DateFormat))
	}

	// the earliest record may not be more than 5 days before startDay param
	diff = startDay.Sub(time.Time(items[len(items)-1].Day))
	diffDays = int(diff.Hours() / 24)
	if diffDays > maxDiffDays {
		return nil, fmt.Errorf("earliest returned rate is for %v. This is %v days before startDay, which exceeds the max of %v", items[len(items)-1].Day.Format(lystype.DateFormat), diffDays, maxDiffDays)
	}

	rangeMap = make(map[string]float32)
	activeLatest := 0

	// for each day in requested range from end to start
	for d := endDay; d.Before(startDay) == false; d = d.AddDate(0, 0, -1) {

		// assign the rate for that day if found
		var rate float32
		found := false
		for _, item := range items[activeLatest:] {
			if d.Format(lystype.DateFormat) == item.Day.Format(lystype.DateFormat) {
				found = true
				rate = item.Rate
				activeLatest++
				break
			}
		}
		// if not found, assign the latest day available before the current day
		if !found {
			rate = items[activeLatest].Rate
		}

		rangeMap[d.Format(lystype.DateFormat)] = rate
	}

	return rangeMap, nil
}

func (s Store) Update(ctx context.Context, input Input, id int64) error {
	return lyspg.Update(ctx, s.Db, schemaName, tableName, pkColName, input, id)
}

func (s Store) UpdatePartial(ctx context.Context, assignmentsMap map[string]any, id int64) error {
	return lyspg.UpdatePartial(ctx, s.Db, schemaName, tableName, pkColName, inputMeta.DbTags, assignmentsMap, id)
}

func (s Store) Validate(validate *validator.Validate, input Input) error {
	return lysmeta.Validate(validate, input)
}
