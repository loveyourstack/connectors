package mmgeodbmeta

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/loveyourstack/lys/lysmeta"
	"github.com/loveyourstack/lys/lyspg"
	"github.com/loveyourstack/lys/lystype"
)

const (
	name           string = "Maxmind GeoDB metadata"
	schemaName     string = "maxmind"
	tableName      string = "geo_db_meta"
	viewName       string = "geo_db_meta"
	pkColName      string = "id"
	defaultOrderBy string = "geo_db"
)

type Input struct {
	GeoDB       string           `db:"geo_db" json:"geo_db,omitempty" validate:"required"`
	LastWriteAt lystype.Datetime `db:"last_write_at" json:"last_write_at,omitzero" validate:"required"`
}

type Model struct {
	Id        int64            `db:"id" json:"id,omitempty"`
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

func (s Store) SelectByGeoDb(ctx context.Context, geoDb string) (item Model, err error) {
	return lyspg.SelectUnique[Model](ctx, s.Db, schemaName, viewName, "geo_db", geoDb)
}

func (s Store) SelectById(ctx context.Context, id int64) (item Model, err error) {
	return lyspg.SelectUnique[Model](ctx, s.Db, schemaName, viewName, pkColName, id)
}

func (s Store) Update(ctx context.Context, input Input, id int64) (err error) {
	return lyspg.Update(ctx, s.Db, schemaName, tableName, pkColName, input, id)
}

func (s Store) UpdatePartial(ctx context.Context, assignmentsMap map[string]any, id int64) (err error) {
	return lyspg.UpdatePartial(ctx, s.Db, schemaName, tableName, pkColName, inputPlan.JsonKeyDbNameMap(), assignmentsMap, id)
}

func (s Store) Upsert(ctx context.Context, input Input) (err error) {
	stmt := fmt.Sprintf(`INSERT INTO %s.%s (%s) VALUES ($1, $2) 
	ON CONFLICT (geo_db) DO UPDATE SET last_write_at = EXCLUDED.last_write_at`,
		schemaName, tableName, strings.Join(inputPlan.DbNames(), ", "))

	_, err = s.Db.Exec(ctx, stmt, input.GeoDB, input.LastWriteAt)
	if err != nil {
		return fmt.Errorf("s.Db.Exec failed: %w", err)
	}

	return nil
}

func (s Store) Validate(validate *validator.Validate, input Input) error {
	return lysmeta.Validate(validate, input)
}
