package mmlocation

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/loveyourstack/lys/lyserr"
	"github.com/loveyourstack/lys/lysmeta"
	"github.com/loveyourstack/lys/lyspg"
)

const (
	name           string = "MaxMind English locations"
	schemaName     string = "maxmind"
	tableName      string = "geoip2_location_en"
	viewName       string = "geoip2_location_en"
	pkColName      string = "geoname_id"
	defaultOrderBy string = "geoname_id"
)

type Input struct {
	CityName       string `db:"city_name" json:"city_name,omitempty"`
	CountryIsoCode string `db:"country_iso_code" json:"country_iso_code,omitempty"`
	CountryName    string `db:"country_name" json:"country_name,omitempty"`
	GeonameId      int    `db:"geoname_id" json:"geoname_id,omitempty"`
}

type Model struct {
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

func (s Store) Analyze(ctx context.Context) error {

	stmt := fmt.Sprintf(`ANALYZE %s.%s;`, schemaName, tableName)
	_, err := s.Db.Exec(ctx, stmt)
	if err != nil {
		return lyserr.Db{Err: fmt.Errorf("s.Db.Exec failed: %w", err), Stmt: stmt}
	}

	return nil
}

func (s Store) BulkInsertSourceTx(ctx context.Context, tx pgx.Tx, colNames []string, source pgx.CopyFromSource) (rowsAffected int64, err error) {

	rowsAffected, err = tx.CopyFrom(ctx, pgx.Identifier{schemaName, tableName}, colNames, source)
	if err != nil {
		return 0, fmt.Errorf("tx.CopyFrom failed: %w", err)
	}
	return rowsAffected, nil
}

func (s Store) GetName() string {
	return name
}
func (s Store) GetPlan() lysmeta.Plan {
	return plan
}

func (s Store) Select(ctx context.Context, params lyspg.SelectParams) (items []Model, unpagedCount lyspg.TotalCount, err error) {
	return lyspg.Select[Model](ctx, s.Db, schemaName, tableName, viewName, defaultOrderBy, plan.DbNames(), params)
}

func (s Store) SelectByGeonameId(ctx context.Context, geonameId int) (item Model, err error) {
	return lyspg.SelectUnique[Model](ctx, s.Db, schemaName, viewName, "geoname_id", geonameId)
}

func (s Store) TruncateTx(ctx context.Context, tx pgx.Tx) error {

	stmt := fmt.Sprintf(`TRUNCATE TABLE %s.%s;`, schemaName, tableName)
	_, err := tx.Exec(ctx, stmt)
	if err != nil {
		return lyserr.Db{Err: fmt.Errorf("tx.Exec failed: %w", err), Stmt: stmt}
	}

	return nil
}
