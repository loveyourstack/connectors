package sysextdatasync

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/loveyourstack/lys/lysextdata"
	"github.com/loveyourstack/lys/lysmeta"
	"github.com/loveyourstack/lys/lyspg"
	"github.com/loveyourstack/lys/lystype"
)

const (
	name           string = "System ext data sync"
	schemaName     string = "system"
	tableName      string = "ext_data_sync"
	viewName       string = "ext_data_sync"
	pkColName      string = "id"
	defaultOrderBy string = "sync_key"
)

type Input struct {
	LastSyncAt lystype.Datetime   `db:"last_sync_at" json:"last_sync_at,omitzero" validate:"required"`
	SyncKey    lysextdata.SyncKey `db:"sync_key" json:"sync_key,omitempty" validate:"required,max=256"`
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

// GetLastSyncAt is a func which allows other stores to have a single-line wrapper for their own GetLastSyncAt funcs
func GetLastSyncAt(ctx context.Context, db *pgxpool.Pool, syncKey lysextdata.SyncKey) (lastSyncAt lystype.Datetime, err error) {

	store := Store{Db: db}
	item, err := store.SelectBySyncKey(ctx, syncKey)
	if err != nil {
		// if no entry yet, just return empty timestamp
		if errors.Is(err, pgx.ErrNoRows) {
			return lystype.Datetime{}, nil
		}
		return lystype.Datetime{}, fmt.Errorf("store.SelectBySyncKey failed: %w", err)
	}

	return item.LastSyncAt, nil
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

func (s Store) SelectById(ctx context.Context, id int64) (item Model, err error) {
	return lyspg.SelectUnique[Model](ctx, s.Db, schemaName, viewName, pkColName, id)
}

func (s Store) SelectBySyncKey(ctx context.Context, syncKey lysextdata.SyncKey) (item Model, err error) {
	return lyspg.SelectUnique[Model](ctx, s.Db, schemaName, viewName, "sync_key", syncKey)
}

func (s Store) SelectLastSyncAt(ctx context.Context, syncKey lysextdata.SyncKey) (lastSyncAt lystype.Datetime, err error) {
	return GetLastSyncAt(ctx, s.Db, syncKey)
}

func (s Store) Upsert(ctx context.Context, syncKey lysextdata.SyncKey) (err error) {

	stmt := fmt.Sprintf(`INSERT INTO %s.%s (%s) VALUES ($1, $2) 
	ON CONFLICT (sync_key) DO UPDATE SET last_sync_at = EXCLUDED.last_sync_at`,
		schemaName, tableName, strings.Join(inputPlan.DbNames(), ", "))

	_, err = s.Db.Exec(ctx, stmt, time.Now().Format(lystype.DatetimeFormat), syncKey)
	if err != nil {
		return fmt.Errorf("s.Db.Exec failed: %w", err)
	}

	return nil
}

func (s Store) Validate(validate *validator.Validate, input Input) error {
	return lysmeta.Validate(validate, input)
}
