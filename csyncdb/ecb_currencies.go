package csyncdb

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/loveyourstack/connectors/apiclients/ecbapi"
	"github.com/loveyourstack/connectors/stores/ecb/ecbcurrency"
)

func EcbCurrencies(ctx context.Context, db *pgxpool.Pool, c ecbapi.Client) (stmt string, err error) {

	// select API items map with Code as key
	apiItemsMap, err := c.GetCurrenciesMap()
	if err != nil {
		return "", fmt.Errorf("c.GetCurrenciesMap failed: %w", err)
	}

	// select DB items map with Code as key
	itemStore := ecbcurrency.Store{Db: db}
	dbItemsMap, stmt, err := itemStore.SelectMapByNaturalKey(ctx)
	if err != nil {
		return stmt, fmt.Errorf("itemStore.SelectMapByNaturalKey failed: %w", err)
	}

	// for each API item
	for key, apiItem := range apiItemsMap {

		// try to find the equivalent DB item
		dbItem, ok := dbItemsMap[key]
		if !ok {
			// insert to DB if not found
			_, stmt, err = itemStore.Insert(ctx, apiItem.Input)
			if err != nil {
				return stmt, fmt.Errorf("itemStore.Insert failed on offerId: %v: %w", key, err)
			}
			c.InfoLog.Info("inserted currency", slog.String("code", apiItem.Code))
			continue
		}

		// found: compare values and only update if needed
		if !itemStore.Equal(apiItem, dbItem) {

			stmt, err = itemStore.Update(ctx, apiItem.Input, dbItem.Id)
			if err != nil {
				return stmt, fmt.Errorf("itemStore.Update failed on offerId: %v: %w", key, err)
			}
			c.InfoLog.Info("updated currency", slog.String("code", apiItem.Code))
		}
	}

	// for each DB item
	for key, dbItem := range dbItemsMap {

		// try to find the equivalent API item
		_, ok := apiItemsMap[key]
		if !ok {
			// delete if not found
			stmt, err = itemStore.Delete(ctx, dbItem.Id)
			if err != nil {
				return stmt, fmt.Errorf("itemStore.Delete failed on offerId: %v: %w", key, err)
			}
			c.InfoLog.Info("deleted currency", slog.String("code", dbItem.Code))
		}
	}

	return "", nil
}
