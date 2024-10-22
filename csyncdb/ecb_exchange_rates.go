package csyncdb

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/loveyourstack/connectors/apiclients/ecbapi"
	"github.com/loveyourstack/connectors/stores/ecb/ecbcurrency"
	"github.com/loveyourstack/connectors/stores/ecb/ecbexchangerate"
)

func EcbExchangeRates(ctx context.Context, db *pgxpool.Pool, c ecbapi.Client, baseCurr string, freq ecbapi.Frequency, startDate, endDate time.Time) error {

	// select map of k = ECB currency code, v = db id
	currStore := ecbcurrency.Store{Db: db}
	currMap, err := currStore.SelectCodeIdMap(ctx)
	if err != nil {
		return fmt.Errorf("currStore.SelectCodeIdMap failed: %w", err)
	}
	if len(currMap) == 0 {
		return fmt.Errorf("no currencies found: pls sync currencies first")
	}

	// select API items map in date range with day+toCurrFk as key
	apiItemsMap, err := c.GetExchangeRatesMap(baseCurr, freq, startDate, endDate, currMap)
	if err != nil {
		return fmt.Errorf("c.GetExchangeRatesMap failed: %w", err)
	}

	// select DB items map in date range with day+toCurrFk as key
	itemStore := ecbexchangerate.Store{Db: db}
	dbItemsMap, err := itemStore.SelectMapByNaturalKey(ctx, baseCurr, freq.String(), startDate, endDate)
	if err != nil {
		return fmt.Errorf("itemStore.SelectMapByNaturalKey failed: %w", err)
	}

	newItems := []ecbexchangerate.Input{}
	updatedItems := make(map[int64]ecbexchangerate.Input) // map key is the DB ID
	deletedItems := []ecbexchangerate.Model{}

	// for each API item
	for key, apiItem := range apiItemsMap {

		// try to find the equivalent DB item
		dbItem, ok := dbItemsMap[key]
		if !ok {
			newItems = append(newItems, apiItem.Input)
			continue
		}

		// found: compare values and only update if needed
		if !itemStore.Equal(apiItem, dbItem) {
			updatedItems[dbItem.Id] = apiItem.Input
		}
	}

	// for each DB item
	for key, dbItem := range dbItemsMap {

		// try to find the equivalent API item
		_, ok := apiItemsMap[key]
		if !ok {
			deletedItems = append(deletedItems, dbItem)
		}
	}

	// run deletes
	if len(deletedItems) > 0 {
		for _, dbItem := range deletedItems {
			err = itemStore.Delete(ctx, dbItem.Id)
			if err != nil {
				return fmt.Errorf("itemStore.Delete failed on ID: %v: %w", dbItem.Id, err)
			}
		}
		c.InfoLog.Info("deleted exchange rates", slog.Int("num", len(deletedItems)))
	}

	// run inserts (bulk)
	if len(newItems) > 0 {
		_, err := itemStore.BulkInsert(ctx, newItems)
		if err != nil {
			return fmt.Errorf("itemStore.BulkInsert failed: %w", err)
		}
		c.InfoLog.Info("inserted exchange rates", slog.Int("num", len(newItems)))
	}

	// run updates
	if len(updatedItems) > 0 {
		for dbId, apiInput := range updatedItems {
			err = itemStore.Update(ctx, apiInput, dbId)
			if err != nil {
				return fmt.Errorf("itemStore.Update failed on ID: %v: %w", dbId, err)
			}
		}
		c.InfoLog.Info("updated exchange rates", slog.Int("num", len(updatedItems)))
	}

	return nil
}
