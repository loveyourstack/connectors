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

	// high volume: uses store bulk methods

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

	itemStore := ecbexchangerate.Store{Db: db}
	itemType := "Exchange rates"

	// select DB items map in date range with day+toCurrFk as key
	dbItemsMap, err := itemStore.SelectMapByNaturalKey(ctx, baseCurr, freq.String(), startDate, endDate)
	if err != nil {
		return fmt.Errorf("itemStore.SelectMapByNaturalKey failed: %w", err)
	}

	newItems := []ecbexchangerate.Input{}
	updatedIds := []int64{}
	updatedItems := []ecbexchangerate.Input{}
	deletedIds := []int64{}

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
			updatedIds = append(updatedIds, dbItem.Id)
			updatedItems = append(updatedItems, apiItem.Input)
		}
	}

	// for each DB item
	for key, dbItem := range dbItemsMap {

		// try to find the equivalent API item
		_, ok := apiItemsMap[key]
		if !ok {
			deletedIds = append(deletedIds, dbItem.Id)
		}
	}

	// run deletes
	if len(deletedIds) > 0 {
		err := itemStore.BulkDelete(ctx, deletedIds)
		if err != nil {
			return fmt.Errorf("itemStore.BulkDelete failed: %w", err)
		}
		c.InfoLog.Info("deleted", slog.String("type", itemType), slog.Int("num", len(deletedIds)))
	}

	// run inserts
	if len(newItems) > 0 {
		_, err := itemStore.BulkInsert(ctx, newItems)
		if err != nil {
			return fmt.Errorf("itemStore.BulkInsert failed: %w", err)
		}
		c.InfoLog.Info("inserted", slog.String("type", itemType), slog.Int("num", len(newItems)))
	}

	// run updates
	if len(updatedItems) > 0 {
		err := itemStore.BulkUpdate(ctx, updatedItems, updatedIds)
		if err != nil {
			return fmt.Errorf("itemStore.BulkUpdate failed: %w", err)
		}
		c.InfoLog.Info("updated", slog.String("type", itemType), slog.Int("num", len(updatedItems)))
	}

	return nil
}
