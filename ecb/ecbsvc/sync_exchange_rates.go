package ecbsvc

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/loveyourstack/connectors/ecb/ecbapi"
	"github.com/loveyourstack/connectors/ecb/stores/ecbcurrency"
	"github.com/loveyourstack/connectors/ecb/stores/ecbexchangerate"
)

// SyncExchangeRates syncs the ECB exchange rates from the API to the DB, comparing items in bulk.
func (svc Service) SyncExchangeRates(ctx context.Context, db *pgxpool.Pool, baseCurr string, freq ecbapi.Frequency, startDate, endDate time.Time) error {

	// high volume: uses store bulk methods

	// select map of k = ECB currency code, v = db id
	currStore := ecbcurrency.Store{Db: db}
	currMap, err := currStore.SelectCodeIdMap(ctx)
	if err != nil {
		return fmt.Errorf("currStore.SelectCodeIdMap failed: %w", err)
	}
	if len(currMap) == 0 {
		return fmt.Errorf("currency table is empty: run ECB currencies sync first")
	}

	// select API items map in date range with day+toCurrFk as key
	apiItemsMap, err := svc.Client.GetExchangeRatesMap(ctx, baseCurr, freq, startDate, endDate, currMap)
	if err != nil {
		return fmt.Errorf("svc.Client.GetExchangeRatesMap failed: %w", err)
	}
	if len(apiItemsMap) == 0 {
		return fmt.Errorf("API returned no items, refusing to sync")
	}

	itemStore := ecbexchangerate.Store{Db: db}
	itemType := "ECB exchange rates"

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
		svc.Logger.Info("deleted", slog.String("type", itemType), slog.Int("num", len(deletedIds)))
	}

	// run inserts
	if len(newItems) > 0 {
		_, err := itemStore.BulkInsert(ctx, newItems)
		if err != nil {
			return fmt.Errorf("itemStore.BulkInsert failed: %w", err)
		}
		svc.Logger.Info("inserted", slog.String("type", itemType), slog.Int("num", len(newItems)))
	}

	// run updates
	if len(updatedItems) > 0 {
		err := itemStore.BulkUpdate(ctx, updatedItems, updatedIds)
		if err != nil {
			return fmt.Errorf("itemStore.BulkUpdate failed: %w", err)
		}
		svc.Logger.Info("updated", slog.String("type", itemType), slog.Int("num", len(updatedItems)))
	}

	return nil
}
