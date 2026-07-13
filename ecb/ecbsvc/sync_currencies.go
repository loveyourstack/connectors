package ecbsvc

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/loveyourstack/connectors/ecb/stores/ecbcurrency"
	"github.com/loveyourstack/lys/lystype"
)

func (svc Service) SelectCurrenciesLastSyncAt(ctx context.Context) (lastSyncAt lystype.Datetime, err error) {
	if svc.SyncStore == nil {
		return lystype.Datetime{}, fmt.Errorf("no sync store provided")
	}
	return svc.SyncStore.SelectLastSyncAt(ctx, CurrenciesSync)
}

// SyncCurrencies syncs the ECB currencies from the API to the DB, comparing items one by one.
func (svc Service) SyncCurrencies(ctx context.Context, db *pgxpool.Pool) error {

	// low volume: uses store single-item methods

	// select API items map with Code as key
	apiItemsMap, err := svc.Client.GetCurrenciesMap(ctx)
	if err != nil {
		return fmt.Errorf("svc.Client.GetCurrenciesMap failed: %w", err)
	}
	if len(apiItemsMap) == 0 {
		return fmt.Errorf("API returned no items, refusing to sync")
	}

	itemStore := ecbcurrency.Store{Db: db}
	itemType := "ECB currencies"

	// select DB items map with Code as key
	dbItemsMap, err := itemStore.SelectMapByNaturalKey(ctx)
	if err != nil {
		return fmt.Errorf("itemStore.SelectMapByNaturalKey failed: %w", err)
	}

	// for each API item
	for key, apiItem := range apiItemsMap {

		// try to find the equivalent DB item
		dbItem, ok := dbItemsMap[key]
		if !ok {
			// insert to DB if not found
			_, err = itemStore.Insert(ctx, apiItem.Input)
			if err != nil {
				return fmt.Errorf("itemStore.Insert failed on key: %v: %w", key, err)
			}
			svc.Logger.Info("inserted", slog.String("type", itemType), slog.Any("code", apiItem.Code))
			continue
		}

		// found: compare values and only update if needed
		if !itemStore.Equal(apiItem, dbItem) {

			err = itemStore.Update(ctx, apiItem.Input, dbItem.Id)
			if err != nil {
				return fmt.Errorf("itemStore.Update failed on key: %v: %w", key, err)
			}
			svc.Logger.Info("updated", slog.String("type", itemType), slog.Any("code", apiItem.Code))
		}
	}

	// for each DB item
	for key, dbItem := range dbItemsMap {

		// try to find the equivalent API item
		_, ok := apiItemsMap[key]
		if !ok {
			// delete if not found
			err = itemStore.Delete(ctx, dbItem.Id)
			if err != nil {
				return fmt.Errorf("itemStore.Delete failed on key: %v: %w", key, err)
			}
			svc.Logger.Info("deleted", slog.String("type", itemType), slog.Any("code", dbItem.Code))
		}
	}

	// if a sync store is provided, upsert the last sync time
	if svc.SyncStore != nil {
		err = svc.SyncStore.Upsert(ctx, CurrenciesSync)
		if err != nil {
			return fmt.Errorf("svc.SyncStore.Upsert failed: %w", err)
		}
	}

	return nil
}
