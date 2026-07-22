package tedbsvc

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/loveyourstack/connectors/tedb/stores/tedbvatrate"
	"github.com/loveyourstack/connectors/tedb/tedbapi"
	"github.com/loveyourstack/lys/lystype"
)

func (svc Service) SelectVatRatesLastSyncAt(ctx context.Context) (lastSyncAt lystype.Datetime, err error) {
	if svc.SyncStore == nil {
		return lystype.Datetime{}, fmt.Errorf("no sync store provided")
	}
	return svc.SyncStore.SelectLastSyncAt(ctx, VatRatesSync)
}

var euCountryIsos = []string{
	"AT", // Austria
	"BE", // Belgium
	"BG", // Bulgaria
	"CY", // Cyprus
	"CZ", // Czechia
	"DE", // Germany
	"DK", // Denmark
	"EE", // Estonia
	"ES", // Spain
	"FI", // Finland
	"FR", // France
	// "GR", // Greece - excluded: API call returns 00002: The Member State "GR" does not exist.
	"HR", // Croatia
	"HU", // Hungary
	"IE", // Ireland
	"IT", // Italy
	"LT", // Lithuania
	"LU", // Luxembourg
	"LV", // Latvia
	"MT", // Malta
	"NL", // Netherlands
	"PL", // Poland
	"PT", // Portugal
	"RO", // Romania
	"SE", // Sweden
	"SI", // Slovenia
	"SK", // Slovakia
}

// SyncVatRates syncs the TEDB VAT rates from the API to the DB, comparing items in bulk.
func (svc Service) SyncVatRates(ctx context.Context, db *pgxpool.Pool, startDate, endDate time.Time) error {

	// high volume: uses store bulk methods

	// the data is messy and it was hard to find a good natural key which works for almost all items. In the end I used:
	// situation_on + member_state + type + category_fk + cn_codes + cpa_codes + comment

	// select API items in date range
	apiItemsMap, err := svc.Client.GetVatRatesMap(ctx, euCountryIsos, startDate, endDate)
	if err != nil {
		return fmt.Errorf("svc.Client.GetVatRatesMap failed: %w", err)
	}
	if len(apiItemsMap) == 0 {
		// API returning no rates is normal for short date intervals
		return tedbapi.ErrNoRatesFound
	}

	itemStore := tedbvatrate.Store{Db: db}
	itemType := "TEDB VAT rates"

	// select DB items map in date range
	dbItemsMap, err := itemStore.SelectMapByNaturalKey(ctx, startDate, endDate)
	if err != nil {
		return fmt.Errorf("itemStore.SelectMapByNaturalKey failed: %w", err)
	}

	newItems := []tedbvatrate.Input{}
	updatedIds := []int64{}
	updatedItems := []tedbvatrate.Input{}
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

	// if a sync store is provided, upsert the last sync time
	if svc.SyncStore != nil {
		err = svc.SyncStore.Upsert(ctx, VatRatesSync)
		if err != nil {
			return fmt.Errorf("svc.SyncStore.Upsert failed: %w", err)
		}
	}

	return nil
}
