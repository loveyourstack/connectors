package tedbsvc

import (
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/loveyourstack/connectors/tedb/tedbapi"
	"github.com/loveyourstack/lys/lysextdata"
)

const (
	// sync keys
	VatRatesSync lysextdata.SyncKey = "TedbVatRates" // caution: this is a single sync key for all vat rates calls, not just the latest
)

type Service struct {
	Client tedbapi.Client
	Db     *pgxpool.Pool
	Logger *slog.Logger

	// optional
	SyncStore lysextdata.ISyncStore
}

func NewService(client tedbapi.Client, db *pgxpool.Pool, logger *slog.Logger) (svc Service) {

	return Service{
		Client: client,
		Db:     db,
		Logger: logger.With("svc", "tedb"),
	}
}

func NewServiceWithSyncStore(client tedbapi.Client, db *pgxpool.Pool, logger *slog.Logger, syncStore lysextdata.ISyncStore) (svc Service) {
	svc = NewService(client, db, logger)
	svc.SyncStore = syncStore
	return svc
}
