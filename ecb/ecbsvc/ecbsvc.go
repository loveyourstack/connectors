package ecbsvc

import (
	"log/slog"

	"github.com/loveyourstack/connectors/ecb/ecbapi"
	"github.com/loveyourstack/lys/lysextdata"
)

const (
	// sync keys
	CurrenciesSync    lysextdata.SyncKey = "EcbCurrencies"
	ExchangeRatesSync lysextdata.SyncKey = "EcbExchangeRates" // caution: this is a single sync key for all XR calls, not just the latest
)

type Service struct {
	Client ecbapi.Client
	Logger *slog.Logger

	// optional
	SyncStore lysextdata.ISyncStore
}

func NewService(client ecbapi.Client, logger *slog.Logger) (svc Service) {

	svcShortname := "ecb"

	return Service{
		Client: client,
		Logger: logger.With("svc", svcShortname),
	}
}

func NewServiceWithSyncStore(client ecbapi.Client, logger *slog.Logger, syncStore lysextdata.ISyncStore) (svc Service) {
	svc = NewService(client, logger)
	svc.SyncStore = syncStore
	return svc
}
