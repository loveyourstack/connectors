package mmsvc

import (
	"log"
	"log/slog"
	"os"

	"github.com/loveyourstack/connectors/maxmind/mmapi"
	"github.com/loveyourstack/lys/lysextdata"
)

const (
	// sync keys
	Geo2LiteCitySync lysextdata.SyncKey = "MaxMindGeo2LiteCity"
)

type Service struct {
	Client        mmapi.Client
	DownloadsPath string
	Logger        *slog.Logger

	// optional
	SyncStore lysextdata.ISyncStore
}

func NewService(client mmapi.Client, downloadsPath string, logger *slog.Logger) (svc Service) {

	if downloadsPath == "" {
		log.Fatal("maxmind svc: downloadsPath is required")
	}
	_, err := os.Stat(downloadsPath)
	if os.IsNotExist(err) {
		log.Fatalf("maxmind svc: downloadsPath does not exist: %s", downloadsPath)
	} else if err != nil {
		log.Fatalf("maxmind svc: error checking downloadsPath: %s", err.Error())
	}

	svcShortname := "maxmind"

	return Service{
		Client:        client,
		DownloadsPath: downloadsPath,
		Logger:        logger.With("svc", svcShortname),
	}
}

func NewServiceWithSyncStore(client mmapi.Client, downloadsPath string, logger *slog.Logger, syncStore lysextdata.ISyncStore) (svc Service) {
	svc = NewService(client, downloadsPath, logger)
	svc.SyncStore = syncStore
	return svc
}
