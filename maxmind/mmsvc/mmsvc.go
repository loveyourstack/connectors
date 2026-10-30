package mmsvc

import (
	"fmt"
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

func NewService(client mmapi.Client, downloadsPath string, logger *slog.Logger) (svc Service, err error) {

	if downloadsPath == "" {
		return Service{}, fmt.Errorf("maxmind svc: downloadsPath is required")
	}
	_, err = os.Stat(downloadsPath)
	if os.IsNotExist(err) {
		return Service{}, fmt.Errorf("maxmind svc: downloadsPath does not exist: %s", downloadsPath)
	} else if err != nil {
		return Service{}, fmt.Errorf("maxmind svc: error checking downloadsPath: %s", err.Error())
	}

	svcShortname := "maxmind"

	return Service{
		Client:        client,
		DownloadsPath: downloadsPath,
		Logger:        logger.With("svc", svcShortname),
	}, nil
}

func NewServiceWithSyncStore(client mmapi.Client, downloadsPath string, logger *slog.Logger, syncStore lysextdata.ISyncStore) (svc Service, err error) {
	svc, err = NewService(client, downloadsPath, logger)
	if err != nil {
		return Service{}, err
	}
	svc.SyncStore = syncStore
	return svc, nil
}
