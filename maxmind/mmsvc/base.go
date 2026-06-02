package mmsvc

import (
	"log"
	"log/slog"
	"os"

	"github.com/loveyourstack/connectors/maxmind/mmapi"
)

type Service struct {
	Client        mmapi.Client
	DownloadsPath string

	InfoLog  *slog.Logger
	ErrorLog *slog.Logger
}

func NewService(client mmapi.Client, downloadsPath string, infoLog, errorLog *slog.Logger) (svc Service) {

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

		InfoLog:  infoLog.With("svc", svcShortname),
		ErrorLog: errorLog.With("svc", svcShortname),
	}
}
