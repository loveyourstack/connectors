package ecbsvc

import (
	"log/slog"

	"github.com/loveyourstack/connectors/ecb/ecbapi"
)

type Service struct {
	Client ecbapi.Client

	InfoLog  *slog.Logger
	ErrorLog *slog.Logger
}

func NewService(client ecbapi.Client, infoLog, errorLog *slog.Logger) (svc Service) {

	svcShortname := "ecb"

	return Service{
		Client: client,

		InfoLog:  infoLog.With("svc", svcShortname),
		ErrorLog: errorLog.With("svc", svcShortname),
	}
}
