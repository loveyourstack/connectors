package ecbsvc

import (
	"log/slog"

	"github.com/loveyourstack/connectors/ecb/ecbapi"
)

type Service struct {
	Client ecbapi.Client

	Logger *slog.Logger
}

func NewService(client ecbapi.Client, logger *slog.Logger) (svc Service) {

	svcShortname := "ecb"

	return Service{
		Client: client,

		Logger: logger.With("svc", svcShortname),
	}
}
