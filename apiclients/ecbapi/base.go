package ecbapi

import (
	"log/slog"
	"net/http"
	"time"
)

// Docs: https://data.ecb.europa.eu/help/api/data

const (
	baseUrl     string = "https://data-api.ecb.europa.e"
	timeoutSecs int    = 20
)

type Client struct {
	HttpClient *http.Client
	InfoLog    *slog.Logger
	ErrorLog   *slog.Logger
}

func NewClient(infoLog, errorLog *slog.Logger) (client Client) {

	apiShortname := "ecb"

	return Client{
		HttpClient: &http.Client{
			Timeout: time.Duration(timeoutSecs) * time.Second,
		},
		InfoLog:  infoLog.With("api", apiShortname),
		ErrorLog: errorLog.With("api", apiShortname),
	}
}
