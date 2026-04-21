package ecbapi

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/loveyourstack/connectors/stores/ecb/ecbapicall"
)

// Docs: https://data.ecb.europa.eu/help/api/data

const (
	baseUrl     string = "https://data-api.ecb.europa.eu"
	timeoutSecs int    = 20
)

type Client struct {
	HttpClient *http.Client
	InfoLog    *slog.Logger
	ErrorLog   *slog.Logger
	CallStore  ecbapicall.Store
}

func NewClient(db *pgxpool.Pool, infoLog, errorLog *slog.Logger) (client Client) {

	apiShortname := "ecb"

	return Client{
		HttpClient: &http.Client{
			Timeout: time.Duration(timeoutSecs) * time.Second,
		},
		InfoLog:  infoLog.With("api", apiShortname),
		ErrorLog: errorLog.With("api", apiShortname),
		CallStore: ecbapicall.Store{
			Db: db,
		},
	}
}
