package mmapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/loveyourstack/connectors/maxmind/stores/mmapicall"
	"github.com/loveyourstack/lys/lystime"
)

// Docs: https://dev.maxmind.com/geoip/updating-databases/

const (
	baseUrl     string = "https://download.maxmind.com/geoip/databases"
	timeoutSecs int    = 20
)

type Conf struct {
	AccountId  string // AccountId is the MaxMind account ID from account information.
	LicenseKey string // LicenseKey is the MaxMind license key which you need to generate from your account.
}

type Client struct {
	conf       Conf
	callStore  mmapicall.Store
	errorLog   *slog.Logger
	httpClient *http.Client
	infoLog    *slog.Logger
}

func NewClient(conf Conf, db *pgxpool.Pool, infoLog *slog.Logger, errorLog *slog.Logger) (client Client) {

	if conf.AccountId == "" {
		log.Fatalf("maxmind client: conf.AccountId is required")
	}
	if conf.LicenseKey == "" {
		log.Fatalf("maxmind client: conf.LicenseKey is required")
	}

	apiShortname := "maxmind"

	return Client{
		conf:      conf,
		callStore: mmapicall.Store{Db: db},
		errorLog:  errorLog.With("api", apiShortname),
		httpClient: &http.Client{
			Timeout: time.Duration(timeoutSecs) * time.Second,
		},
		infoLog: infoLog.With("api", apiShortname),
	}
}

func (c Client) doRequest(ctx context.Context, method, url string, body io.Reader) (respBody []byte, attempt, statusCode int, err error) {

	// define request
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("http.NewRequestWithContext failed: %w", err)
	}

	// add basic auth
	req.SetBasicAuth(c.conf.AccountId, c.conf.LicenseKey)

	// start attempts loop
	maxAttempts := 3
	defaultBackoff := 5 * time.Second

	for {
		attempt++

		// check max attempts
		if attempt > maxAttempts {
			return nil, attempt, 0, fmt.Errorf("max attempts exceeded (%d)", maxAttempts)
		}

		// do request
		resp, err := c.httpClient.Do(req)
		if err != nil {

			// exit on context cancellation
			if errors.Is(err, context.Canceled) {
				return nil, attempt, 499, err // 499: Client closed request
			}

			// retry on context deadline exceeded
			if errors.Is(err, context.DeadlineExceeded) {
				c.infoLog.Info("context deadline exceeded, retrying", "attempt", attempt)
				_ = lystime.Sleep(ctx, defaultBackoff)
				continue
			}

			// retry on net timeout
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				c.infoLog.Info("request timed out, retrying", "attempt", attempt)
				_ = lystime.Sleep(ctx, defaultBackoff)
				continue
			}

			if resp != nil {
				statusCode = resp.StatusCode
			}
			return nil, attempt, statusCode, fmt.Errorf("c.httpClient.Do failed: %w", err)
		}

		// read body
		respBody, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, attempt, resp.StatusCode, fmt.Errorf("io.ReadAll failed: %w", err)
		}

		// closing body immediately rather in defer due to this code being in a retry loop
		resp.Body.Close()

		// exit on success
		if resp.StatusCode == http.StatusOK {
			return respBody, attempt, resp.StatusCode, nil
		}

		// error: switch on status code
		switch resp.StatusCode {

		// retry on temporary server errors
		case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			c.infoLog.Info("temporary server error, retrying", "statusCode", resp.StatusCode, "attempt", attempt)
			_ = lystime.Sleep(ctx, defaultBackoff)
			continue

		// add handling for other codes as needed

		default:
			return nil, attempt, resp.StatusCode, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

	} // next attempt
}
