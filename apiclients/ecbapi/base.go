package ecbapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
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

func (c Client) doRequest(ctx context.Context, method, url string, body io.Reader) (respBody []byte, attempt, statusCode int, err error) {

	// define request
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("http.NewRequestWithContext failed: %w", err)
	}

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
		resp, err := c.HttpClient.Do(req)
		if err != nil {

			// exit on context cancellation
			if errors.Is(err, context.Canceled) {
				return nil, attempt, 499, err // 499: Client closed request
			}

			// retry on context deadline exceeded
			if errors.Is(err, context.DeadlineExceeded) {
				c.InfoLog.Info("context deadline exceeded, retrying", "attempt", attempt)
				time.Sleep(defaultBackoff)
				continue
			}

			// retry on net timeout
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				c.InfoLog.Info("request timed out, retrying", "attempt", attempt)
				time.Sleep(defaultBackoff)
				continue
			}

			if resp != nil {
				statusCode = resp.StatusCode
			}
			return nil, attempt, statusCode, fmt.Errorf("c.HttpClient.Do failed: %w", err)
		}
		defer resp.Body.Close()

		// read body
		respBody, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, attempt, resp.StatusCode, fmt.Errorf("io.ReadAll failed: %w", err)
		}

		// exit on success
		if resp.StatusCode == http.StatusOK {
			return respBody, attempt, resp.StatusCode, nil
		}

		// error: switch on status code
		switch resp.StatusCode {

		// retry on temporary server errors
		case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			c.InfoLog.Info("temporary server error, retrying", "statusCode", resp.StatusCode, "attempt", attempt)
			time.Sleep(defaultBackoff)
			continue

		// add handling for other codes as needed

		default:
			return nil, attempt, resp.StatusCode, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

	} // next attempt
}
