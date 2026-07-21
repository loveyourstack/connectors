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
	"github.com/loveyourstack/connectors/ecb/stores/ecbapicall"
	"github.com/loveyourstack/lys/lystime"
)

// Docs: https://data.ecb.europa.eu/help/api/data

const (
	baseUrl     string = "https://data-api.ecb.europa.eu"
	timeoutSecs int    = 20
)

var ErrNoRatesFound = errors.New("no rates found")

type Client struct {
	callStore  ecbapicall.Store
	httpClient *http.Client
	logger     *slog.Logger
}

func NewClient(db *pgxpool.Pool, logger *slog.Logger) (client Client) {

	apiShortname := "ecb"

	return Client{
		callStore: ecbapicall.Store{Db: db},
		httpClient: &http.Client{
			Timeout: time.Duration(timeoutSecs) * time.Second,
		},
		logger: logger.With("api", apiShortname),
	}
}

func (c Client) doRequest(ctx context.Context, method, url string, body io.Reader) (respBody []byte, attempt, statusCode int, err error) {

	// start attempts loop
	maxAttempts := 3
	defaultBackoff := 5 * time.Second

	for {
		attempt++

		// check max attempts
		if attempt > maxAttempts {
			return nil, attempt, 0, fmt.Errorf("max attempts exceeded (%d)", maxAttempts)
		}

		// define request
		req, err := http.NewRequestWithContext(ctx, method, url, body)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("http.NewRequestWithContext failed: %w", err)
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
				c.logger.Info("context deadline exceeded, retrying", "attempt", attempt)
				_ = lystime.Sleep(ctx, defaultBackoff)
				continue
			}

			// retry on net timeout
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				c.logger.Info("request timed out, retrying", "attempt", attempt)
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
			resp.Body.Close()
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
			c.logger.Info("temporary server error, retrying", "statusCode", resp.StatusCode, "attempt", attempt)
			_ = lystime.Sleep(ctx, defaultBackoff)
			continue

		// add handling for other codes as needed

		default:
			return nil, attempt, resp.StatusCode, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

	} // next attempt
}
