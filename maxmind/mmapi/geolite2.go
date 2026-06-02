package mmapi

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/loveyourstack/connectors/maxmind/stores/mmapicall"
)

func getDbCsvUrl(geoDb GeoLite2Db) string {
	return fmt.Sprintf("%s/%s-CSV/download?suffix=zip", baseUrl, geoDb)
}

func (c Client) GetApiGeoLite2DbLastModified(ctx context.Context, geoDb GeoLite2Db) (lastModified time.Time, err error) {

	csvUrl := getDbCsvUrl(geoDb)

	// prepare call log input
	callInput := mmapicall.Input{
		Attempt:    0, // set from doRequest response
		DurationMs: 0, // set in defer
		Endpoint:   csvUrl,
		Method:     http.MethodHead,
		Page:       1,
		Result:     "", // set below depending on success or error
		StatusCode: 0,  // set from doRequest response
	}

	start := time.Now()

	// defer call log to capture duration and result
	defer func() {
		callInput.DurationMs = time.Since(start).Milliseconds()

		_, err := c.callStore.Insert(context.Background(), callInput) // use background context to ensure call log is inserted even if main context is cancelled
		if err != nil {
			c.errorLog.Error("c.callStore.Insert failed", "error", err, "callInput", callInput)
		}
	}()

	// make HEAD request
	_, respHeader, attempt, statusCode, err := c.doRequest(ctx, http.MethodHead, csvUrl, nil)
	callInput.Attempt = attempt
	callInput.StatusCode = statusCode
	if err != nil {
		callInput.Result = "request error: " + err.Error()
		return time.Time{}, fmt.Errorf("c.doRequest failed: %w", err)
	}

	// try to get last-modified header
	lastModifiedStr := respHeader.Get("last-modified")
	if lastModifiedStr == "" {
		errStr := "missing last-modified header in response"
		callInput.Result = errStr
		return time.Time{}, fmt.Errorf("%s", errStr)
	}

	// parse last-modified header according to RFC1123 format, e.g. Fri, 29 May 2026 17:31:44 GMT
	lastModified, err = time.Parse(time.RFC1123, lastModifiedStr)
	if err != nil {
		errStr := "invalid last-modified header format: " + err.Error()
		callInput.Result = errStr
		return time.Time{}, fmt.Errorf("%s", errStr)
	}

	callInput.Result = "OK"

	return lastModified, nil
}

func (c Client) GetApiGeoLite2DbZip(ctx context.Context, geoDb GeoLite2Db) (respBody []byte, err error) {

	csvUrl := getDbCsvUrl(geoDb)

	// prepare call log input
	callInput := mmapicall.Input{
		Attempt:    0, // set from doRequest response
		DurationMs: 0, // set in defer
		Endpoint:   csvUrl,
		Method:     http.MethodGet,
		Page:       1,
		Result:     "", // set below depending on success or error
		StatusCode: 0,  // set from doRequest response
	}

	start := time.Now()

	// defer call log to capture duration and result
	defer func() {
		callInput.DurationMs = time.Since(start).Milliseconds()

		_, err := c.callStore.Insert(context.Background(), callInput) // use background context to ensure call log is inserted even if main context is cancelled
		if err != nil {
			c.errorLog.Error("c.callStore.Insert failed", "error", err, "callInput", callInput)
		}
	}()

	respBody, _, attempt, statusCode, err := c.doRequest(ctx, http.MethodGet, csvUrl, nil)
	callInput.Attempt = attempt
	callInput.StatusCode = statusCode
	if err != nil {
		callInput.Result = "request error: " + err.Error()
		return nil, fmt.Errorf("c.doRequest failed: %w", err)
	}

	callInput.Result = "OK"

	return respBody, nil
}
