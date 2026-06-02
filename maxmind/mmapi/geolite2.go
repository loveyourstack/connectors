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

	respBody, attempt, statusCode, err := c.doRequest(ctx, http.MethodGet, csvUrl, nil)
	callInput.Attempt = attempt
	callInput.StatusCode = statusCode
	if err != nil {
		callInput.Result = "request error: " + err.Error()
		return nil, fmt.Errorf("c.doRequest failed: %w", err)
	}

	callInput.Result = "OK"

	return respBody, nil
}
