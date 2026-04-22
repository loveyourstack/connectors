package ecbapi

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/loveyourstack/connectors/stores/ecb/ecbapicall"
	"github.com/loveyourstack/connectors/stores/ecb/ecbexchangerate"
	"github.com/loveyourstack/lys/lystype"
)

type ExchangeRate struct {
	FromCurr  string // base currency code
	ToCurr    string // code
	Freq      Frequency
	PeriodStr string // daily: YYYY-MM-DD, monthly: YYYY-MM
	Rate      float32
}

// GetApiExchangeRates returns average daily or monthly exchange rates from baseCurr to all other available currencies
func (c Client) GetApiExchangeRates(ctx context.Context, baseCurr string, freq Frequency, startDate, endDate time.Time) (exRates []ExchangeRate, err error) {

	// validate dates
	if startDate.After(time.Now()) {
		return nil, fmt.Errorf("startDate must be before now")
	}
	if startDate.After(endDate) {
		return nil, fmt.Errorf("startDate must be before endDate")
	}
	if endDate.After(time.Now()) {
		return nil, fmt.Errorf("endDate must be before now")
	}

	// set vars depending on freq
	var dateFormat string
	switch freq {
	case Daily:
		dateFormat = "2006-01-02"
	case Monthly:
		dateFormat = "2006-01"
	default:
		return nil, fmt.Errorf("invalid freq '%s'", freq)
	}

	// build URL
	exrBaseUrl := baseUrl + "/service/data/EXR"
	params := url.Values{}
	params.Add("detail", "dataonly")
	params.Add("format", "csvdata")
	params.Add("startPeriod", startDate.Format(dateFormat))
	params.Add("endPeriod", endDate.Format(dateFormat))
	exrUrl := fmt.Sprintf("%s/%s..%s.SP00.A?%s", exrBaseUrl, freq, baseCurr, params.Encode())

	start := time.Now()

	// prepare call log input
	callInput := ecbapicall.Input{
		Attempt:    0, // set from doRequest response
		DurationMs: 0, // set in defer
		Endpoint:   exrUrl,
		Method:     http.MethodGet,
		Page:       1,
		Result:     "", // set below depending on success or error
		StatusCode: 0,  // set from doRequest response
	}

	// defer call log to capture duration and result
	defer func() {
		callInput.DurationMs = time.Since(start).Milliseconds()

		_, err := c.CallStore.Insert(context.Background(), callInput) // use background context to ensure call log is inserted even if main context is cancelled
		if err != nil {
			c.ErrorLog.Error("c.CallStore.Insert failed", "error", err, "callInput", callInput)
		}
	}()

	// get rates in CSV format from API
	respBody, attempt, statusCode, err := c.doRequest(ctx, http.MethodGet, exrUrl, nil)
	callInput.Attempt = attempt
	callInput.StatusCode = statusCode
	if err != nil {

		// exit without err on context cancellation
		if errors.Is(err, context.Canceled) {
			callInput.Result = "context canceled"
			return nil, nil
		}

		callInput.Result = "request error: " + err.Error()
		return nil, fmt.Errorf("c.doRequest failed: %w", err)
	}

	// read csv content
	csvContent, err := csv.NewReader(io.NopCloser(bytes.NewReader(respBody))).ReadAll()
	if err != nil {
		errMsg := "csv.NewReader().ReadAll failed: "
		callInput.Result = errMsg + err.Error()
		return nil, fmt.Errorf("%s: %w", errMsg, err)
	}

	if len(csvContent) < 2 {
		errMsg := "no rates found for these params"
		callInput.Result = errMsg
		return nil, fmt.Errorf("%s", errMsg)
	}

	/* csvContent looks like this:
	KEY,FREQ,CURRENCY,CURRENCY_DENOM,EXR_TYPE,EXR_SUFFIX,TIME_PERIOD,OBS_VALUE
	EXR.D.AUD.EUR.SP00.A,D,AUD,EUR,SP00,A,2024-09-02,1.6322
	EXR.D.AUD.EUR.SP00.A,D,AUD,EUR,SP00,A,2024-09-03,1.6394
	*/

	// for each line
	for i, lineA := range csvContent {

		// skip header
		if i == 0 {
			continue
		}

		// parse out the values
		exRate := ExchangeRate{
			FromCurr:  baseCurr,
			ToCurr:    lineA[2],
			Freq:      freq,
			PeriodStr: lineA[6],
		}

		rateFl64, err := strconv.ParseFloat(lineA[7], 32)
		if err != nil {
			errMsg := fmt.Sprintf("strconv.ParseFloat failed for rate '%s' on line %d: ", lineA[7], i)
			callInput.Result = errMsg + err.Error()
			return nil, fmt.Errorf("%s: %w", errMsg, err)
		}
		exRate.Rate = float32(rateFl64)

		exRates = append(exRates, exRate)
	}

	callInput.Result = "OK"

	return exRates, nil
}

func (c Client) GetExchangeRates(ctx context.Context, baseCurr string, freq Frequency, startDate, endDate time.Time, currMap map[string]int64) (items []ecbexchangerate.Input, err error) {

	apiItems, err := c.GetApiExchangeRates(ctx, baseCurr, freq, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("c.GetApiExchangeRates failed: %w", err)
	}

	for _, apiItem := range apiItems {
		_item, err := apiExchangeRateToItem(apiItem, currMap)
		if err != nil {
			return nil, fmt.Errorf("apiExchangeRateToItem failed: %w", err)
		}
		items = append(items, _item)
	}

	return items, nil
}

func (c Client) GetExchangeRatesMap(ctx context.Context, baseCurr string, freq Frequency, startDate, endDate time.Time, currMap map[string]int64) (itemsMap map[string]ecbexchangerate.Model, err error) {

	items, err := c.GetExchangeRates(ctx, baseCurr, freq, startDate, endDate, currMap)
	if err != nil {
		return nil, fmt.Errorf("c.GetExchangeRates failed: %w", err)
	}

	// convert to map with day+toCurrFk as key
	itemsMap = make(map[string]ecbexchangerate.Model)
	for _, input := range items {
		item := ecbexchangerate.Model{
			Input: input,
		}
		itemsMap[input.Day.Format(lystype.DateFormat)+"+"+fmt.Sprintf("%v", input.ToCurrencyFk)] = item
	}

	return itemsMap, nil
}

func apiExchangeRateToItem(apiItem ExchangeRate, currMap map[string]int64) (item ecbexchangerate.Input, err error) {

	// day: if monthly, use 1st of month
	var day lystype.Date
	switch apiItem.Freq {
	case Daily:
		periodTime, err := time.Parse("2006-01-02", apiItem.PeriodStr)
		if err != nil {
			return ecbexchangerate.Input{}, fmt.Errorf("time.Parse (Daily) failed for PeriodStr '%s': %w", apiItem.PeriodStr, err)
		}
		day = lystype.Date(periodTime)
	case Monthly:
		periodTime, err := time.Parse("2006-01", apiItem.PeriodStr)
		if err != nil {
			return ecbexchangerate.Input{}, fmt.Errorf("time.Parse (Daily) failed for PeriodStr '%s': %w", apiItem.PeriodStr, err)
		}
		day = lystype.Date(periodTime)
	default:
		return ecbexchangerate.Input{}, fmt.Errorf("invalid frequency: %s", apiItem.Freq)
	}

	// from curr
	fromCurrFk, ok := currMap[apiItem.FromCurr]
	if !ok {
		return ecbexchangerate.Input{}, fmt.Errorf("from currency code not in map: %s", apiItem.FromCurr)
	}

	// to curr
	toCurrFk, ok := currMap[apiItem.ToCurr]
	if !ok {
		return ecbexchangerate.Input{}, fmt.Errorf("to currency code not in map: %s", apiItem.ToCurr)
	}

	item = ecbexchangerate.Input{
		Day:            day,
		Frequency:      apiItem.Freq.String(),
		FromCurrencyFk: fromCurrFk,
		Rate:           apiItem.Rate,
		ToCurrencyFk:   toCurrFk,
	}

	return item, nil
}
