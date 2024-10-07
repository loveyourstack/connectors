package ecbapi

import (
	"encoding/csv"
	"fmt"
	"net/url"
	"strconv"
	"time"

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

// GetAPIExchangeRates returns average daily or monthly exchange rates from baseCurr to all other available currencies
func (c Client) GetAPIExchangeRates(baseCurr string, freq Frequency, startDate, endDate time.Time) (exRates []ExchangeRate, err error) {

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
	path := fmt.Sprintf("/%s..%s.SP00.A", freq, baseCurr)
	params := url.Values{}
	params.Add("detail", "dataonly")
	params.Add("format", "csvdata")
	params.Add("startPeriod", startDate.Format(dateFormat))
	params.Add("endPeriod", endDate.Format(dateFormat))
	exrUrl := exrBaseUrl + path + "?" + params.Encode()

	// get rates
	resp, err := c.HttpClient.Get(exrUrl)
	if err != nil {
		return nil, fmt.Errorf("c.HttpClient.Get failed: %w", err)
	}
	defer resp.Body.Close()

	// read csv content
	csvContent, err := csv.NewReader(resp.Body).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("csv.NewReader().ReadAll failed: %w", err)
	}

	if len(csvContent) < 2 {
		return nil, fmt.Errorf("no rates found for these params")
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
			return nil, fmt.Errorf("strconv.ParseFloat failed for rate '%s': %w", lineA[7], err)
		}
		exRate.Rate = float32(rateFl64)

		exRates = append(exRates, exRate)
	}

	return exRates, nil
}

func (c Client) GetExchangeRates(baseCurr string, freq Frequency, startDate, endDate time.Time, currMap map[string]int64) (items []ecbexchangerate.Input, err error) {

	apiItems, err := c.GetAPIExchangeRates(baseCurr, freq, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("c.GetAPIExchangeRates failed: %w", err)
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

func (c Client) GetExchangeRatesMap(baseCurr string, freq Frequency, startDate, endDate time.Time, currMap map[string]int64) (itemsMap map[string]ecbexchangerate.Model, err error) {

	items, err := c.GetExchangeRates(baseCurr, freq, startDate, endDate, currMap)
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
