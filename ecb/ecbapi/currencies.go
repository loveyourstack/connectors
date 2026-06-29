package ecbapi

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"time"

	"github.com/loveyourstack/connectors/ecb/stores/ecbapicall"
	"github.com/loveyourstack/connectors/ecb/stores/ecbcurrency"
)

type Currency struct {
	Code string
	Name string
}

// GetApiCurrencies returns all available currencies
func (c Client) GetApiCurrencies(ctx context.Context) (currencies []Currency, err error) {

	dataStructureUrl := baseUrl + "/service/datastructure/ECB/ECB_EXR1/1.0?references=children"

	// prepare call log input
	callInput := ecbapicall.Input{
		Attempt:    0, // set from doRequest response
		DurationMs: 0, // set in defer
		Endpoint:   dataStructureUrl,
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
			c.logger.Error("c.callStore.Insert failed", "error", err, "callInput", callInput)
		}
	}()

	// get dataStructure XML containing currencies from API
	respBody, attempt, statusCode, err := c.doRequest(ctx, http.MethodGet, dataStructureUrl, nil)
	callInput.Attempt = attempt
	callInput.StatusCode = statusCode
	if err != nil {
		callInput.Result = "request error: " + err.Error()
		return nil, fmt.Errorf("c.doRequest failed: %w", err)
	}

	// unmarshal body into struct
	respS := dataStructureResponse{}
	err = xml.Unmarshal(respBody, &respS)
	if err != nil {
		errMsg := "xml.Unmarshal failed: "
		callInput.Result = errMsg + err.Error()
		return nil, fmt.Errorf("%s: %w", errMsg, err)
	}

	// parse out currencies
	for _, codeList := range respS.Structures.Codelists.Codelist {
		if codeList.Name.Text != "Currency code list" {
			continue
		}

		for _, code := range codeList.Code {
			currencies = append(currencies, Currency{
				Code: code.ID,
				Name: code.Name.Text,
			})
		}
	}
	if len(currencies) == 0 {
		errStr := "currencies could not be parsed out of datastructure xml response"
		callInput.Result = errStr
		return nil, fmt.Errorf("%s", errStr)
	}

	callInput.Result = "OK"

	return currencies, nil
}

// dataStructureResponse only contains the fields needed to parse out the currencies from the full API response, not all fields are included here
type dataStructureResponse struct {
	XMLName    xml.Name `xml:"Structure"`
	Structures struct {
		Codelists struct {
			Codelist []struct {
				Name struct {
					Text string `xml:",chardata"`
				} `xml:"Name"`
				Code []struct {
					ID   string `xml:"id,attr"`
					Name struct {
						Text string `xml:",chardata"`
					} `xml:"Name"`
				} `xml:"Code"`
			} `xml:"Codelist"`
		} `xml:"Codelists"`
	} `xml:"Structures"`
}

func (c Client) GetCurrencies(ctx context.Context) (items []ecbcurrency.Input, err error) {

	apiItems, err := c.GetApiCurrencies(ctx)
	if err != nil {
		return nil, fmt.Errorf("c.GetApiCurrencies failed: %w", err)
	}

	for _, apiItem := range apiItems {
		items = append(items, apiCurrencyToItem(apiItem))
	}

	return items, nil
}

func (c Client) GetCurrenciesMap(ctx context.Context) (itemsMap map[string]ecbcurrency.Model, err error) {

	items, err := c.GetCurrencies(ctx)
	if err != nil {
		return nil, fmt.Errorf("c.GetCurrencies failed: %w", err)
	}

	// convert to map with Code as key
	itemsMap = make(map[string]ecbcurrency.Model)
	for _, input := range items {
		item := ecbcurrency.Model{
			Input: input,
		}
		itemsMap[input.Code] = item
	}

	return itemsMap, nil
}

func apiCurrencyToItem(apiItem Currency) (item ecbcurrency.Input) {

	item = ecbcurrency.Input{
		Code: apiItem.Code,
		Name: apiItem.Name,
	}

	return item
}
