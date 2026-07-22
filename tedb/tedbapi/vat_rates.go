package tedbapi

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/loveyourstack/connectors/tedb/stores/tedbapicall"
	"github.com/loveyourstack/connectors/tedb/stores/tedbvatcategory"
	"github.com/loveyourstack/connectors/tedb/stores/tedbvatcncode"
	"github.com/loveyourstack/connectors/tedb/stores/tedbvatcpacode"
	"github.com/loveyourstack/connectors/tedb/stores/tedbvatrate"
	"github.com/loveyourstack/lys/lysset"
	"github.com/loveyourstack/lys/lystype"
)

// from: https://ec.europa.eu/taxation_customs/tedb/ws/VatRetrievalService.wsdl

type retrieveVatRatesResponse struct {
	XMLName               xml.Name              `xml:"retrieveVatRatesRespMsg"`
	AdditionalInformation additionalInformation `xml:"additionalInformation"`
	VatRateResults        []VatRateResult       `xml:"vatRateResults"`
}

type additionalInformation struct {
	Countries *countries `xml:"countries"`
}

type countries struct {
	Country []countryInformation `xml:"country"`
}

type countryInformation struct {
	ISOCode         string `xml:"isoCode"`
	CNCodeProvided  bool   `xml:"cnCodeProvided"`
	CPACodeProvided bool   `xml:"cpaCodeProvided"`
}

// VatRateResult represents a single VAT rate result from the TEDB API.
type VatRateResult struct {
	MemberState string        `xml:"memberState"`
	Type        string        `xml:"type"`
	Rate        RateValue     `xml:"rate"`
	SituationOn string        `xml:"situationOn"`
	CNCodes     *ResponseCode `xml:"cnCodes"`  // CN: for goods
	CPACodes    *ResponseCode `xml:"cpaCodes"` // CPA: for services
	Category    *Category     `xml:"category"`
	Comment     string        `xml:"comment"`
}

type RateValue struct {
	Type  string   `xml:"type"`
	Value *float64 `xml:"value"`
}

type ResponseCode struct {
	Code []ResponseCodeDetails `xml:"code"`
}

type ResponseCodeDetails struct {
	Value       string `xml:"value"`
	Description string `xml:"description"`
}

type Category struct {
	Identifier  string `xml:"identifier"`
	Description string `xml:"description"`
}

type retrieveVatRatesReqMsg struct {
	XMLName      xml.Name            `xml:"urn:ec.europa.eu:taxud:tedb:services:v1:IVatRetrievalService retrieveVatRatesReqMsg"`
	MemberStates requestMemberStates `xml:"urn:ec.europa.eu:taxud:tedb:services:v1:IVatRetrievalService:types memberStates"`
	From         string              `xml:"urn:ec.europa.eu:taxud:tedb:services:v1:IVatRetrievalService:types from,omitempty"`
	To           string              `xml:"urn:ec.europa.eu:taxud:tedb:services:v1:IVatRetrievalService:types to"`
}

type requestMemberStates struct {
	ISOCode []string `xml:"urn:ec.europa.eu:taxud:tedb:services:v1:IVatRetrievalService:types isoCode"`
}

type soapEnvelopeResponse struct {
	XMLName xml.Name         `xml:"http://schemas.xmlsoap.org/soap/envelope/ Envelope"`
	Body    soapBodyResponse `xml:"http://schemas.xmlsoap.org/soap/envelope/ Body"`
}

type soapBodyResponse struct {
	Response *retrieveVatRatesResponse `xml:"urn:ec.europa.eu:taxud:tedb:services:v1:IVatRetrievalService retrieveVatRatesRespMsg"`
	Fault    *soapFault                `xml:"Fault"`
}

type soapFault struct {
	FaultCode   string          `xml:"faultcode"`
	FaultString string          `xml:"faultstring"`
	Detail      soapFaultDetail `xml:"detail"`
}

type soapFaultDetail struct {
	RetrieveVatRatesFaultMsg *retrieveVatRatesFaultMsg `xml:"urn:ec.europa.eu:taxud:tedb:services:v1:IVatRetrievalService retrieveVatRatesFaultMsg"`
}

type retrieveVatRatesFaultMsg struct {
	Errors []retrieveVatRatesError `xml:"error"`
}

type retrieveVatRatesError struct {
	Code        string `xml:"code"`
	Description string `xml:"description"`
}

// GetApiVatRates retrieves VAT rates for the supplied member states and date range from the TEDB API.
func (c Client) GetApiVatRates(ctx context.Context, countryISOs []string, startDate, endDate time.Time) (results []VatRateResult, err error) {

	// check params
	for i, countryISO := range countryISOs {
		countryISO = strings.ToUpper(strings.TrimSpace(countryISO))
		if len(countryISO) != 2 {
			return nil, fmt.Errorf("countryISO must be a 2-letter ISO code")
		}
		countryISOs[i] = countryISO
	}
	if startDate.IsZero() || endDate.IsZero() {
		return nil, fmt.Errorf("startDate and endDate are required")
	}
	if startDate.After(endDate) {
		return nil, fmt.Errorf("startDate must be before or equal to endDate")
	}

	// prepare call log input
	callInput := tedbapicall.Input{
		Attempt:    0, // set from doRequest response
		DurationMs: 0, // set in defer
		Endpoint:   fmt.Sprintf("%s (%s - %s)", baseUrl, startDate.Format(lystype.DateFormat), endDate.Format(lystype.DateFormat)),
		Method:     http.MethodPost,
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

	// create SOAP request payload
	payload := retrieveVatRatesReqMsg{
		MemberStates: requestMemberStates{ISOCode: countryISOs},
		From:         startDate.Format(lystype.DateFormat),
		To:           endDate.Format(lystype.DateFormat),
	}
	payloadXML, err := xml.Marshal(payload)
	if err != nil {
		errMsg := "xml.Marshal failed: "
		callInput.Result = errMsg + err.Error()
		return nil, fmt.Errorf("%s%w", errMsg, err)
	}

	const (
		tedbVatServiceNamespace        = "urn:ec.europa.eu:taxud:tedb:services:v1:VatRetrievalService"
		tedbVatServiceMessageNamespace = "urn:ec.europa.eu:taxud:tedb:services:v1:IVatRetrievalService"
		tedbVatServiceTypesNamespace   = "urn:ec.europa.eu:taxud:tedb:services:v1:IVatRetrievalService:types"
		tedbRetrieveVatSoapAction      = "urn:ec.europa.eu:taxud:tedb:services:v1:VatRetrievalService/RetrieveVatRates"
	)

	// create SOAP envelope
	envelopeXML := []byte(
		`<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:svc="` + tedbVatServiceMessageNamespace + `" xmlns:typ="` + tedbVatServiceTypesNamespace + `"><soapenv:Body>` +
			string(payloadXML) +
			`</soapenv:Body></soapenv:Envelope>`,
	)

	// send SOAP request
	respBody, attempt, statusCode, err := c.doRequest(ctx, baseUrl, tedbRetrieveVatSoapAction, bytes.NewReader(envelopeXML))
	callInput.Attempt = attempt
	callInput.StatusCode = statusCode
	if err != nil {
		callInput.Result = "request error: " + err.Error()
		return nil, fmt.Errorf("doRequest failed: %w", err)
	}

	// unmarshal SOAP response
	var soapResp soapEnvelopeResponse
	if err := xml.Unmarshal(respBody, &soapResp); err != nil {
		errMsg := "xml.Unmarshal failed: "
		callInput.Result = errMsg + err.Error()
		return nil, fmt.Errorf("%s%w", errMsg, err)
	}

	// check for SOAP fault
	if soapResp.Body.Fault != nil {
		fault := soapResp.Body.Fault

		// if faults are in the detail, include them in the error message
		if fault.Detail.RetrieveVatRatesFaultMsg != nil && len(fault.Detail.RetrieveVatRatesFaultMsg.Errors) > 0 {
			errParts := make([]string, 0, len(fault.Detail.RetrieveVatRatesFaultMsg.Errors))
			for _, faultErr := range fault.Detail.RetrieveVatRatesFaultMsg.Errors {
				errParts = append(errParts, fmt.Sprintf("%s: %s", faultErr.Code, faultErr.Description))
			}
			errMsg := fmt.Sprintf("SOAP fault %s (%s): %s", fault.FaultCode, fault.FaultString, strings.Join(errParts, "; "))
			callInput.Result = errMsg
			return nil, fmt.Errorf("%s", errMsg)
		}

		// otherwise, return the fault code and string
		errMsg := fmt.Sprintf("SOAP fault %s: %s", fault.FaultCode, fault.FaultString)
		callInput.Result = errMsg
		return nil, fmt.Errorf("%s", errMsg)
	}

	// check for missing response
	if soapResp.Body.Response == nil {
		errMsg := "SOAP response did not contain retrieveVatRatesRespMsg"
		callInput.Result = errMsg
		return nil, fmt.Errorf("%s", errMsg)
	}

	// check for missing results
	if len(soapResp.Body.Response.VatRateResults) == 0 {
		callInput.Result = ErrNoRatesFound.Error()
		return nil, ErrNoRatesFound
	}

	// success
	callInput.Result = "OK"
	return soapResp.Body.Response.VatRateResults, nil
}

// GetVatRates retrieves VAT rates for the supplied member states and date range from the TEDB API.
func (c Client) GetVatRates(ctx context.Context, countryISOs []string, startDate, endDate time.Time) (items []tedbvatrate.Input, err error) {

	apiItems, err := c.GetApiVatRates(ctx, countryISOs, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("c.GetApiVatRates failed: %w", err)
	}

	// do a pass through API items to get distinct categories and codes
	cats, cnCodes, cpaCodes, err := c.getDistinctCategoriesAndCodes(apiItems)
	if err != nil {
		return nil, fmt.Errorf("c.getDistinctCategoriesAndCodes failed: %w", err)
	}

	// get categories map, inserting new ones as needed
	catMap, err := c.getCategoriesMap(ctx, cats)
	if err != nil {
		return nil, fmt.Errorf("c.getCategoriesMap failed: %w", err)
	}

	// add new CN and CPA codes to db
	err = c.addNewCodesToDb(ctx, cnCodes, cpaCodes)
	if err != nil {
		return nil, fmt.Errorf("c.addNewCodesToDb failed: %w", err)
	}

	for _, apiItem := range apiItems {
		_item, err := c.apiVatRateToItem(apiItem, catMap)
		if err != nil {
			return nil, fmt.Errorf("apiVatRateToItem failed: %w", err)
		}
		items = append(items, _item)
	}

	return items, nil
}

func (c Client) GetVatRatesMap(ctx context.Context, countryISOs []string, startDate, endDate time.Time) (itemsMap map[string]tedbvatrate.Model, err error) {

	items, err := c.GetVatRates(ctx, countryISOs, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("c.GetVatRates failed: %w", err)
	}

	// normalize startDate and endDate to midnight UTC to ensure that situation_on comparisons work correctly below
	startDateOnly := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, time.UTC)
	endDateOnly := time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 0, 0, 0, 0, time.UTC)

	// convert to map with situation_on + member_state + type + category_fk + cn_codes + cpa_codes + comment as key
	itemsMap = make(map[string]tedbvatrate.Model)
	for _, input := range items {

		// API sometimes returns items outside the requested date range. Filter them out so that API to DB sync comparison works correctly
		situationOnT := time.Time(input.SituationOn)
		situationOnDate := time.Date(situationOnT.Year(), situationOnT.Month(), situationOnT.Day(), 0, 0, 0, 0, time.UTC)
		if situationOnDate.Before(startDateOnly) || situationOnDate.After(endDateOnly) {
			//c.logger.Debug("skipping API item outside requested date range", "situationOn", situationOnDate, "startDate", startDateOnly, "endDate", endDateOnly)
			continue
		}

		// DE data in 2025-2026 is duplicated with records where the comment starts with "VAT - Import - ". Filter them out
		if input.MemberState == "DE" && strings.HasPrefix(input.Comment, "VAT - Import - ") {
			continue
		}

		item := tedbvatrate.Model{
			Input: input,
		}
		key := fmt.Sprintf("%s+%s+%s+%v+%s+%s+%s", input.SituationOn.String(), input.MemberState, input.Type, input.CategoryFk,
			strings.Join(input.CnCodes, ","), strings.Join(input.CpaCodes, ","), input.Comment)

		// there are a few duplicates remaining even with this large key. Sometimes they are explained in the comment field, but usually not.
		// de-duping the data is more important, so let the later record overwrite the earlier one
		itemsMap[key] = item
	}

	return itemsMap, nil
}

func (c Client) apiVatRateToItem(apiItem VatRateResult, catMap map[string]int64) (item tedbvatrate.Input, err error) {

	rate := 0.0
	if apiItem.Rate.Value != nil {
		rate = *apiItem.Rate.Value
	}

	if len(apiItem.SituationOn) < 10 {
		return item, fmt.Errorf("apiItem.SituationOn is too short to parse as a date: '%s'", apiItem.SituationOn)
	}
	situationOnT, err := time.Parse("2006-01-02", apiItem.SituationOn[0:10]) // ignore timezone offset so that comparison with start/end date works correctly
	if err != nil {
		return item, fmt.Errorf("time.Parse failed for situationOn '%s': %w", apiItem.SituationOn, err)
	}

	item = tedbvatrate.Input{
		CategoryFk:  -1, // None
		CnCodes:     []string{},
		Comment:     apiItem.Comment,
		CpaCodes:    []string{},
		MemberState: apiItem.MemberState,
		RateType:    apiItem.Rate.Type,
		Rate:        rate,
		SituationOn: lystype.Date(situationOnT),
		Type:        apiItem.Type,
	}

	if apiItem.Category != nil {
		if id, ok := catMap[apiItem.Category.Identifier]; ok {
			item.CategoryFk = id
		} else {
			return item, fmt.Errorf("category identifier '%s' not found in catMap", apiItem.Category.Identifier)
		}
	}
	if apiItem.CNCodes != nil {
		for _, cnCode := range apiItem.CNCodes.Code {
			if cnCode.Value == "" {
				continue
			}
			item.CnCodes = append(item.CnCodes, cnCode.Value)
		}
		slices.Sort(item.CnCodes) // sort for deterministic comparison
	}
	if apiItem.CPACodes != nil {
		for _, cpaCode := range apiItem.CPACodes.Code {
			if cpaCode.Value == "" {
				continue
			}
			item.CpaCodes = append(item.CpaCodes, cpaCode.Value)
		}
		slices.Sort(item.CpaCodes) // sort for deterministic comparison
	}

	return item, nil
}

func (c Client) getCategoriesMap(ctx context.Context, inputs []tedbvatcategory.Input) (catMap map[string]int64, err error) {

	// select existing categories from DB
	catMap, err = c.catStore.SelectIdentifierIdMap(ctx)
	if err != nil {
		return nil, fmt.Errorf("c.catStore.SelectIdentifierIdMap failed: %w", err)
	}

	// check if new categories need to be inserted
	newCats := []tedbvatcategory.Input{}
	for _, input := range inputs {
		if _, ok := catMap[input.Identifier]; !ok {
			newCats = append(newCats, input)
		}
	}

	// return if no new categories to insert
	if len(newCats) == 0 {
		return catMap, nil
	}

	// insert new categories
	for _, newCat := range newCats {
		newId, err := c.catStore.Insert(ctx, newCat)
		if err != nil {
			return nil, fmt.Errorf("c.catStore.Insert failed for category: %s: %w", newCat.Identifier, err)
		}
		c.logger.Info("inserted category", "identifier", newCat.Identifier, "id", newId)
		catMap[newCat.Identifier] = newId
	}

	// return updated map
	return catMap, nil
}

func (c Client) getDistinctCategoriesAndCodes(apiItems []VatRateResult) (cats []tedbvatcategory.Input, cnCodes []tedbvatcncode.Input, cpaCodes []tedbvatcpacode.Input, err error) {

	catSeen := lysset.New[string]()
	cnCodeSeen := lysset.New[string]()
	cpaCodeSeen := lysset.New[string]()

	for _, apiItem := range apiItems {

		// categories
		if apiItem.Category != nil {
			if !catSeen.Contains(apiItem.Category.Identifier) {
				catSeen.Add(apiItem.Category.Identifier)
				cats = append(cats, tedbvatcategory.Input{
					Description: apiItem.Category.Description,
					Identifier:  apiItem.Category.Identifier,
				})
			}
		}

		// cn codes
		if apiItem.CNCodes != nil {
			for _, cnCode := range apiItem.CNCodes.Code {
				if cnCode.Value == "" {
					continue
				}
				if !cnCodeSeen.Contains(cnCode.Value) {
					cnCodeSeen.Add(cnCode.Value)
					cnCodes = append(cnCodes, tedbvatcncode.Input{
						Description: cnCode.Description,
						Value:       cnCode.Value,
					})
				}
			}
		}

		// cpa codes
		if apiItem.CPACodes != nil {
			for _, cpaCode := range apiItem.CPACodes.Code {
				if cpaCode.Value == "" {
					continue
				}
				if !cpaCodeSeen.Contains(cpaCode.Value) {
					cpaCodeSeen.Add(cpaCode.Value)
					cpaCodes = append(cpaCodes, tedbvatcpacode.Input{
						Description: cpaCode.Description,
						Value:       cpaCode.Value,
					})
				}
			}
		}
	}

	return cats, cnCodes, cpaCodes, nil
}

func (c Client) addNewCodesToDb(ctx context.Context, cnCodes []tedbvatcncode.Input, cpaCodes []tedbvatcpacode.Input) (err error) {

	// get existing codes from DB
	existingCnCodes, err := c.cnCodeStore.SelectValueSet(ctx)
	if err != nil {
		return fmt.Errorf("c.cnCodeStore.SelectValueSet failed: %w", err)
	}
	existingCpaCodes, err := c.cpaCodeStore.SelectValueSet(ctx)
	if err != nil {
		return fmt.Errorf("c.cpaCodeStore.SelectValueSet failed: %w", err)
	}

	// filter out codes that already exist in DB
	newCnCodes := []tedbvatcncode.Input{}
	for _, cnCode := range cnCodes {
		if !existingCnCodes.Contains(cnCode.Value) {
			newCnCodes = append(newCnCodes, cnCode)
		}
	}
	newCpaCodes := []tedbvatcpacode.Input{}
	for _, cpaCode := range cpaCodes {
		if !existingCpaCodes.Contains(cpaCode.Value) {
			newCpaCodes = append(newCpaCodes, cpaCode)
		}
	}

	// add new codes to db
	if len(newCnCodes) > 0 {
		_, err := c.cnCodeStore.BulkInsert(ctx, newCnCodes)
		if err != nil {
			return fmt.Errorf("c.cnCodeStore.BulkInsert failed: %w", err)
		}
		c.logger.Info("inserted CN codes", "count", len(newCnCodes))
	}

	if len(newCpaCodes) > 0 {
		_, err := c.cpaCodeStore.BulkInsert(ctx, newCpaCodes)
		if err != nil {
			return fmt.Errorf("c.cpaCodeStore.BulkInsert failed: %w", err)
		}
		c.logger.Info("inserted CPA codes", "count", len(newCpaCodes))
	}

	return nil
}
