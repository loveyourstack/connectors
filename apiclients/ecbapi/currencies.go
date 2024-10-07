package ecbapi

import (
	"encoding/xml"
	"fmt"
	"io"

	"github.com/loveyourstack/connectors/stores/ecb/ecbcurrency"
)

type Currency struct {
	Code string
	Name string
}

// GetApiCurrencies returns all available currencies
func (c Client) GetApiCurrencies() (currencies []Currency, err error) {

	dataStructureUrl := baseUrl + "/service/datastructure/ECB/ECB_EXR1/1.0?references=children"

	// get all data structures
	resp, err := c.HttpClient.Get(dataStructureUrl)
	if err != nil {
		return nil, fmt.Errorf("c.HttpClient.Get failed: %w", err)
	}
	defer resp.Body.Close()

	// read xml body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("io.ReadAll failed: %w", err)
	}

	// unmarshal body into struct
	respS := dataStructureResponse{}
	err = xml.Unmarshal(respBody, &respS)
	if err != nil {
		return nil, fmt.Errorf("xml.Unmarshal failed: %w", err)
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
		return nil, fmt.Errorf("currencies could not be parsed out of datastructure xml response")
	}

	return currencies, nil
}

type dataStructureResponse struct {
	XMLName        xml.Name `xml:"Structure"`
	Text           string   `xml:",chardata"`
	Xsi            string   `xml:"xsi,attr"`
	XML            string   `xml:"xml,attr"`
	Mes            string   `xml:"mes,attr"`
	Str            string   `xml:"str,attr"`
	Com            string   `xml:"com,attr"`
	SchemaLocation string   `xml:"schemaLocation,attr"`
	Header         struct {
		Text     string `xml:",chardata"`
		ID       string `xml:"ID"`
		Test     string `xml:"Test"`
		Prepared string `xml:"Prepared"`
		Sender   struct {
			Text string `xml:",chardata"`
			ID   string `xml:"id,attr"`
		} `xml:"Sender"`
		Receiver struct {
			Text string `xml:",chardata"`
			ID   string `xml:"id,attr"`
		} `xml:"Receiver"`
	} `xml:"Header"`
	Structures struct {
		Text                string `xml:",chardata"`
		OrganisationSchemes struct {
			Text         string `xml:",chardata"`
			AgencyScheme struct {
				Text                string `xml:",chardata"`
				URN                 string `xml:"urn,attr"`
				IsExternalReference string `xml:"isExternalReference,attr"`
				AgencyID            string `xml:"agencyID,attr"`
				ID                  string `xml:"id,attr"`
				IsFinal             string `xml:"isFinal,attr"`
				Version             string `xml:"version,attr"`
				Name                struct {
					Text string `xml:",chardata"`
					Lang string `xml:"lang,attr"`
				} `xml:"Name"`
				Agency []struct {
					Text string `xml:",chardata"`
					URN  string `xml:"urn,attr"`
					ID   string `xml:"id,attr"`
					Name struct {
						Text string `xml:",chardata"`
						Lang string `xml:"lang,attr"`
					} `xml:"Name"`
				} `xml:"Agency"`
			} `xml:"AgencyScheme"`
		} `xml:"OrganisationSchemes"`
		Codelists struct {
			Text     string `xml:",chardata"`
			Codelist []struct {
				Text                string `xml:",chardata"`
				URN                 string `xml:"urn,attr"`
				IsExternalReference string `xml:"isExternalReference,attr"`
				AgencyID            string `xml:"agencyID,attr"`
				ID                  string `xml:"id,attr"`
				IsFinal             string `xml:"isFinal,attr"`
				Version             string `xml:"version,attr"`
				Name                struct {
					Text string `xml:",chardata"`
					Lang string `xml:"lang,attr"`
				} `xml:"Name"`
				Code []struct {
					Text string `xml:",chardata"`
					URN  string `xml:"urn,attr"`
					ID   string `xml:"id,attr"`
					Name struct {
						Text string `xml:",chardata"`
						Lang string `xml:"lang,attr"`
					} `xml:"Name"`
				} `xml:"Code"`
			} `xml:"Codelist"`
		} `xml:"Codelists"`
		Concepts struct {
			Text          string `xml:",chardata"`
			ConceptScheme struct {
				Text                string `xml:",chardata"`
				URN                 string `xml:"urn,attr"`
				IsExternalReference string `xml:"isExternalReference,attr"`
				AgencyID            string `xml:"agencyID,attr"`
				ID                  string `xml:"id,attr"`
				IsFinal             string `xml:"isFinal,attr"`
				Version             string `xml:"version,attr"`
				Name                struct {
					Text string `xml:",chardata"`
					Lang string `xml:"lang,attr"`
				} `xml:"Name"`
				Concept []struct {
					Text string `xml:",chardata"`
					URN  string `xml:"urn,attr"`
					ID   string `xml:"id,attr"`
					Name struct {
						Text string `xml:",chardata"`
						Lang string `xml:"lang,attr"`
					} `xml:"Name"`
				} `xml:"Concept"`
			} `xml:"ConceptScheme"`
		} `xml:"Concepts"`
		DataStructures struct {
			Text          string `xml:",chardata"`
			DataStructure struct {
				Text                string `xml:",chardata"`
				URN                 string `xml:"urn,attr"`
				IsExternalReference string `xml:"isExternalReference,attr"`
				AgencyID            string `xml:"agencyID,attr"`
				ID                  string `xml:"id,attr"`
				IsFinal             string `xml:"isFinal,attr"`
				URI                 string `xml:"uri,attr"`
				Version             string `xml:"version,attr"`
				Name                struct {
					Text string `xml:",chardata"`
					Lang string `xml:"lang,attr"`
				} `xml:"Name"`
				DataStructureComponents struct {
					Text          string `xml:",chardata"`
					DimensionList struct {
						Text      string `xml:",chardata"`
						URN       string `xml:"urn,attr"`
						ID        string `xml:"id,attr"`
						Dimension []struct {
							Text            string `xml:",chardata"`
							URN             string `xml:"urn,attr"`
							ID              string `xml:"id,attr"`
							Position        string `xml:"position,attr"`
							ConceptIdentity struct {
								Text string `xml:",chardata"`
								Ref  struct {
									Text                      string `xml:",chardata"`
									MaintainableParentID      string `xml:"maintainableParentID,attr"`
									Package                   string `xml:"package,attr"`
									MaintainableParentVersion string `xml:"maintainableParentVersion,attr"`
									AgencyID                  string `xml:"agencyID,attr"`
									ID                        string `xml:"id,attr"`
									Class                     string `xml:"class,attr"`
								} `xml:"Ref"`
							} `xml:"ConceptIdentity"`
							LocalRepresentation struct {
								Text        string `xml:",chardata"`
								Enumeration struct {
									Text string `xml:",chardata"`
									Ref  struct {
										Text     string `xml:",chardata"`
										Package  string `xml:"package,attr"`
										AgencyID string `xml:"agencyID,attr"`
										ID       string `xml:"id,attr"`
										Version  string `xml:"version,attr"`
										Class    string `xml:"class,attr"`
									} `xml:"Ref"`
								} `xml:"Enumeration"`
								EnumerationFormat struct {
									Text      string `xml:",chardata"`
									MinLength string `xml:"minLength,attr"`
									TextType  string `xml:"textType,attr"`
									MaxLength string `xml:"maxLength,attr"`
								} `xml:"EnumerationFormat"`
							} `xml:"LocalRepresentation"`
						} `xml:"Dimension"`
						TimeDimension struct {
							Text            string `xml:",chardata"`
							URN             string `xml:"urn,attr"`
							ID              string `xml:"id,attr"`
							Position        string `xml:"position,attr"`
							ConceptIdentity struct {
								Text string `xml:",chardata"`
								Ref  struct {
									Text                      string `xml:",chardata"`
									MaintainableParentID      string `xml:"maintainableParentID,attr"`
									Package                   string `xml:"package,attr"`
									MaintainableParentVersion string `xml:"maintainableParentVersion,attr"`
									AgencyID                  string `xml:"agencyID,attr"`
									ID                        string `xml:"id,attr"`
									Class                     string `xml:"class,attr"`
								} `xml:"Ref"`
							} `xml:"ConceptIdentity"`
							LocalRepresentation struct {
								Text       string `xml:",chardata"`
								TextFormat struct {
									Text     string `xml:",chardata"`
									TextType string `xml:"textType,attr"`
								} `xml:"TextFormat"`
							} `xml:"LocalRepresentation"`
						} `xml:"TimeDimension"`
					} `xml:"DimensionList"`
					Group struct {
						Text           string `xml:",chardata"`
						URN            string `xml:"urn,attr"`
						ID             string `xml:"id,attr"`
						GroupDimension []struct {
							Text               string `xml:",chardata"`
							DimensionReference struct {
								Text string `xml:",chardata"`
								Ref  struct {
									Text string `xml:",chardata"`
									ID   string `xml:"id,attr"`
								} `xml:"Ref"`
							} `xml:"DimensionReference"`
						} `xml:"GroupDimension"`
					} `xml:"Group"`
					AttributeList struct {
						Text      string `xml:",chardata"`
						URN       string `xml:"urn,attr"`
						ID        string `xml:"id,attr"`
						Attribute []struct {
							Text             string `xml:",chardata"`
							URN              string `xml:"urn,attr"`
							AssignmentStatus string `xml:"assignmentStatus,attr"`
							ID               string `xml:"id,attr"`
							ConceptIdentity  struct {
								Text string `xml:",chardata"`
								Ref  struct {
									Text                      string `xml:",chardata"`
									MaintainableParentID      string `xml:"maintainableParentID,attr"`
									Package                   string `xml:"package,attr"`
									MaintainableParentVersion string `xml:"maintainableParentVersion,attr"`
									AgencyID                  string `xml:"agencyID,attr"`
									ID                        string `xml:"id,attr"`
									Class                     string `xml:"class,attr"`
								} `xml:"Ref"`
							} `xml:"ConceptIdentity"`
							LocalRepresentation struct {
								Text       string `xml:",chardata"`
								TextFormat struct {
									Text      string `xml:",chardata"`
									TextType  string `xml:"textType,attr"`
									MaxLength string `xml:"maxLength,attr"`
									MinLength string `xml:"minLength,attr"`
								} `xml:"TextFormat"`
								Enumeration struct {
									Text string `xml:",chardata"`
									Ref  struct {
										Text     string `xml:",chardata"`
										Package  string `xml:"package,attr"`
										AgencyID string `xml:"agencyID,attr"`
										ID       string `xml:"id,attr"`
										Version  string `xml:"version,attr"`
										Class    string `xml:"class,attr"`
									} `xml:"Ref"`
								} `xml:"Enumeration"`
								EnumerationFormat struct {
									Text      string `xml:",chardata"`
									MinLength string `xml:"minLength,attr"`
									TextType  string `xml:"textType,attr"`
									MaxLength string `xml:"maxLength,attr"`
								} `xml:"EnumerationFormat"`
							} `xml:"LocalRepresentation"`
							AttributeRelationship struct {
								Text      string `xml:",chardata"`
								Dimension []struct {
									Text string `xml:",chardata"`
									Ref  struct {
										Text string `xml:",chardata"`
										ID   string `xml:"id,attr"`
									} `xml:"Ref"`
								} `xml:"Dimension"`
								PrimaryMeasure struct {
									Text string `xml:",chardata"`
									Ref  struct {
										Text string `xml:",chardata"`
										ID   string `xml:"id,attr"`
									} `xml:"Ref"`
								} `xml:"PrimaryMeasure"`
							} `xml:"AttributeRelationship"`
						} `xml:"Attribute"`
					} `xml:"AttributeList"`
					MeasureList struct {
						Text           string `xml:",chardata"`
						URN            string `xml:"urn,attr"`
						ID             string `xml:"id,attr"`
						PrimaryMeasure struct {
							Text            string `xml:",chardata"`
							URN             string `xml:"urn,attr"`
							ID              string `xml:"id,attr"`
							ConceptIdentity struct {
								Text string `xml:",chardata"`
								Ref  struct {
									Text                      string `xml:",chardata"`
									MaintainableParentID      string `xml:"maintainableParentID,attr"`
									Package                   string `xml:"package,attr"`
									MaintainableParentVersion string `xml:"maintainableParentVersion,attr"`
									AgencyID                  string `xml:"agencyID,attr"`
									ID                        string `xml:"id,attr"`
									Class                     string `xml:"class,attr"`
								} `xml:"Ref"`
							} `xml:"ConceptIdentity"`
							LocalRepresentation struct {
								Text       string `xml:",chardata"`
								TextFormat struct {
									Text       string `xml:",chardata"`
									IsSequence string `xml:"isSequence,attr"`
									TextType   string `xml:"textType,attr"`
									MaxLength  string `xml:"maxLength,attr"`
								} `xml:"TextFormat"`
							} `xml:"LocalRepresentation"`
						} `xml:"PrimaryMeasure"`
					} `xml:"MeasureList"`
				} `xml:"DataStructureComponents"`
			} `xml:"DataStructure"`
		} `xml:"DataStructures"`
	} `xml:"Structures"`
}

func (c Client) GetCurrencies() (items []ecbcurrency.Input, err error) {

	apiItems, err := c.GetApiCurrencies()
	if err != nil {
		return nil, fmt.Errorf("c.GetApiCurrencies failed: %w", err)
	}

	for _, apiItem := range apiItems {
		items = append(items, apiCurrencyToItem(apiItem))
	}

	return items, nil
}

func (c Client) GetCurrenciesMap() (itemsMap map[string]ecbcurrency.Model, err error) {

	items, err := c.GetCurrencies()
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
