package mmapi

import "fmt"

type GeoLite2Db string

const (
	GeoLite2Asn     GeoLite2Db = "GeoLite2-ASN"
	GeoLite2City    GeoLite2Db = "GeoLite2-City"
	GeoLite2Country GeoLite2Db = "GeoLite2-Country"
)

var AllGeoDbs = []GeoLite2Db{GeoLite2Asn, GeoLite2City, GeoLite2Country}

func ParseGeoDb(s string) (GeoLite2Db, error) {
	for _, geoDb := range AllGeoDbs {
		if string(geoDb) == s {
			return geoDb, nil
		}
	}
	return "", fmt.Errorf("invalid GeoLite2Db: %s", s)
}
