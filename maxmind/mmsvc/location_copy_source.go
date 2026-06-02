package mmsvc

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
)

// https://pkg.go.dev/github.com/jackc/pgx/v5#CopyFromSource

type locationCopySource struct {
	csvReader *csv.Reader
	colIdxMap map[string]int

	rowNum       int
	skippedCount int
	currVals     []any
	err          error
}

func newLocationCopySource(csvReader *csv.Reader, colIdxMap map[string]int) *locationCopySource {
	return &locationCopySource{
		csvReader: csvReader,
		colIdxMap: colIdxMap,
	}
}

func (s *locationCopySource) Next() bool {

	if s.err != nil {
		return false
	}

	for {
		cols, err := s.csvReader.Read()
		if err == io.EOF {
			return false
		}
		if err != nil {
			s.err = fmt.Errorf("csvReader.Read failed for data row %d: %w", s.rowNum+1, err)
			return false
		}

		s.rowNum++

		// skip where city, country iso and country are all unknown
		if cols[s.colIdxMap["city_name"]] == "" && cols[s.colIdxMap["country_iso_code"]] == "" && cols[s.colIdxMap["country_name"]] == "" {
			s.skippedCount++
			continue
		}

		// assign default values where needed
		if cols[s.colIdxMap["city_name"]] == "" {
			cols[s.colIdxMap["city_name"]] = "Unknown"
		}
		if cols[s.colIdxMap["country_iso_code"]] == "" {
			cols[s.colIdxMap["country_iso_code"]] = "ZZ"
		}
		if cols[s.colIdxMap["country_name"]] == "" {
			cols[s.colIdxMap["country_name"]] = "Unknown"
		}

		// parse geoname_id to int64
		geoNameId, err := strconv.ParseInt(cols[s.colIdxMap["geoname_id"]], 10, 64)
		if err != nil {
			s.err = fmt.Errorf("strconv.ParseInt failed for geoname_id in data row %d: %w", s.rowNum, err)
			return false
		}

		s.currVals = []any{
			geoNameId,
			cols[s.colIdxMap["country_iso_code"]],
			cols[s.colIdxMap["country_name"]],
			cols[s.colIdxMap["city_name"]],
		}
		return true
	}
}

func (s *locationCopySource) Values() ([]any, error) {
	return s.currVals, nil
}

func (s *locationCopySource) Err() error {
	return s.err
}
