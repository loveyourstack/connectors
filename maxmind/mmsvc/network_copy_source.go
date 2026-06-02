package mmsvc

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
)

// https://pkg.go.dev/github.com/jackc/pgx/v5#CopyFromSource

type networkCopySource struct {
	csvReader *csv.Reader
	colIdxMap map[string]int

	rowNum       int
	skippedCount int
	currVals     []any
	err          error
}

func newNetworkCopySource(csvReader *csv.Reader, colIdxMap map[string]int) *networkCopySource {
	return &networkCopySource{
		csvReader: csvReader,
		colIdxMap: colIdxMap,
	}
}

func (s *networkCopySource) Next() bool {

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

		// skip empty geoname_id
		if cols[s.colIdxMap["geoname_id"]] == "" {
			s.skippedCount++
			continue
		}

		// parse geoname_id to int64
		geoNameId, err := strconv.ParseInt(cols[s.colIdxMap["geoname_id"]], 10, 64)
		if err != nil {
			s.err = fmt.Errorf("strconv.ParseInt failed for geoname_id in data row %d: %w", s.rowNum, err)
			return false
		}

		s.currVals = []any{
			cols[s.colIdxMap["network"]],
			geoNameId,
		}
		return true
	}
}

func (s *networkCopySource) Values() ([]any, error) {
	return s.currVals, nil
}

func (s *networkCopySource) Err() error {
	return s.err
}
