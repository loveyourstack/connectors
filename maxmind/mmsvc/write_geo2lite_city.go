package mmsvc

import (
	"archive/zip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/loveyourstack/connectors/maxmind/mmapi"
	"github.com/loveyourstack/connectors/maxmind/stores/mmgeodbmeta"
	"github.com/loveyourstack/connectors/maxmind/stores/mmlocation"
	"github.com/loveyourstack/connectors/maxmind/stores/mmnetwork"
	"github.com/loveyourstack/lys/lystype"
)

func (svc Service) WriteGeo2LiteCity(ctx context.Context, db *pgxpool.Pool, getNewZip bool) (err error) {

	zipFilename := fmt.Sprintf("%s.zip", mmapi.GeoLite2City)
	zipPath := filepath.Join(svc.DownloadsPath, zipFilename)

	// get/update GeoLite2 City zip from API if requested
	// otherwise assume zip is already in downloads path
	if getNewZip {
		svc.InfoLog.Info("fetching new GeoLite2 City zip from MaxMind API")

		zipBytes, err := svc.Client.GetApiGeoLite2DbZip(ctx, mmapi.GeoLite2City)
		if err != nil {
			return fmt.Errorf("svc.Client.GetApiGeoLite2DbZip failed: %w", err)
		}

		// write to downloads path
		err = os.WriteFile(zipPath, zipBytes, 0644)
		if err != nil {
			return fmt.Errorf("os.WriteFile failed: %w", err)
		}
	}

	// open zip file
	zipReader, err := zip.OpenReader(zipPath)
	if err != nil {
		return fmt.Errorf("zip.OpenReader failed: %w", err)
	}
	defer zipReader.Close()

	// stream ipv4 and ipv6 blocks to db
	networkStore := mmnetwork.Store{Db: db}
	err = svc.streamGeo2LiteCityBlocks(ctx, db, networkStore, zipReader)
	if err != nil {
		return fmt.Errorf("streamGeo2LiteCityBlocks failed: %w", err)
	}

	// analyze table to update stats
	err = networkStore.Analyze(ctx)
	if err != nil {
		return fmt.Errorf("networkStore.Analyze failed: %w", err)
	}

	// stream locations_en to db
	locStore := mmlocation.Store{Db: db}
	err = svc.streamGeo2LiteCityLocationsEn(ctx, db, locStore, zipReader)
	if err != nil {
		return fmt.Errorf("streamGeo2LiteCityLocationsEn failed: %w", err)
	}

	// analyze table to update stats
	err = locStore.Analyze(ctx)
	if err != nil {
		return fmt.Errorf("locStore.Analyze failed: %w", err)
	}

	// upsert geo db meta with last write time
	metaStore := mmgeodbmeta.Store{Db: db}
	input := mmgeodbmeta.Input{
		GeoDB:       string(mmapi.GeoLite2City),
		LastWriteAt: lystype.Datetime(time.Now()),
	}
	err = metaStore.Upsert(ctx, input)
	if err != nil {
		return fmt.Errorf("metaStore.Upsert failed: %w", err)
	}

	return nil
}

// assignMandatoryColIndexes assigns column indexes to the provided colIdxMap based on the headerCols slice.
func assignMandatoryColIndexes(headerCols []string, colIdxMap map[string]int) error {

	for idx, col := range headerCols {
		if _, ok := colIdxMap[col]; ok {
			colIdxMap[col] = idx
		}
	}

	for col, idx := range colIdxMap {
		if idx == -1 {
			return fmt.Errorf("column %s not found in csv file", col)
		}
	}

	return nil
}

// getFileFromZip returns a zip.File from the provided zip reader whose suffix matches the provided file name.
func getFileFromZip(zipReader *zip.ReadCloser, fileName string) (zipFile *zip.File, err error) {

	for _, file := range zipReader.File {
		// use HasSuffix because the file name is prefixed with the containing folder name, e.g. GeoLite2-City-CSV_20260526/GeoLite2-City-Blocks-IPv6.csv
		if strings.HasSuffix(file.Name, fileName) {
			return file, nil
		}
	}

	return nil, fmt.Errorf("file %s not found in zip", fileName)
}

// openCsvReaderFromZip opens a csv file from the provided zip reader and returns a csv.Reader for that file, along with the opened file which should be closed by the caller.
func openCsvReaderFromZip(zipReader *zip.ReadCloser, csvFileName string) (csvReader *csv.Reader, csvFile io.ReadCloser, err error) {

	zipFile, err := getFileFromZip(zipReader, csvFileName)
	if err != nil {
		return nil, nil, fmt.Errorf("getFileFromZip (%s) failed: %w", csvFileName, err)
	}

	csvFile, err = zipFile.Open()
	if err != nil {
		return nil, nil, fmt.Errorf("zipFile.Open failed: %w", err)
	}

	csvReader = csv.NewReader(csvFile)
	csvReader.ReuseRecord = true

	return csvReader, csvFile, nil
}

// streamGeo2LiteCityBlocks streams the GeoLite2 City blocks data from the provided zip reader to the database using the provided network store.
// It streams both the IPv4 and IPv6 blocks files. The destination tables are truncated before streaming, and all operations are done within a transaction.
func (svc Service) streamGeo2LiteCityBlocks(ctx context.Context, db *pgxpool.Pool, networkStore mmnetwork.Store, zipReader *zip.ReadCloser) (err error) {

	// begin tx
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("db.Begin failed: %w", err)
	}
	defer tx.Rollback(ctx)

	// truncate table
	err = networkStore.TruncateTx(ctx, tx)
	if err != nil {
		return fmt.Errorf("networkStore.TruncateTx failed: %w", err)
	}

	// stream blocks ipv4 to db
	err = svc.streamGeo2LiteCityBlocksFileTx(ctx, tx, networkStore, zipReader, "GeoLite2-City-Blocks-IPv4.csv")
	if err != nil {
		return fmt.Errorf("streamGeo2LiteCityBlocksFileTx (IPv4) failed: %w", err)
	}

	// stream blocks ipv6 to db
	err = svc.streamGeo2LiteCityBlocksFileTx(ctx, tx, networkStore, zipReader, "GeoLite2-City-Blocks-IPv6.csv")
	if err != nil {
		return fmt.Errorf("streamGeo2LiteCityBlocksFileTx (IPv6) failed: %w", err)
	}

	// commit tx
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("tx.Commit failed: %w", err)
	}

	return nil
}

// streamGeo2LiteCityBlocksFileTx streams a GeoLite2 City blocks csv file from the provided zip reader to the database using the provided network store and transaction.
func (svc Service) streamGeo2LiteCityBlocksFileTx(ctx context.Context, tx pgx.Tx, networkStore mmnetwork.Store, zipReader *zip.ReadCloser, csvFileName string) (err error) {

	svc.InfoLog.Debug("streaming GeoLite2 City blocks to database", slog.String("csvFileName", csvFileName))

	// open csv reader for csv file in zip
	csvReader, csvFile, err := openCsvReaderFromZip(zipReader, csvFileName)
	if err != nil {
		return fmt.Errorf("openCsvReaderFromZip failed: %w", err)
	}
	defer csvFile.Close()

	// read header row
	headerCols, err := csvReader.Read()
	if err != nil {
		if err == io.EOF {
			return fmt.Errorf("file is empty")
		}
		return fmt.Errorf("csvReader.Read (header) failed: %w", err)
	}

	// define mandatory columns
	colIdxMap := make(map[string]int) // k = column name, v = column index in csv file
	colIdxMap["network"] = -1
	colIdxMap["geoname_id"] = -1

	// assign mandatory column indexes based on header row.
	// doing it like this so that the order of columns in the csv file doesn't matter, as long as the header names are correct
	err = assignMandatoryColIndexes(headerCols, colIdxMap)
	if err != nil {
		return fmt.Errorf("assignMandatoryColIndexes failed: %w", err)
	}

	// define pgx CopyFromSource
	copySource := newNetworkCopySource(csvReader, colIdxMap)

	// stream rows to db
	colNames := []string{"network", "geoname_id"}
	rowsAffected, err := networkStore.BulkInsertSourceTx(ctx, tx, colNames, copySource)
	if err != nil {
		return fmt.Errorf("networkStore.BulkInsertSourceTx failed: %w", err)
	}

	svc.InfoLog.Debug("streamGeo2LiteCityBlocksFileTx: finished streaming csv rows", slog.String("fileName", csvFileName), slog.Int64("rowsInserted", rowsAffected), slog.Int("skippedCount", copySource.skippedCount))

	return nil
}

// streamGeo2LiteCityLocationsEn streams the GeoLite2 City locations_en data from the provided zip reader to the database using the provided location store.
// The destination table is truncated before streaming, and all operations are done within a transaction.
func (svc Service) streamGeo2LiteCityLocationsEn(ctx context.Context, db *pgxpool.Pool, locStore mmlocation.Store, zipReader *zip.ReadCloser) (err error) {

	svc.InfoLog.Debug("streaming GeoLite2 City locations_en to database")

	csvFileName := "GeoLite2-City-Locations-en.csv"

	// open csv reader for csv file in zip
	csvReader, csvFile, err := openCsvReaderFromZip(zipReader, csvFileName)
	if err != nil {
		return fmt.Errorf("openCsvReaderFromZip failed: %w", err)
	}
	defer csvFile.Close()

	// read header row
	headerCols, err := csvReader.Read()
	if err != nil {
		if err == io.EOF {
			return fmt.Errorf("file is empty")
		}
		return fmt.Errorf("csvReader.Read (header) failed: %w", err)
	}

	// define mandatory columns
	colIdxMap := make(map[string]int) // k = column name, v = column index in csv file
	colIdxMap["city_name"] = -1
	colIdxMap["country_iso_code"] = -1
	colIdxMap["country_name"] = -1
	colIdxMap["geoname_id"] = -1

	// assign mandatory column indexes based on header row.
	// doing it like this so that the order of columns in the csv file doesn't matter, as long as the header names are correct
	err = assignMandatoryColIndexes(headerCols, colIdxMap)
	if err != nil {
		return fmt.Errorf("assignMandatoryColIndexes failed: %w", err)
	}

	// begin tx
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("db.Begin failed: %w", err)
	}
	defer tx.Rollback(ctx)

	// truncate table
	err = locStore.TruncateTx(ctx, tx)
	if err != nil {
		return fmt.Errorf("locStore.TruncateTx failed: %w", err)
	}

	// define pgx CopyFromSource
	copySource := newLocationCopySource(csvReader, colIdxMap)

	// stream rows to db
	colNames := []string{"geoname_id", "country_iso_code", "country_name", "city_name"}
	rowsAffected, err := locStore.BulkInsertSourceTx(ctx, tx, colNames, copySource)
	if err != nil {
		return fmt.Errorf("locStore.BulkInsertSourceTx failed: %w", err)
	}

	svc.InfoLog.Debug("streamGeo2LiteCityLocationsEn: finished streaming csv rows", slog.Int64("rowsInserted", rowsAffected), slog.Int("skippedCount", copySource.skippedCount))

	// commit tx
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("tx.Commit failed: %w", err)
	}

	return nil
}
