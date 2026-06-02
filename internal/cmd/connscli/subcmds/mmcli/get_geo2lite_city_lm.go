package mmcli

import (
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/loveyourstack/connectors/internal/cmd/connscli/cliapp"
	"github.com/loveyourstack/connectors/maxmind/mmapi"
	"github.com/loveyourstack/connectors/maxmind/stores/mmgeodbmeta"
	"github.com/spf13/cobra"
)

func GetGeo2LiteCityLmCmd(cliApp *cliapp.App) *cobra.Command {
	return &cobra.Command{
		Use:  "getGeo2LiteCityLm",
		Long: "Gets the last modified time of the GeoLite2 City data and compares it to the last write time in the database.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			ctx := cmd.Context()

			defer cliApp.Db.Close()

			// try to get last write time from db
			lastWriteAt := time.Time{}
			metaStore := mmgeodbmeta.Store{Db: cliApp.Db}
			meta, err := metaStore.SelectByGeoDb(ctx, string(mmapi.GeoLite2City))
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					// continue with zero value
				} else {
					return fmt.Errorf("metaStore.SelectByGeoDb failed: %w", err)
				}
			} else {
				lastWriteAt = meta.LastWriteAt.ToTime()
			}

			fmt.Printf("Last write time in DB: %s\n", lastWriteAt)

			// get last modified time from API
			lastModified, err := cliApp.MaxMindClient.GetApiGeoLite2DbLastModified(ctx, mmapi.GeoLite2City)
			if err != nil {
				return fmt.Errorf("cliApp.MaxMindClient.GetApiGeoLite2DbLastModified failed: %w", err)
			}

			fmt.Printf("Last modified time from API: %s\n", lastModified)

			if lastModified.After(lastWriteAt) {
				fmt.Println("A new version of the GeoLite2 City data is available.")
			} else {
				fmt.Println("The GeoLite2 City data in the DB is up to date.")
			}

			return nil
		},
	}
}
