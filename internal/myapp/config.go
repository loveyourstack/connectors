package myapp

import (
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/loveyourstack/connectors/aws/awsapi"
	"github.com/loveyourstack/connectors/maxmind/mmapi"
	"github.com/loveyourstack/lys/lyspgdb"
)

// general contains the general application config
type general struct {
	AppName       string
	DownloadsPath string
}

// Config contains all configuration settings
type Config struct {
	General     general
	Db          lyspgdb.Database `toml:"database"`
	DbSuperUser lyspgdb.User
	DbOwnerUser lyspgdb.User

	Aws     awsapi.Conf
	MaxMind mmapi.Conf
}

func (c *Config) LoadFromFile(configFilePath string) (err error) {

	// ensure supplied path exists
	if _, err := os.Stat(configFilePath); os.IsNotExist(err) {
		return fmt.Errorf("configFilePath does not exist: %s", configFilePath)
	} else if err != nil {
		return fmt.Errorf("os.Stat failed: %w", err)
	}

	// read conf from toml file
	if _, err := toml.DecodeFile(configFilePath, c); err != nil {
		return fmt.Errorf("toml.DecodeFile failed: %w", err)
	}

	return nil
}
