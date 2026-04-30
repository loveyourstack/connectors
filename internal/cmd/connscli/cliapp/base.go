package cliapp

import (
	"github.com/loveyourstack/connectors/apiclients/ecbapi"
	"github.com/loveyourstack/connectors/internal/cmd"
)

// App is a lyscli application
type App struct {
	*cmd.Application
	EcbClient ecbapi.Client
}
