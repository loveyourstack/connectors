package cliapp

import (
	"github.com/loveyourstack/connectors/ecb/ecbapi"
	"github.com/loveyourstack/connectors/ecb/ecbsvc"
	"github.com/loveyourstack/connectors/internal/cmd"
)

// App is a lyscli application
type App struct {
	*cmd.Application

	// clients
	EcbClient ecbapi.Client

	// services
	EcbSvc ecbsvc.Service
}
