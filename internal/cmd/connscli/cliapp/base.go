package cliapp

import (
	"github.com/loveyourstack/connectors/ecb/ecbapi"
	"github.com/loveyourstack/connectors/ecb/ecbsvc"
	"github.com/loveyourstack/connectors/internal/cmd"
	"github.com/loveyourstack/connectors/maxmind/mmapi"
	"github.com/loveyourstack/connectors/maxmind/mmsvc"
)

// App is a lyscli application
type App struct {
	*cmd.Application

	// clients
	EcbClient     ecbapi.Client
	MaxMindClient mmapi.Client

	// services
	EcbSvc     ecbsvc.Service
	MaxMindSvc mmsvc.Service
}
