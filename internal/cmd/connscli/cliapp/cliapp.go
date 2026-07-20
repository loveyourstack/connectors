package cliapp

import (
	"github.com/loveyourstack/connectors/aws/awsapi"
	"github.com/loveyourstack/connectors/aws/awssvc"
	"github.com/loveyourstack/connectors/ecb/ecbapi"
	"github.com/loveyourstack/connectors/ecb/ecbsvc"
	"github.com/loveyourstack/connectors/internal/cmd"
	"github.com/loveyourstack/connectors/maxmind/mmapi"
	"github.com/loveyourstack/connectors/maxmind/mmsvc"
	"github.com/loveyourstack/connectors/tedb/tedbapi"
	"github.com/loveyourstack/connectors/tedb/tedbsvc"
)

// App is a connscli application
type App struct {
	*cmd.Application

	// clients
	AwsClient     *awsapi.Client
	EcbClient     ecbapi.Client
	MaxMindClient mmapi.Client
	TedbClient    tedbapi.Client

	// services
	AwsSvc     awssvc.Service
	EcbSvc     ecbsvc.Service
	MaxMindSvc mmsvc.Service
	TedbSvc    tedbsvc.Service
}
