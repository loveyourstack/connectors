package awssvc

import (
	"log"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/loveyourstack/connectors/aws/awsapi"
	"github.com/loveyourstack/connectors/aws/stores/awsusersgrule"
)

type Service struct {
	client *awsapi.Client

	userSgRuleStore awsusersgrule.Store

	logger *slog.Logger
}

// NewService creates a new AWS service.
func NewService(db *pgxpool.Pool, client *awsapi.Client, logger *slog.Logger) (svc Service) {

	if client == nil {
		log.Fatal("awssvc: client is required")
	}

	svcShortname := "aws"

	return Service{
		client: client,

		userSgRuleStore: awsusersgrule.Store{Db: db},

		logger: logger.With("svc", svcShortname),
	}
}
