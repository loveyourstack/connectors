package awssvc

import (
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/loveyourstack/connectors/aws/awsapi"
	"github.com/loveyourstack/connectors/aws/stores/awsusersgrule"
)

type Service struct {
	client *awsapi.Client

	userSgRuleStore awsusersgrule.Store

	infoLog  *slog.Logger
	errorLog *slog.Logger
}

func NewService(db *pgxpool.Pool, client *awsapi.Client, infoLog, errorLog *slog.Logger) (svc Service) {

	svcShortname := "aws"

	return Service{
		client: client,

		userSgRuleStore: awsusersgrule.Store{Db: db},

		infoLog:  infoLog.With("svc", svcShortname),
		errorLog: errorLog.With("svc", svcShortname),
	}
}
