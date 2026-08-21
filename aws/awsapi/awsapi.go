package awsapi

import (
	"context"
	"fmt"
	"log"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	awsCreds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/loveyourstack/connectors/aws/stores/awsapicall"
)

type Conf struct {
	AccessKeyId     string // IAM user access key ID
	Region          string // AWS region, e.g. "us-east-1"
	SecretAccessKey string // IAM user secret access key
}

type Client struct {
	callStore awsapicall.Store
	conf      Conf

	ec2Client *ec2.Client
	s3Client  *s3.Client

	logger *slog.Logger
}

// NewClient creates a new AWS API client.
func NewClient(conf Conf, db *pgxpool.Pool, logger *slog.Logger) *Client {

	if conf.AccessKeyId == "" {
		log.Fatal("awsapi client: conf.AccessKeyId is required")
	}
	if conf.Region == "" {
		log.Fatal("awsapi client: conf.Region is required")
	}
	if conf.SecretAccessKey == "" {
		log.Fatal("awsapi client: conf.SecretAccessKey is required")
	}

	apiShortname := "aws"

	// return pointer: client is modified by lazy initialization
	return &Client{
		conf:      conf,
		callStore: awsapicall.Store{Db: db},

		ec2Client: nil, // lazily initialized in makeEc2Client
		s3Client:  nil, // lazily initialized in makeS3Client

		logger: logger.With("api", apiShortname),
	}
}

// getConfig return AWS config using the provided credentials and region.
func (c *Client) getConfig(ctx context.Context) (cfg aws.Config, err error) {

	staticProvider := awsCreds.NewStaticCredentialsProvider(c.conf.AccessKeyId, c.conf.SecretAccessKey, "")
	cfg, err = awsCfg.LoadDefaultConfig(ctx, awsCfg.WithRegion(c.conf.Region), awsCfg.WithCredentialsProvider(staticProvider))
	if err != nil {
		return aws.Config{}, fmt.Errorf("awsCfg.LoadDefaultConfig failed: %w", err)
	}

	return cfg, nil
}

// makeEc2Client lazily initializes the EC2 client and reuses it for subsequent calls
func (c *Client) makeEc2Client(ctx context.Context) (err error) {

	if c.ec2Client != nil {
		return nil
	}

	cfg, err := c.getConfig(ctx)
	if err != nil {
		return fmt.Errorf("c.getConfig failed: %w", err)
	}

	c.ec2Client = ec2.NewFromConfig(cfg)
	return nil
}

// makeS3Client lazily initializes the S3 client and reuses it for subsequent calls
func (c *Client) makeS3Client(ctx context.Context) (err error) {

	if c.s3Client != nil {
		return nil
	}

	cfg, err := c.getConfig(ctx)
	if err != nil {
		return fmt.Errorf("c.getConfig failed: %w", err)
	}

	c.s3Client = s3.NewFromConfig(cfg)
	return nil
}
