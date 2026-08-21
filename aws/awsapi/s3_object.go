package awsapi

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/loveyourstack/connectors/aws/stores/awsapicall"
)

func (c *Client) PutS3Object(ctx context.Context, bucket, key string, body io.Reader, contentType string) (err error) {

	// make S3 client if needed
	err = c.makeS3Client(ctx)
	if err != nil {
		return fmt.Errorf("c.makeS3Client failed: %w", err)
	}

	// prepare call log input
	callInput := awsapicall.Input{
		Attempt:    1,
		DurationMs: 0, // set in defer
		Endpoint:   "s3Client.PutObject",
		Page:       1,
		Result:     "", // set below
	}

	start := time.Now()

	defer func() {
		callInput.DurationMs = time.Since(start).Milliseconds()

		_, err := c.callStore.Insert(context.Background(), callInput)
		if err != nil {
			c.logger.Error("c.callStore.Insert failed", "error", err, "callInput", callInput)
		}
	}()

	// call S3 PutObject
	_, err = c.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		Body:        body,
		ContentType: &contentType,
	})
	if err != nil {
		callInput.Result = err.Error()
		return fmt.Errorf("c.s3Client.PutObject failed: %w", err)
	}

	callInput.Result = "OK"
	return nil
}
