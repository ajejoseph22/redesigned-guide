package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/joho/godotenv"
	"log"
	"math"
	"os"
	"strconv"
	"sync"
)

// Map through list of objects and return a list of the object keys
func s3ObjectToKeyMap(vs []types.Object, f func(types.Object) string) []string {
	vsm := make([]string, len(vs))
	for i, v := range vs {
		vsm[i] = f(v)
	}
	return vsm
}

func main() {
	ExecuteFeeder()
}

// Download the keys from the bucket and send them to an SQS queue
func downloadKeysToQueue(cfg *aws.Config) error {
	S3 := s3.NewFromConfig(*cfg)
	SQS := sqs.NewFromConfig(*cfg)

	bucketName, _ := os.LookupEnv("SOURCE_BUCKET_NAME")
	objectPrefix, _ := os.LookupEnv("OBJECT_PREFIX")
	delimiter, _ := os.LookupEnv("DELIMITER")
	queueUrl, _ := os.LookupEnv("QUEUE_URL")

	getAndLoadObjectKeys := func(nextToken string) *s3.ListObjectsV2Output {
		props := &s3.ListObjectsV2Input{
			Bucket:    aws.String(bucketName),
			Prefix:    aws.String(objectPrefix),
			Delimiter: aws.String(delimiter)}
		if nextToken != "" {
			props.ContinuationToken = aws.String(nextToken)
		}

		// Get the objects from the bucket
		resp, err := S3.ListObjectsV2(context.TODO(), props)
		if err != nil {
			// todo: log to some external service (DataDog)
			fmt.Println(err)
		}

		// Transform the list of objects to list of keys only
		resultIterable := s3ObjectToKeyMap(resp.Contents, func(v types.Object) string {
			return *v.Key
		})
		resultLength := len(resultIterable)
		var deltas = int(math.Ceil(float64(resultLength / 10)))

		var wg sync.WaitGroup
		wg.Add(deltas)

		// Loop through the keys in batches of 10s (queue batch maximum)
		for i := 0; i < len(resultIterable); i += 10 {
			// Run the loading of messages in a different thread/goroutine
			go func(i int) {
				defer wg.Done()
				var entries []sqsTypes.SendMessageBatchRequestEntry

				var nextLen int
				if resultLength-i >= 10 {
					nextLen = 10
				} else {
					nextLen = resultLength - i
				}

				// For each group of 10 (or less) keys, generate the SQS entries
				for j := i; j < (i + nextLen); j++ {
					key := resultIterable[j]
					entries = append(entries,
						sqsTypes.SendMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(j)), MessageBody: aws.String(key)})
				}

				// Batch send the keys to SQS
				_, err := SQS.SendMessageBatch(context.TODO(), &sqs.SendMessageBatchInput{
					QueueUrl: aws.String(queueUrl),
					Entries:  entries,
				})
				if err != nil {
					// todo: log to some external service (DataDog)
					fmt.Println(err)
				}
			}(i)
		}

		wg.Wait()

		return resp
	}

	response := getAndLoadObjectKeys("")

	// Recursively fetch (and) new keys as long as a ContinuationToken is returned
	for response.ContinuationToken != nil {
		response = getAndLoadObjectKeys(*response.ContinuationToken)
	}

	return nil
}

func ExecuteFeeder() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Some error occured while loading .env file. Err: %s", err)
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(os.Getenv("AWS_DEFAULT_REGION")))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	err = downloadKeysToQueue(&cfg)
	if err != nil {
		panic(err)
	}
}
