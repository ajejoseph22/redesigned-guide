package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	types2 "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"log"
	"strconv"
	"sync"
)

const QueueUrl = "https://sqs.us-east-1.amazonaws.com/514260427086/standard_queue"

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

func downloadKeysToQueue(cfg *aws.Config) error {
	S3 := s3.NewFromConfig(*cfg)
	SQS := sqs.NewFromConfig(*cfg)

	getObjectKeys := func(nextToken string) *s3.ListObjectsV2Output {
		props := &s3.ListObjectsV2Input{
			Bucket:    aws.String("sketch123456765-legacy-s3"),
			Prefix:    aws.String("image/"),
			Delimiter: aws.String("/")}
		if nextToken != "" {
			props.ContinuationToken = aws.String(nextToken)
		}

		resp, err := S3.ListObjectsV2(context.TODO(), props)
		if err != nil {
			// todo: handle error
			fmt.Println(err)
		}

		resultIterable := s3ObjectToKeyMap(resp.Contents, func(v types.Object) string {
			return *v.Key
		})
		resultLength := len(resultIterable)

		var wg sync.WaitGroup
		wg.Add(resultLength)

		for i := 0; i < len(resultIterable); i += 10 {
			go func(i int) {
				defer wg.Done()
				var entries []types2.SendMessageBatchRequestEntry

				for j := i; j < (i + 10); j++ {
					element := resultIterable[j]
					entries = append(entries,
						types2.SendMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(j)), MessageBody: aws.String(element)})
				}

				_, err := SQS.SendMessageBatch(context.TODO(), &sqs.SendMessageBatchInput{
					QueueUrl: aws.String(QueueUrl),
					Entries:  entries,
				})
				if err != nil {
					// todo: handle error
					fmt.Println(err)
				}

			}(i)
		}

		wg.Wait()

		return resp
	}

	response := getObjectKeys("")

	for response.ContinuationToken != nil {
		response = getObjectKeys(*response.ContinuationToken)
	}

	return nil
}

func ExecuteFeeder() {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-east-1"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	err = downloadKeysToQueue(&cfg)
	if err != nil {
		panic(err)
	}
}
