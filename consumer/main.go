package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/joho/godotenv"
	"log"
	"os"
)

func main() {
	ExecuteCopy()
}

func readAndCopyKeys(cfg *aws.Config) {
	SQS := sqs.NewFromConfig(*cfg)
	S3 := s3.NewFromConfig(*cfg)

	destinationBucketName := os.Getenv("DESTINATION_BUCKET_NAME")
	sourceBucketName := os.Getenv("SOURCE_BUCKET_NAME")
	delimiter := os.Getenv("DELIMITER")
	queueUrl := os.Getenv("QUEUE_URL")

	ch := make(chan int, 2)

	// 2 threads
	for {
		ch <- 1
		go func() {
			messages, err := SQS.ReceiveMessage(context.TODO(),
				&sqs.ReceiveMessageInput{
					QueueUrl:            aws.String(queueUrl),
					MaxNumberOfMessages: 10,
					AttributeNames:      []types.QueueAttributeName{"All"}})
			if err != nil {
				return
			}

			var successfullyCopiedEntries []types.DeleteMessageBatchRequestEntry
			for _, message := range messages.Messages {
				_, err := S3.CopyObject(context.TODO(),
					&s3.CopyObjectInput{
						Bucket:     aws.String(destinationBucketName),
						Key:        message.Body,
						CopySource: aws.String(fmt.Sprintf("%s%s%s", sourceBucketName, delimiter, *message.Body)),
					})
				if err == nil {
					successfullyCopiedEntries = append(successfullyCopiedEntries,
						types.DeleteMessageBatchRequestEntry{Id: message.MessageId, ReceiptHandle: message.ReceiptHandle})
				}

				// todo: use gorm to update DB
			}

			// Delete successfullyCopiedKeys from Queue. The ones that failed to be copied will remain in the queue and
			// will be processed again
			if len(successfullyCopiedEntries) > 0 {
				_, err = SQS.DeleteMessageBatch(context.TODO(),
					&sqs.DeleteMessageBatchInput{QueueUrl: aws.String(queueUrl), Entries: successfullyCopiedEntries})
				if err != nil {
					// todo: handle error
					fmt.Println(err)
				}
			}
			<-ch
		}()
	}
}

func ExecuteCopy() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Some error occured while loading .env file. Err: %s", err)
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-east-1"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	readAndCopyKeys(&cfg)
}
