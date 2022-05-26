package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"log"
	"os"
	"strings"
)

func main() {
	ExecuteFeeder()
}

func ExecuteFeeder() {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-west-2"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	S3 := s3.NewFromConfig(cfg)

	getObjectKeys := func(nextToken string) *s3.ListObjectsV2Output {
		resp, err := S3.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket:            aws.String("my-bucket"),
			ContinuationToken: aws.String(nextToken),
			Delimiter:         aws.String("/")})

		if err != nil {
			// todo: handle error
		}

		f, err := os.OpenFile("images.txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			panic(err)
		}

		defer func(f *os.File) {
			err := f.Close()
			if err != nil {
				panic(err)
			}
		}(f)

		resultIterable := S3ObjectToKeyMap(resp.Contents, func(v types.Object) string {
			return *v.Key
		})

		if _, err = f.WriteString(strings.Join(resultIterable, "\n")); err != nil {
			panic(err)
		}

		/*objects = append(objects, S3ObjectToKeyMap(resp.Contents, func(v types.Object) string {
			return *v.Key
		})...)*/

		return resp
	}

	response := getObjectKeys("")

	for response.ContinuationToken != nil {
		response = getObjectKeys(*response.ContinuationToken)
	}

}
