package main

import (
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Map returns a new slice containing the results of applying the function f to each string in the original slice.

func S3ObjectToKeyMap(vs []types.Object, f func(types.Object) string) []string {
	vsm := make([]string, len(vs))
	for i, v := range vs {
		vsm[i] = f(v)
	}
	return vsm
}
