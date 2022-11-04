// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ceresdb_test

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	cc "github.com/CeresDB/ceresdb-client-go/ceresdb"
)

var GrpcAddr = "127.0.0.1:8831"

func init() {
	if v := os.Getenv("CERESDB_ADDR"); v != "" {
		GrpcAddr = v
	}
}

func now() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func TestWrite(t *testing.T) {
	client, err := cc.NewClient(GrpcAddr)
	if err != nil {
		log.Fatalf("init ceresdb grpc client failed, err:%v", err)
	}

	now := now()
	metric := "cpu"
	points := []cc.Point{
		{
			Metric: metric,
			Tags: map[string]string{
				"hostname": "host0",
				"region":   "eu-west-1",
			},
			Timestamp: now,
			Fields: map[string]float64{
				"usage_user":   58,
				"usage_system": 23,
			},
		},
		{
			Metric: metric,
			Tags: map[string]string{
				"hostname": "host1",
				"region":   "eu-west-1",
			},
			Timestamp: now,
			Fields: map[string]float64{
				"usage_user":   58,
				"usage_system": 23,
			},
		},
	}
	success, err := client.Write(context.TODO(), points)
	if err != nil {
		log.Fatalf("write point failed, err:%v", err)
	}

	if success != 2 {
		t.Errorf("Expect 2, got %d", success)
	}

	resp, err := client.Query(context.TODO(), "select * from cpu")
	if err != nil {
		log.Fatalf("query failed, err:%v", err)
	}

	t.Logf("query result: %s", resp)
}
