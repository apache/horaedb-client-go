// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package test

import (
	"context"
	"os"
	"testing"

	"github.com/CeresDB/ceresdb-client-go/ceresdb"
	"github.com/CeresDB/ceresdb-client-go/utils"
	"github.com/stretchr/testify/require"
)

var clusterEndpoint = "127.0.0.1:8831"

func init() {
	if v := os.Getenv("CERESDB_CLUSTER_ADDR"); v != "" {
		clusterEndpoint = v
	}
}

func TestClusterMultiMetricWriteAndQuery(t *testing.T) {
	client, err := ceresdb.NewClient(clusterEndpoint,
		ceresdb.EnableLoggerDebug(true),
	)
	require.NoError(t, err, "init ceresdb client failed")

	timestamp := utils.CurrentMS()

	metric1Rows, err := build2Rows("ceresdb_route_test1", timestamp, 2)
	require.NoError(t, err, "build metric1 rows failed")

	metric2Rows, err := build2Rows("ceresdb_route_test2", timestamp, 3)
	require.NoError(t, err, "build metric2 rows failed")

	rows := append(metric1Rows, metric2Rows...)

	resp, err := client.Write(context.Background(), rows)
	require.NoError(t, err, "write rows failed")

	require.Equal(t, resp.Success, uint32(5), "write success value is not expected")

	testBaseQuery(t, client, "ceresdb_route_test1", timestamp, 2)
	testBaseQuery(t, client, "ceresdb_route_test2", timestamp, 3)
	t.Log("multi metric write is paas")
}
