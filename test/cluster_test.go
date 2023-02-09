// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package test

import (
	"context"
	"os"
	"testing"

	"github.com/CeresDB/ceresdb-client-go/ceresdb"
	"github.com/CeresDB/ceresdb-client-go/types"
	"github.com/CeresDB/ceresdb-client-go/utils"
	"github.com/stretchr/testify/require"
)

var clusterEndpoint = "127.0.0.1:8831"

func init() {
	if v := os.Getenv("CERESDB_CLUSTER_ADDR"); v != "" {
		clusterEndpoint = v
	}
}

func TestClusterMultiWriteAndQuery(t *testing.T) {
	t.Skip("ignore local test")

	client, err := ceresdb.NewClient(clusterEndpoint, types.Direct,
		ceresdb.EnableLoggerDebug(true),
	)
	require.NoError(t, err, "init ceresdb client failed")

	timestamp := utils.CurrentMS()

	table1Points, err := buildTablePoints("ceresdb_route_test1", timestamp, 2)
	require.NoError(t, err, "build table1 points failed")

	table2Points, err := buildTablePoints("ceresdb_route_test2", timestamp, 3)
	require.NoError(t, err, "build table2 points failed")

	table1Points = append(table1Points, table2Points...)

	req := types.WriteRequest{
		Points: table1Points,
	}
	resp, err := client.Write(context.Background(), req)
	require.NoError(t, err, "write rows failed")

	require.Equal(t, resp.Success, uint32(5), "write success value is not expected")

	testBaseQuery(t, client, "ceresdb_route_test1", timestamp, 2)
	testBaseQuery(t, client, "ceresdb_route_test2", timestamp, 3)
	t.Log("multi table write is paas")
}
