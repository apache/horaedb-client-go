// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/CeresDB/ceresdb-client-go/ceresdb"
	"github.com/CeresDB/ceresdb-client-go/types"
	"github.com/CeresDB/ceresdb-client-go/utils"
	"github.com/stretchr/testify/require"
)

var endpoint = "127.0.0.1:8831"

func init() {
	if v := os.Getenv("CERESDB_ADDR"); v != "" {
		endpoint = v
	}
}

// nolint
func build2Rows(metric string, timestamp int64, count int) ([]*types.Row, error) {
	rows := make([]*types.Row, 0, count)

	builder := ceresdb.NewRowBuilder(metric)

	idx := 0
	for ; idx < count; idx++ {
		row, err := builder.Reset().
			SetTimestamp(timestamp).
			AddTag("tagA", fmt.Sprintf("tagA:%s:%d", metric, idx)).
			AddTag("tagB", fmt.Sprintf("tagB:%s:%d", metric, idx)).
			AddField("vbool", true).
			AddField("vstring", fmt.Sprintf("row%d", idx)).
			AddField("vfloat64", float64(0.64)).
			AddField("vfloat32", float32(0.32)).
			AddField("vint", int(-1)).
			AddField("vint64", int64(-64)).
			AddField("vint32", int32(-32)).
			AddField("vint16", int16(-16)).
			AddField("vint8", int8(-8)).
			AddField("vuint", uint(1)).
			AddField("vuint64", uint64(64)).
			AddField("vuint32", uint32(32)).
			AddField("vuint16", uint16(16)).
			AddField("vuint8", uint8(8)).
			Build()
		if err != nil {
			return nil, err
		}

		rows = append(rows, row)
	}

	return rows, nil
}

func TestBaseWriteAndQuery(t *testing.T) {
	t.Skip("ignore local test")

	client, err := ceresdb.NewClient(endpoint)
	require.NoError(t, err, "init ceresdb client failed")
	timestamp := utils.CurrentMS()

	testBaseWrite(t, client, "ceresdb_test", timestamp, 2)
	testBaseQuery(t, client, "ceresdb_test", timestamp, 2)
}

// nolint
func testBaseWrite(t *testing.T, client ceresdb.Client, metric string, timestamp int64, count int) {
	rows, err := build2Rows(metric, timestamp, count)
	require.NoError(t, err, "build rows failed")
	require.Equal(t, len(rows), count, "build rows failed, not expected")

	resp, err := client.Write(context.Background(), rows)
	require.NoError(t, err, "write rows failed")

	require.Equal(t, resp.Success, uint32(count), "write success value is not expected")

	t.Log(metric + " base write is paas")
}

// nolint
func testBaseQuery(t *testing.T, client ceresdb.Client, metric string, timestamp int64, count int) {
	req := types.QueryRequest{
		Metrics: []string{metric},
		Ql:      fmt.Sprintf("select * from %s where timestamp = %d", metric, timestamp),
	}
	resp, err := client.Query(context.Background(), req)
	require.NoError(t, err, "query rows failed")

	require.Equal(t, resp.RowCount, uint32(count), "query rowCount value is not expected")

	records := resp.MapToRecord()
	require.Equal(t, len(records), count, "map to record size is not expected")

	r1 := records[0]

	ts, err := r1.GetTimestamp()
	require.NoError(t, err, "get timestamp fail")
	require.Equal(t, ts, timestamp, "timestamp int not expected")

	t1, err := r1.GetString("tagA")
	require.NoError(t, err, "get tag t1 fail")
	require.Contains(t, t1, fmt.Sprintf("tagA:%s", metric), "tag t1 is not expected")

	t2, err := r1.GetString("tagB")
	require.NoError(t, err, "get tag t2 fail")
	require.Contains(t, t2, fmt.Sprintf("tagB:%s", metric), "tag t2 is not expected")

	vBool, err := r1.GetBool("vbool")
	require.NoError(t, err, "get vbool fail")
	require.Equal(t, vBool, true, "vbool is not expected")

	vString, err := r1.GetString("vstring")
	require.NoError(t, err, "get vstring fail")
	require.Contains(t, vString, "row", "vstring is not expected")

	vFloat64, err := r1.GetFloat64("vfloat64")
	require.NoError(t, err, "get vfloat64 fail")
	require.Equal(t, vFloat64, float64(0.64), "vfloat64 is not expected")

	vFloat32, err := r1.GetFloat32("vfloat32")
	require.NoError(t, err, "get vfloat32 fail")
	require.Equal(t, vFloat32, float32(0.32), "vfloat32 is not expected")

	vInt, err := r1.GetInt("vint")
	require.NoError(t, err, "get vint fail")
	require.Equal(t, vInt, int(-1), "vint is not expected")

	vInt64, err := r1.GetInt64("vint64")
	require.NoError(t, err, "get vint64 fail")
	require.Equal(t, vInt64, int64(-64), "vint64 is not expected")

	vInt32, err := r1.GetInt32("vint32")
	require.NoError(t, err, "get vint32 fail")
	require.Equal(t, vInt32, int32(-32), "vint32 is not expected")

	vInt16, err := r1.GetInt16("vint16")
	require.NoError(t, err, "get vint16 fail")
	require.Equal(t, vInt16, int16(-16), "vint16 is not expected")

	vInt8, err := r1.GetInt8("vint8")
	require.NoError(t, err, "get vint8 fail")
	require.Equal(t, vInt8, int8(-8), "vint8 is not expected")

	vUInt, err := r1.GetUint("vuint")
	require.NoError(t, err, "get vuint fail")
	require.Equal(t, vUInt, uint(1), "vuint is not expected")

	vUInt64, err := r1.GetUInt64("vuint64")
	require.NoError(t, err, "get vuint64 fail")
	require.Equal(t, vUInt64, uint64(64), "vuint64 is not expected")

	vUInt32, err := r1.GetUInt32("vuint32")
	require.NoError(t, err, "get vuint32 fail")
	require.Equal(t, vUInt32, uint32(32), "vuint32 is not expected")

	vUInt16, err := r1.GetUInt16("vuint16")
	require.NoError(t, err, "get vuint16 fail")
	require.Equal(t, vUInt16, uint16(16), "vuint16 is not expected")

	vUInt8, err := r1.GetUInt8("vuint8")
	require.NoError(t, err, "get vuint32 fail")
	require.Equal(t, vUInt8, uint8(8), "vuint8 is not expected")

	t.Log(metric + " base query is paas")
}
