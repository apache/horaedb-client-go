// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/CeresDB/ceresdb-client-go/ceresdb"
	"github.com/stretchr/testify/require"
)

var endpoint = "127.0.0.1:8831"

func init() {
	if v := os.Getenv("CERESDB_ADDR"); v != "" {
		endpoint = v
	}
}

func currentMS() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func TestBaseWriteAndQuery(t *testing.T) {
	t.Skip("ignore local test")

	client, err := ceresdb.NewClient(endpoint, ceresdb.Direct, ceresdb.WithDefaultDatabase("public"))
	require.NoError(t, err, "init ceresdb client failed")
	timestamp := currentMS()

	testBaseWrite(t, client, "ceresdb_test", timestamp, 2)
	testBaseQuery(t, client, "ceresdb_test", timestamp, 2)
}

func TestNoDatabaseSelected(t *testing.T) {
	t.Skip("ignore local test")

	client, err := ceresdb.NewClient(endpoint, ceresdb.Direct)
	require.NoError(t, err, "init ceresdb client failed")

	points, err := buildTablePoints("test", currentMS(), 3)
	require.NoError(t, err, "build points failed")
	require.Equal(t, len(points), 3, "build points failed, not expected")

	req := ceresdb.WriteRequest{
		Points: points,
	}
	_, err = client.Write(context.Background(), req)
	require.ErrorIs(t, err, ceresdb.ErrNoDatabaseSelected)
}

func TestDatabaseInRequest(t *testing.T) {
	t.Skip("ignore local test")

	client, err := ceresdb.NewClient(endpoint, ceresdb.Direct, ceresdb.WithDefaultDatabase("not_exist_db"))
	require.NoError(t, err, "init ceresdb client failed")

	points, err := buildTablePoints("test", currentMS(), 3)
	require.NoError(t, err, "build points failed")
	require.Equal(t, len(points), 3, "build points failed, not expected")

	req := ceresdb.WriteRequest{
		ReqCtx: ceresdb.RequestContext{
			Database: "public",
		},
		Points: points,
	}
	resp, err := client.Write(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, resp.Success, uint32(3))
}

// nolint
func buildTablePoints(table string, timestamp int64, count int) ([]ceresdb.Point, error) {
	points := make([]ceresdb.Point, 0, count)
	for idx := 0; idx < count; idx++ {
		point, err := ceresdb.NewPointBuilder(table).
			SetTimestamp(timestamp).
			AddTag("tagA", ceresdb.NewStringValue(fmt.Sprintf("tagA:%s:%d", table, idx))).
			AddTag("tagB", ceresdb.NewStringValue(fmt.Sprintf("tagB:%s:%d", table, idx))).
			AddField("vbool", ceresdb.NewBoolValue(true)).
			AddField("vstring", ceresdb.NewStringValue(fmt.Sprintf("row%d", idx))).
			AddField("vfloat64", ceresdb.NewDoubleValue(0.64)).
			AddField("vfloat32", ceresdb.NewFloatValue(0.32)).
			AddField("vint64", ceresdb.NewInt64Value(-64)).
			AddField("vint32", ceresdb.NewInt32Value(-32)).
			AddField("vint16", ceresdb.NewInt16Value(-16)).
			AddField("vint8", ceresdb.NewInt8Value(-8)).
			AddField("vuint64", ceresdb.NewUint64Value(64)).
			AddField("vuint32", ceresdb.NewUint32Value(32)).
			AddField("vuint16", ceresdb.NewUint16Value(16)).
			AddField("vuint8", ceresdb.NewUint8Value(8)).
			AddField("vbinary", ceresdb.NewVarbinaryValue([]byte{1, 2, 3})).
			Build()
		if err != nil {
			return nil, err
		}
		points = append(points, point)
	}
	return points, nil
}

// nolint
func testBaseWrite(t *testing.T, client ceresdb.Client, table string, timestamp int64, count int) {
	points, err := buildTablePoints(table, timestamp, count)
	require.NoError(t, err, "build points failed")
	require.Equal(t, len(points), count, "build points failed, not expected")

	req := ceresdb.WriteRequest{
		Points: points,
	}
	resp, err := client.Write(context.Background(), req)
	require.NoError(t, err, "write points failed")

	require.Equal(t, resp.Success, uint32(count), "write success value is not expected")

	t.Log(table + " base write is paas")
}

// nolint
func testBaseQuery(t *testing.T, client ceresdb.Client, table string, timestamp int64, count int) {
	req := ceresdb.SQLQueryRequest{
		Tables: []string{table},
		SQL:    fmt.Sprintf("select * from %s where timestamp = %d", table, timestamp),
	}
	resp, err := client.SQLQuery(context.Background(), req)
	require.NoError(t, err, "query rows failed")

	require.Equal(t, len(resp.Rows), count, "query rowCount value is not expected")

	rows := resp.Rows
	require.Equal(t, len(rows), count, "rows size not expected")

	row1 := rows[0]

	require.True(t, row1.HasColumn("timestamp"), "column timestamp not found")
	ts := row1.ColumnValue("timestamp").TimestampValue()
	require.Equal(t, ts, timestamp, "timestamp int not expected")

	require.True(t, row1.HasColumn("tagA"), "column tagA not found")
	t1 := row1.ColumnValue("tagA").StringValue()
	require.Contains(t, t1, fmt.Sprintf("tagA:%s", table), "tag t1 is not expected")

	require.True(t, row1.HasColumn("tagB"), "column tagB not found")
	t2 := row1.ColumnValue("tagB").StringValue()
	require.Contains(t, t2, fmt.Sprintf("tagB:%s", table), "tag t2 is not expected")

	require.True(t, row1.HasColumn("vbool"), "column vbool not found")
	vBool := row1.ColumnValue("vbool").BoolValue()
	require.Equal(t, vBool, true, "vbool is not expected")

	require.True(t, row1.HasColumn("vstring"), "column vstring not found")
	vString := row1.ColumnValue("vstring").StringValue()
	require.Contains(t, vString, "row", "vstring is not expected")

	require.True(t, row1.HasColumn("vfloat64"), "column vfloat64 not found")
	vFloat64 := row1.ColumnValue("vfloat64").DoubleValue()
	require.Equal(t, vFloat64, float64(0.64), "vfloat64 is not expected")

	require.True(t, row1.HasColumn("vfloat32"), "column vfloat32 not found")
	vFloat32 := row1.ColumnValue("vfloat32").FloatValue()
	require.Equal(t, vFloat32, float32(0.32), "vfloat32 is not expected")

	require.True(t, row1.HasColumn("vint64"), "column vint64 not found")
	vInt64 := row1.ColumnValue("vint64").Int64Value()
	require.Equal(t, vInt64, int64(-64), "vint64 is not expected")

	require.True(t, row1.HasColumn("vint32"), "column vint32 not found")
	vInt32 := row1.ColumnValue("vint32").Int32Value()
	require.Equal(t, vInt32, int32(-32), "vint32 is not expected")

	require.True(t, row1.HasColumn("vint16"), "column vint16 not found")
	vInt16 := row1.ColumnValue("vint16").Int16Value()
	require.Equal(t, vInt16, int16(-16), "vint16 is not expected")

	require.True(t, row1.HasColumn("vint8"), "column vint8 not found")
	vInt8 := row1.ColumnValue("vint8").Int8Value()
	require.Equal(t, vInt8, int8(-8), "vint8 is not expected")

	require.True(t, row1.HasColumn("vuint64"), "column vuint64 not found")
	vUInt64 := row1.ColumnValue("vuint64").Uint64Value()
	require.Equal(t, vUInt64, uint64(64), "vuint64 is not expected")

	require.True(t, row1.HasColumn("vuint32"), "column vuint32 not found")
	vUInt32 := row1.ColumnValue("vuint32").Uint32Value()
	require.Equal(t, vUInt32, uint32(32), "vuint32 is not expected")

	require.True(t, row1.HasColumn("vuint16"), "column vuint16 not found")
	vUInt16 := row1.ColumnValue("vuint16").Uint16Value()
	require.Equal(t, vUInt16, uint16(16), "vuint16 is not expected")

	require.True(t, row1.HasColumn("vuint8"), "column vuint8 not found")
	vUInt8 := row1.ColumnValue("vuint8").Uint8Value()
	require.Equal(t, vUInt8, uint8(8), "vuint8 is not expected")

	require.True(t, row1.HasColumn("vbinary"), "column vbinary not found")
	vBinary := row1.ColumnValue("vbinary").VarbinaryValue()
	require.Equal(t, vBinary, []byte{1, 2, 3}, "vbinary is not expected")

	t.Log(table + " base query is paas")
}
