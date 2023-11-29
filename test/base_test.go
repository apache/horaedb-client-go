/*
 * Copyright 2022 The HoraeDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/CeresDB/horaedb-client-go/horaedb"
	"github.com/stretchr/testify/require"
)

var endpoint = "127.0.0.1:8831"

func init() {
	if v := os.Getenv("HORAEDB_ADDR"); v != "" {
		endpoint = v
	}
}

func currentMS() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func TestBaseWriteAndQuery(t *testing.T) {
	t.Skip("ignore local test")

	client, err := horaedb.NewClient(endpoint, horaedb.Direct, horaedb.WithDefaultDatabase("public"))
	require.NoError(t, err, "init horaedb client failed")
	timestamp := currentMS()

	testBaseWrite(t, client, "horaedb_test", timestamp, 2)
	testBaseQuery(t, client, "horaedb_test", timestamp, 2)
}

func TestNoDatabaseSelected(t *testing.T) {
	t.Skip("ignore local test")

	client, err := horaedb.NewClient(endpoint, horaedb.Direct)
	require.NoError(t, err, "init horaedb client failed")

	points, err := buildTablePoints("test", currentMS(), 3)
	require.NoError(t, err, "build points failed")
	require.Equal(t, len(points), 3, "build points failed, not expected")

	req := horaedb.WriteRequest{
		Points: points,
	}
	_, err = client.Write(context.Background(), req)
	require.ErrorIs(t, err, horaedb.ErrNoDatabaseSelected)
}

func TestDatabaseInRequest(t *testing.T) {
	t.Skip("ignore local test")

	client, err := horaedb.NewClient(endpoint, horaedb.Direct, horaedb.WithDefaultDatabase("not_exist_db"))
	require.NoError(t, err, "init horaedb client failed")

	points, err := buildTablePoints("horaedb_test", currentMS(), 3)
	require.NoError(t, err, "build points failed")
	require.Equal(t, len(points), 3, "build points failed, not expected")

	req := horaedb.WriteRequest{
		ReqCtx: horaedb.RequestContext{
			Database: "public",
		},
		Points: points,
	}
	resp, err := client.Write(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, resp.Success, uint32(3))
}

// nolint
func buildTablePoints(table string, timestamp int64, count int) ([]horaedb.Point, error) {
	points := make([]horaedb.Point, 0, count)
	for idx := 0; idx < count; idx++ {
		point, err := horaedb.NewPointBuilder(table).
			SetTimestamp(timestamp).
			AddTag("tagA", horaedb.NewStringValue(fmt.Sprintf("tagA:%s:%d", table, idx))).
			AddTag("tagB", horaedb.NewStringValue(fmt.Sprintf("tagB:%s:%d", table, idx))).
			AddField("vbool", horaedb.NewBoolValue(true)).
			AddField("vstring", horaedb.NewStringValue(fmt.Sprintf("row%d", idx))).
			AddField("vfloat64", horaedb.NewDoubleValue(0.64)).
			AddField("vfloat32", horaedb.NewFloatValue(0.32)).
			AddField("vint64", horaedb.NewInt64Value(-64)).
			AddField("vint32", horaedb.NewInt32Value(-32)).
			AddField("vint16", horaedb.NewInt16Value(-16)).
			AddField("vint8", horaedb.NewInt8Value(-8)).
			AddField("vuint64", horaedb.NewUint64Value(64)).
			AddField("vuint32", horaedb.NewUint32Value(32)).
			AddField("vuint16", horaedb.NewUint16Value(16)).
			AddField("vuint8", horaedb.NewUint8Value(8)).
			AddField("vbinary", horaedb.NewVarbinaryValue([]byte{1, 2, 3})).
			Build()
		if err != nil {
			return nil, err
		}
		points = append(points, point)
	}
	return points, nil
}

// nolint
func testBaseWrite(t *testing.T, client horaedb.Client, table string, timestamp int64, count int) {
	points, err := buildTablePoints(table, timestamp, count)
	require.NoError(t, err, "build points failed")
	require.Equal(t, len(points), count, "build points failed, not expected")

	req := horaedb.WriteRequest{
		Points: points,
	}
	resp, err := client.Write(context.Background(), req)
	require.NoError(t, err, "write points failed")

	require.Equal(t, resp.Success, uint32(count), "write success value is not expected")

	t.Log(table + " base write is paas")
}

// nolint
func testBaseQuery(t *testing.T, client horaedb.Client, table string, timestamp int64, count int) {
	req := horaedb.SQLQueryRequest{
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
	ts, ok := row1.Column("timestamp")
	require.True(t, ok, "timestamp int not found")
	require.Equal(t, ts.Value().TimestampValue(), timestamp, "timestamp int not expected")

	require.True(t, row1.HasColumn("tagA"), "column tagA not found")
	t1, ok := row1.Column("tagA")
	require.True(t, ok, "tag t1 not found")
	require.Contains(t, t1.Value().StringValue(), fmt.Sprintf("tagA:%s", table), "tag t1 is not expected")

	require.True(t, row1.HasColumn("tagB"), "column tagB not found")
	t2, ok := row1.Column("tagB")
	require.True(t, ok, "tag t1 not found")
	require.Contains(t, t2.Value().StringValue(), fmt.Sprintf("tagB:%s", table), "tag t2 is not expected")

	require.True(t, row1.HasColumn("vbool"), "column vbool not found")
	vBool, ok := row1.Column("vbool")
	require.True(t, ok, "vbool not found")
	require.Equal(t, vBool.Value().BoolValue(), true, "vbool is not expected")

	require.True(t, row1.HasColumn("vstring"), "column vstring not found")
	vString, ok := row1.Column("vstring")
	require.True(t, ok, "vstring not found")
	require.Contains(t, vString.Value().StringValue(), "row", "vstring is not expected")

	require.True(t, row1.HasColumn("vfloat64"), "column vfloat64 not found")
	vFloat64, ok := row1.Column("vfloat64")
	require.True(t, ok, "vfloat64 found")
	require.Equal(t, vFloat64.Value().DoubleValue(), float64(0.64), "vfloat64 is not expected")

	require.True(t, row1.HasColumn("vfloat32"), "column vfloat32 not found")
	vFloat32, ok := row1.Column("vfloat32")
	require.True(t, ok, "vfloat32 not found")
	require.Equal(t, vFloat32.Value().FloatValue(), float32(0.32), "vfloat32 is not expected")

	require.True(t, row1.HasColumn("vint64"), "column vint64 not found")
	vInt64, ok := row1.Column("vint64")
	require.True(t, ok, "vint64 not found")
	require.Equal(t, vInt64.Value().Int64Value(), int64(-64), "vint64 is not expected")

	require.True(t, row1.HasColumn("vint32"), "column vint32 not found")
	vInt32, ok := row1.Column("vint32")
	require.True(t, ok, "vint32 not found")
	require.Equal(t, vInt32.Value().Int32Value(), int32(-32), "vint32 is not expected")

	require.True(t, row1.HasColumn("vint16"), "column vint16 not found")
	vInt16, ok := row1.Column("vint16")
	require.True(t, ok, "vint16 not found")
	require.Equal(t, vInt16.Value().Int16Value(), int16(-16), "vint16 is not expected")

	require.True(t, row1.HasColumn("vint8"), "column vint8 not found")
	vInt8, ok := row1.Column("vint8")
	require.True(t, ok, "vint8 not found")
	require.Equal(t, vInt8.Value().Int8Value(), int8(-8), "vint8 is not expected")

	require.True(t, row1.HasColumn("vuint64"), "column vuint64 not found")
	vUInt64, ok := row1.Column("vuint64")
	require.True(t, ok, "uvint64 not found")
	require.Equal(t, vUInt64.Value().Uint64Value(), uint64(64), "vuint64 is not expected")

	require.True(t, row1.HasColumn("vuint32"), "column vuint32 not found")
	vUInt32, ok := row1.Column("vuint32")
	require.True(t, ok, "vuint32 not found")
	require.Equal(t, vUInt32.Value().Uint32Value(), uint32(32), "vuint32 is not expected")

	require.True(t, row1.HasColumn("vuint16"), "column vuint16 not found")
	vUInt16, ok := row1.Column("vuint16")
	require.True(t, ok, "vuint16 not found")
	require.Equal(t, vUInt16.Value().Uint16Value(), uint16(16), "vuint16 is not expected")

	require.True(t, row1.HasColumn("vuint8"), "column vuint8 not found")
	vUInt8, ok := row1.Column("vuint8")
	require.True(t, ok, "vuint8 not found")
	require.Equal(t, vUInt8.Value().Uint8Value(), uint8(8), "vuint8 is not expected")

	require.True(t, row1.HasColumn("vbinary"), "column vbinary not found")
	vBinary, ok := row1.Column("vbinary")
	require.True(t, ok, "vbinary not found")
	require.Equal(t, vBinary.Value().VarbinaryValue(), []byte{1, 2, 3}, "vbinary is not expected")

	require.False(t, row1.HasColumn("vnot_exist"), "vnot_exist found")
	_, notExist := row1.Column("vnot_exist")
	require.False(t, notExist, "vnot_exist found")

	t.Log(table + " base query is paas")
}
