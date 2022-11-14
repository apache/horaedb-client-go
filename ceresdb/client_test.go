// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ceresdb_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/CeresDB/ceresdb-client-go/ceresdb"
	"github.com/CeresDB/ceresdb-client-go/types"
	"github.com/stretchr/testify/assert"
)

var endpoint = "127.0.0.1:8831"

func init() {
	if v := os.Getenv("CERESDB_ADDR"); v != "" {
		endpoint = v
	}
}

func now() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func build2Rows() ([]*types.Row, error) {
	rows := make([]*types.Row, 0, 2)

	timestamp := int64(1668124800000)

	builder := types.NewRowBuilder("ceresdb_test")

	row1, err := builder.
		SetTimestamp(timestamp).
		AddTag("t1", "1A").
		AddTag("t2", "2A").
		AddField("vbool", true).
		AddField("vstring", "row1").
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
	rows = append(rows, row1)

	row2, err := builder.Reset().
		SetTimestamp(timestamp+1000).
		AddTag("t1", "1A").
		AddTag("t2", "2B").
		AddField("vbool", true).
		AddField("vstring", "row2").
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
	rows = append(rows, row2)

	return rows, nil
}

func TestBaseWrite(t *testing.T) {
	client, err := ceresdb.NewClient(endpoint)
	assert.NoError(t, err, "init ceresb client failed")

	rows, err := build2Rows()
	assert.NoError(t, err, "build rows failed")

	resp, err := client.Write(context.Background(), rows)
	assert.NoError(t, err, "write rows failed")

	assert.Equal(t, resp.Success, uint32(2), "write success value is not expected")

	t.Log("base write is paas")

}

func TestBaseQuery(t *testing.T) {
	client, err := ceresdb.NewClient(endpoint)
	assert.NoError(t, err, "init ceredb client failed")

	req := types.QueryRequest{
		Metrics: nil,
		Ql:      `select * from ceresdb_test`,
	}
	resp, err := client.Query(context.Background(), req)
	assert.NoError(t, err, "query rows failed")

	assert.Equal(t, resp.RowCount, uint32(2), "query rowCount value is not expected")

	records := resp.MapToRecord()
	assert.Equal(t, len(records), 2, "map to record size is not expected")

	r1 := records[0]

	timestamp, err := r1.GetTimestamp()
	assert.NoError(t, err, "get timestamp fail")
	assert.Equal(t, timestamp, int64(1668124800000), "timestamp int not expected")

	t1, err := r1.GetString("t1")
	assert.NoError(t, err, "get tag t1 fail")
	assert.Equal(t, t1, "1A", "tag t1 is not expected")

	t2, err := r1.GetString("t2")
	assert.NoError(t, err, "get tag t2 fail")
	assert.Equal(t, t2, "2A", "tag t2 is not expected")

	vBool, err := r1.GetBool("vbool")
	assert.NoError(t, err, "get vbool fail")
	assert.Equal(t, vBool, true, "vbool is not expected")

	vString, err := r1.GetString("vstring")
	assert.NoError(t, err, "get vstring fail")
	assert.Equal(t, vString, "row1", "vstring is not expected")

	vFloat64, err := r1.GetFloat64("vfloat64")
	assert.NoError(t, err, "get vfloat64 fail")
	assert.Equal(t, vFloat64, float64(0.64), "vfloat64 is not expected")

	vFloat32, err := r1.GetFloat32("vfloat32")
	assert.NoError(t, err, "get vfloat32 fail")
	assert.Equal(t, vFloat32, float32(0.32), "vfloat32 is not expected")

	vInt, err := r1.GetInt("vint")
	assert.NoError(t, err, "get vint fail")
	assert.Equal(t, vInt, int(-1), "vint is not expected")

	vInt64, err := r1.GetInt64("vint64")
	assert.NoError(t, err, "get vint64 fail")
	assert.Equal(t, vInt64, int64(-64), "vint64 is not expected")

	vInt32, err := r1.GetInt32("vint32")
	assert.NoError(t, err, "get vint32 fail")
	assert.Equal(t, vInt32, int32(-32), "vint32 is not expected")

	vInt16, err := r1.GetInt16("vint16")
	assert.NoError(t, err, "get vint16 fail")
	assert.Equal(t, vInt16, int16(-16), "vint16 is not expected")

	vInt8, err := r1.GetInt8("vint8")
	assert.NoError(t, err, "get vint8 fail")
	assert.Equal(t, vInt8, int8(-8), "vint8 is not expected")

	vUInt, err := r1.GetUint("vuint")
	assert.NoError(t, err, "get vuint fail")
	assert.Equal(t, vUInt, uint(1), "vuint is not expected")

	vUInt64, err := r1.GetUInt64("vuint64")
	assert.NoError(t, err, "get vuint64 fail")
	assert.Equal(t, vUInt64, uint64(64), "vuint64 is not expected")

	vUInt32, err := r1.GetUInt32("vuint32")
	assert.NoError(t, err, "get vuint32 fail")
	assert.Equal(t, vUInt32, uint32(32), "vuint32 is not expected")

	vUInt16, err := r1.GetUInt16("vuint16")
	assert.NoError(t, err, "get vuint16 fail")
	assert.Equal(t, vUInt16, uint16(16), "vuint16 is not expected")

	vUInt8, err := r1.GetUInt8("vuint8")
	assert.NoError(t, err, "get vuint32 fail")
	assert.Equal(t, vUInt8, uint8(8), "vuint8 is not expected")

	t.Log("base query is paas")
}
