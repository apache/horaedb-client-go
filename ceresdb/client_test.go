// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ceresdb_test

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/CeresDB/ceresdb-client-go/ceresdb"
	"github.com/CeresDB/ceresdb-client-go/types"
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

	curr := now()

	builder := types.NewRowBuilder("ceresdb_test")

	row1, err := builder.
		SetTimestamp(curr).
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
		SetTimestamp(curr+10).
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
	if err != nil {
		log.Fatalf("init ceresdb client failed, err:%v", err)
	}

	rows, err := build2Rows()
	if err != nil {
		log.Fatalf("build rows failed, err:%v", err)
	}

	resp, err := client.Write(context.Background(), rows)
	if err != nil {
		log.Fatalf("write point failed, err:%v", err)
	}

	if resp.Success != 2 {
		t.Errorf("Expect 2, got %d", resp.Success)
	}
}
