// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package utils

import (
	"errors"
	"github.com/CeresDB/ceresdb-client-go/types"

	"github.com/CeresDB/ceresdbproto/golang/pkg/storagepb"
	"github.com/linkedin/goavro"
)

func ParseQueryResponse(response *storagepb.QueryResponse) ([]types.Row, error) {
	// schema is nil when SQL is DDL(such as create/drop table)
	if response.SchemaContent == "" {
		return []map[string]interface{}{}, nil
	}

	codec, err := goavro.NewCodec(response.SchemaContent)
	if err != nil {
		return nil, err
	}

	arvoRecords := make([]map[string]interface{}, 0, len(response.Rows))
	for _, binaryRow := range response.Rows {
		v, _, err := codec.NativeFromBinary(binaryRow)
		if err != nil {
			return nil, err
		}
		arvoRecord, ok := v.(map[string]interface{})
		if !ok {
			return nil, errors.New("response is not record type")
		}
		arvoRecords = append(arvoRecords, arvoRecord)
	}

	return arvoRecords, nil
}
