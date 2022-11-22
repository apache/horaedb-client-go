// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ceresdb

import (
	"fmt"
	"github.com/CeresDB/ceresdb-client-go/types"
)

const (
	RESERVED_COLUMN_TSID      = "tsid"
	RESERVED_COLUMN_TIMESTAMP = "timestamp"
)

type RowBuilder struct {
	metric string
	row    *types.Row
	built  bool
}

func NewRowBuilder(metric string) *RowBuilder {
	return &RowBuilder{
		metric: metric,
		row: &types.Row{
			Series: types.Series{
				Metric: metric,
				Tags:   make(map[string]string),
			},
			Fields: make(map[string]interface{}),
		},
		built: false,
	}
}

// reset bulider
// The row can then be built again with the same metric
func (b *RowBuilder) Reset() *RowBuilder {
	b.row = &types.Row{
		Series: types.Series{
			Metric: b.metric,
			Tags:   make(map[string]string),
		},
		Fields: make(map[string]interface{}),
	}
	b.built = false
	return b
}

func (b *RowBuilder) SetTimestamp(timestamp int64) *RowBuilder {
	b.row.Timestamp = timestamp
	return b
}

func (b *RowBuilder) AddTag(k, v string) *RowBuilder {
	b.row.Tags[k] = v
	return b
}

func (b *RowBuilder) AddField(k string, v interface{}) *RowBuilder {
	b.row.Fields[k] = v
	return b
}

func (b *RowBuilder) Build() (*types.Row, error) {
	if b.built {
		return nil, types.ErrBuiltBuilder
	}

	row := b.row

	if row.Metric == "" {
		return nil, types.ErrRowEmptyMetric
	}

	if row.Timestamp <= 0 {
		return nil, types.ErrRowEmptyTimestamp
	}

	if len(row.Fields) == 0 {
		return nil, types.ErrRowEmptyFields
	}

	for tagK, _ := range row.Tags {
		if isReservedColumn(tagK) {
			return nil, fmt.Errorf("Tag name is reserved column name in ceresdb, name:%s", tagK)
		}
	}

	for fieldK, fieldV := range row.Fields {
		if isReservedColumn(fieldK) {
			return nil, fmt.Errorf("Field name is reserved column name in ceresdb, name:%s", fieldK)
		}

		convertedFieldV, err := convertField(fieldV)
		if err != nil {
			return nil, fmt.Errorf("Not valid field, key:%s, value:%v", fieldK, fieldV)
		}
		row.Fields[fieldK] = convertedFieldV
	}

	b.built = true
	return row, nil
}

func isReservedColumn(name string) bool {
	return name == RESERVED_COLUMN_TSID || name == RESERVED_COLUMN_TIMESTAMP
}

func convertField(v interface{}) (interface{}, error) {
	switch v := v.(type) {
	case bool, string, float64, float32, int64, int32, int16, int8, uint64, uint32, uint16, uint8:
		return v, nil
	case int:
		return int64(v), nil
	case uint:
		return uint64(v), nil
	default:
		return nil, types.ErrRowInvalidFieldType
	}
}
