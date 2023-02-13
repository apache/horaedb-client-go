// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ceresdb

import (
	"fmt"

	"github.com/CeresDB/ceresdb-client-go/types"
)

const (
	ReservedColumnTsid      = "tsid"
	ReservedColumnTimestamp = "timestamp"
)

func NewPointBuilder(table string) *PointBuilder {
	return &PointBuilder{
		point: types.Point{
			Table:  table,
			Tags:   make(map[string]types.Value),
			Fields: make(map[string]types.Value),
		},
	}
}

type PointBuilder struct {
	point types.Point
}

func (b *PointBuilder) SetTimestamp(timestamp int64) *PointBuilder {
	b.point.Timestamp = timestamp
	return b
}

func (b *PointBuilder) AddTag(k string, v types.Value) *PointBuilder {
	b.point.Tags[k] = v
	return b
}

func (b *PointBuilder) AddField(k string, v types.Value) *PointBuilder {
	b.point.Fields[k] = v
	return b
}

func (b *PointBuilder) Build() (types.Point, error) {
	err := checkPoint(b.point)
	if err != nil {
		return types.Point{}, err
	}
	return b.point, nil
}

func checkPoint(point types.Point) error {
	if point.Table == "" {
		return types.ErrPointEmptyTable
	}

	if point.Timestamp <= 0 {
		return types.ErrPointEmptyTimestamp
	}

	if len(point.Tags) == 0 {
		return types.ErrPointEmptyTags
	}

	if len(point.Fields) == 0 {
		return types.ErrPointEmptyFields
	}

	for tagK := range point.Tags {
		if isReservedColumn(tagK) {
			return fmt.Errorf("tag name is reserved column name in ceresdb, name:%s", tagK)
		}
	}

	return nil
}

func isReservedColumn(name string) bool {
	return name == ReservedColumnTsid || name == ReservedColumnTimestamp
}
