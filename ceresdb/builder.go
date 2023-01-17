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

type PointsBuilder struct {
	table  string
	points []types.Point
}

func NewPointsBuilder(table string) *PointsBuilder {
	return &PointsBuilder{
		table:  table,
		points: make([]types.Point, 0),
	}
}

func (b *PointsBuilder) Add() *PointBuilder {
	return &PointBuilder{
		parent: b,
		point: types.Point{
			Table:  b.table,
			Tags:   make(map[string]types.Value),
			Fields: make(map[string]types.Value),
		},
	}
}

func (b *PointsBuilder) Build() ([]types.Point, error) {
	if b.table == "" {
		return nil, types.ErrPointEmptyTable
	}

	for _, point := range b.points {
		if point.Timestamp <= 0 {
			return nil, types.ErrPointEmptyTimestamp
		}

		if len(point.Fields) == 0 {
			return nil, types.ErrPointEmptyFields
		}

		for tagK := range point.Tags {
			if isReservedColumn(tagK) {
				return nil, fmt.Errorf("tag name is reserved column name in ceresdb, name:%s", tagK)
			}
		}
	}

	return b.points, nil
}

type PointBuilder struct {
	parent *PointsBuilder
	point  types.Point
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

func (b *PointBuilder) Build() *PointsBuilder {
	return b.parent
}

func isReservedColumn(name string) bool {
	return name == ReservedColumnTsid || name == ReservedColumnTimestamp
}
