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

type TablePointsBuilder struct {
	table  string
	points []types.Point
}

func NewTablePointsBuilder(table string) *TablePointsBuilder {
	return &TablePointsBuilder{
		table:  table,
		points: make([]types.Point, 0),
	}
}

func NewPointBuilder(table string) *PointBuilder {
	return &PointBuilder{
		point: types.Point{
			Table:  table,
			Tags:   make(map[string]types.Value),
			Fields: make(map[string]types.Value),
		},
	}
}

func (b *TablePointsBuilder) AddPoint() *PointBuilder {
	return &PointBuilder{
		parent: b,
		point: types.Point{
			Table:  b.table,
			Tags:   make(map[string]types.Value),
			Fields: make(map[string]types.Value),
		},
	}
}

func (b *TablePointsBuilder) Build() ([]types.Point, error) {
	if b.table == "" {
		return nil, types.ErrPointEmptyTable
	}

	for _, point := range b.points {
		if err := checkPoint(point); err != nil {
			return nil, err
		}
	}

	return b.points, nil
}

type PointBuilder struct {
	parent *TablePointsBuilder
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

func (b *PointBuilder) Build() (types.Point, error) {
	err := checkPoint(b.point)
	if err != nil {
		return types.Point{}, err
	}
	return b.point, nil
}

func (b *PointBuilder) BuildAndContinue() *TablePointsBuilder {
	b.parent.points = append(b.parent.points, b.point)
	return b.parent
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
