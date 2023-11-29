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

package ceresdb

import (
	"fmt"
)

const (
	reservedColumnTsid      = "tsid"
	reservedColumnTimestamp = "timestamp"
)

type Point struct {
	Table     string
	Timestamp int64
	Tags      map[string]Value
	Fields    map[string]Value
}

func NewPointBuilder(table string) *PointBuilder {
	return &PointBuilder{
		point: Point{
			Table:  table,
			Tags:   make(map[string]Value),
			Fields: make(map[string]Value),
		},
	}
}

type PointBuilder struct {
	point Point
}

func (b *PointBuilder) SetTimestamp(timestamp int64) *PointBuilder {
	b.point.Timestamp = timestamp
	return b
}

func (b *PointBuilder) AddTag(k string, v Value) *PointBuilder {
	b.point.Tags[k] = v
	return b
}

func (b *PointBuilder) AddField(k string, v Value) *PointBuilder {
	b.point.Fields[k] = v
	return b
}

func (b *PointBuilder) Build() (Point, error) {
	err := checkPoint(b.point)
	if err != nil {
		return Point{}, err
	}
	return b.point, nil
}

func checkPoint(point Point) error {
	if point.Table == "" {
		return ErrPointEmptyTable
	}

	if point.Timestamp <= 0 {
		return ErrPointEmptyTimestamp
	}

	if len(point.Tags) == 0 {
		return ErrPointEmptyTags
	}

	if len(point.Fields) == 0 {
		return ErrPointEmptyFields
	}

	for tagK := range point.Tags {
		if isReservedColumn(tagK) {
			return fmt.Errorf("tag name is reserved column name in ceresdb, name:%s", tagK)
		}
	}

	return nil
}

func isReservedColumn(name string) bool {
	return name == reservedColumnTsid || name == reservedColumnTimestamp
}
