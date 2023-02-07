// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package utils

import (
	"errors"
	"fmt"

	"github.com/CeresDB/ceresdb-client-go/types"
	"github.com/CeresDB/ceresdbproto/golang/pkg/storagepb"
)

func GetTablesFromPoints(points []types.Point) []string {
	dict := make(map[string]byte)
	tables := make([]string, 0, len(points))
	for _, point := range points {
		if _, ok := dict[point.Table]; !ok {
			dict[point.Table] = 0
			tables = append(tables, point.Table)
		}
	}
	return tables
}

func SplitPointsByRoute(points []types.Point, routes map[string]types.Route) (map[string][]types.Point, error) {
	pointsByRoute := make(map[string][]types.Point, len(routes))
	for _, point := range points {
		route, ok := routes[point.Table]
		if !ok {
			return nil, fmt.Errorf("route for table %s not found", point.Table)
		}
		if rows, ok := pointsByRoute[route.Endpoint]; ok {
			pointsByRoute[route.Endpoint] = append(rows, point)
		} else {
			pointsByRoute[route.Endpoint] = []types.Point{point}
		}
	}

	return pointsByRoute, nil
}

func CombineWriteResponse(r1 types.WriteResponse, r2 types.WriteResponse) types.WriteResponse {
	r1.Success += r2.Success
	r1.Failed += r2.Failed
	return r1
}

func BuildPbWriteRequest(points []types.Point) (*storagepb.WriteRequest, error) {
	tuples := make(map[string]*writeTuple) // table -> tuple

	for _, point := range points {
		tuple, ok := tuples[point.Table]
		if !ok {
			tuple = &writeTuple{
				writeSeriesEntries: map[string]*storagepb.WriteSeriesEntry{},
				orderedTags:        orderedNames{nameIndexes: map[string]int{}},
				orderedFields:      orderedNames{nameIndexes: map[string]int{}},
			}
			tuples[point.Table] = tuple
		}

		seriesKey := ""
		for tagK := range point.Tags {
			tuple.orderedTags.insert(tagK)
		}
		for _, orderedTag := range tuple.orderedTags.toOrdered() {
			seriesKey += point.Tags[orderedTag].StringValue()
		}

		writeEntry, ok := tuple.writeSeriesEntries[seriesKey]
		if !ok {
			writeEntry = &storagepb.WriteSeriesEntry{
				Tags:        make([]*storagepb.Tag, 0, len(point.Tags)),
				FieldGroups: make([]*storagepb.FieldGroup, 0, 1),
			}
			for idx, orderedTag := range tuple.orderedTags.toOrdered() {
				tagV := point.Tags[orderedTag]
				if tagV.IsNull() {
					continue
				}
				writeEntry.Tags = append(writeEntry.Tags, &storagepb.Tag{
					NameIndex: uint32(idx),
					Value: &storagepb.Value{
						Value: &storagepb.Value_StringValue{
							StringValue: tagV.StringValue(),
						},
					},
				})
			}
			tuple.writeSeriesEntries[seriesKey] = writeEntry
		}

		fieldGroup := &storagepb.FieldGroup{
			Timestamp: point.Timestamp,
			Fields:    make([]*storagepb.Field, 0, len(point.Fields)),
		}
		for fieldK, fieldV := range point.Fields {
			idx := tuple.orderedFields.insert(fieldK)
			if fieldV.IsNull() {
				continue
			}
			pbV, err := buildPbValue(fieldV)
			if err != nil {
				return nil, err
			}
			fieldGroup.Fields = append(fieldGroup.Fields, &storagepb.Field{
				NameIndex: uint32(idx),
				Value:     pbV,
			})
		}
		writeEntry.FieldGroups = append(writeEntry.FieldGroups, fieldGroup)
	}

	writeRequest := &storagepb.WriteRequest{
		TableRequests: make([]*storagepb.WriteTableRequest, 0, len(tuples)),
	}
	for table, tuple := range tuples {
		writeTableReq := storagepb.WriteTableRequest{
			Table:   table,
			Entries: []*storagepb.WriteSeriesEntry{},
		}
		writeTableReq.TagNames = tuple.orderedTags.toOrdered()
		writeTableReq.FieldNames = tuple.orderedFields.toOrdered()
		for _, writeSeriesEntry := range tuple.writeSeriesEntries {
			writeTableReq.Entries = append(writeTableReq.Entries, writeSeriesEntry)
		}
		writeRequest.TableRequests = append(writeRequest.TableRequests, &writeTableReq)
	}
	return writeRequest, nil
}

func buildPbValue(v types.Value) (*storagepb.Value, error) {
	switch v.DataType() {
	case types.BOOL:
		return &storagepb.Value{
			Value: &storagepb.Value_BoolValue{
				BoolValue: v.BoolValue(),
			},
		}, nil
	case types.STRING:
		return &storagepb.Value{
			Value: &storagepb.Value_StringValue{
				StringValue: v.StringValue(),
			},
		}, nil
	case types.DOUBLE:
		return &storagepb.Value{
			Value: &storagepb.Value_Float64Value{
				Float64Value: v.DoubleValue(),
			},
		}, nil
	case types.FLOAT:
		return &storagepb.Value{
			Value: &storagepb.Value_Float32Value{
				Float32Value: v.FloatValue(),
			},
		}, nil
	case types.INT64:
		return &storagepb.Value{
			Value: &storagepb.Value_Int64Value{
				Int64Value: v.Int64Value(),
			},
		}, nil
	case types.INT32:
		return &storagepb.Value{
			Value: &storagepb.Value_Int32Value{
				Int32Value: v.Int32Value(),
			},
		}, nil
	case types.INT16:
		return &storagepb.Value{
			Value: &storagepb.Value_Int16Value{
				Int16Value: int32(v.Int16Value()),
			},
		}, nil
	case types.INT8:
		return &storagepb.Value{
			Value: &storagepb.Value_Int8Value{
				Int8Value: int32(v.Int8Value()),
			},
		}, nil
	case types.UINT64:
		return &storagepb.Value{
			Value: &storagepb.Value_Uint64Value{
				Uint64Value: v.Uint64Value(),
			},
		}, nil
	case types.UINT32:
		return &storagepb.Value{
			Value: &storagepb.Value_Uint32Value{
				Uint32Value: v.Uint32Value(),
			},
		}, nil
	case types.UINT16:
		return &storagepb.Value{
			Value: &storagepb.Value_Uint16Value{
				Uint16Value: uint32(v.Uint16Value()),
			},
		}, nil
	case types.UINT8:
		return &storagepb.Value{
			Value: &storagepb.Value_Uint8Value{
				Uint8Value: uint32(v.Uint8Value()),
			},
		}, nil
	case types.VARBINARY:
		return &storagepb.Value{
			Value: &storagepb.Value_VarbinaryValue{
				VarbinaryValue: v.VarbinaryValue(),
			},
		}, nil
	default:
		return nil, errors.New("invalid field type in build pb")
	}
}

type writeTuple struct {
	writeSeriesEntries map[string]*storagepb.WriteSeriesEntry // seriesKey -> entry
	orderedTags        orderedNames
	orderedFields      orderedNames
}

// for sort keys
// index grows linearly with the insertion order
type orderedNames struct {
	curIndex    int            // cur largest curIndex
	nameIndexes map[string]int // name -> curIndex
}

func (d *orderedNames) insert(name string) int {
	idx, ok := d.nameIndexes[name]
	if ok {
		return idx
	}
	idx = d.curIndex
	d.nameIndexes[name] = idx
	d.curIndex = idx + 1
	return idx
}

func (d *orderedNames) toOrdered() []string {
	if d.curIndex == 0 {
		return []string{}
	}

	order := make([]string, d.curIndex)
	for name, idx := range d.nameIndexes {
		order[idx] = name
	}
	return order
}
