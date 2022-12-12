// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package utils

import (
	"errors"
	"fmt"

	"github.com/CeresDB/ceresdb-client-go/types"
	"github.com/CeresDB/ceresdbproto/golang/pkg/storagepb"
)

func GetMetricsFromRows(rows []*types.Row) []string {
	dict := make(map[string]byte)
	metrics := make([]string, 0, len(rows))
	for _, row := range rows {
		if _, ok := dict[row.Metric]; !ok {
			dict[row.Metric] = 0
			metrics = append(metrics, row.Metric)
		}
	}
	return metrics
}

func SplitRowsByRoute(rows []*types.Row, routes map[string]types.Route) (map[string][]*types.Row, error) {
	rowsByRoute := make(map[string][]*types.Row, len(routes))
	for _, row := range rows {
		route, ok := routes[row.Metric]
		if !ok {
			return nil, fmt.Errorf("route for metric %s not found", row.Metric)
		}
		if rows, ok := rowsByRoute[route.Endpoint]; ok {
			rowsByRoute[route.Endpoint] = append(rows, row)
		} else {
			rowsByRoute[route.Endpoint] = []*types.Row{row}
		}
	}

	return rowsByRoute, nil
}

func CombineWriteResponse(r1 types.WriteResponse, r2 types.WriteResponse) types.WriteResponse {
	r1.Success += r2.Success
	r1.Failed += r2.Failed
	return r1
}

func BuildPbWriteRequest(rows []*types.Row) (*storagepb.WriteRequest, error) {
	tuples := make(map[string]*writeTuple)

	for _, row := range rows {
		tuple, ok := tuples[row.Metric]
		if !ok {
			tuple = &writeTuple{
				writeMetric: storagepb.WriteMetric{
					Metric:  row.Metric,
					Entries: []*storagepb.WriteEntry{},
				},
				orderedTags:   orderedNames{nameIndexes: map[string]int{}},
				orderedFields: orderedNames{nameIndexes: map[string]int{}},
			}
			tuples[row.Metric] = tuple
		}

		writeEntry := &storagepb.WriteEntry{
			Tags:        make([]*storagepb.Tag, 0, len(row.Tags)),
			FieldGroups: make([]*storagepb.FieldGroup, 0, 1),
		}

		for tagK, tagV := range row.Tags {
			idx := tuple.orderedTags.insert(tagK)
			writeEntry.Tags = append(writeEntry.Tags, &storagepb.Tag{
				NameIndex: uint32(idx),
				Value: &storagepb.Value{
					Value: &storagepb.Value_StringValue{
						StringValue: tagV,
					},
				},
			})
		}

		fieldGroup := &storagepb.FieldGroup{
			Timestamp: row.Timestamp,
			Fields:    make([]*storagepb.Field, 0, len(row.Fields)),
		}
		for fieldK, fieldV := range row.Fields {
			idx := tuple.orderedFields.insert(fieldK)
			pbV, err := buildPbValue(fieldV)
			if err != nil {
				return nil, err
			}
			fieldGroup.Fields = append(fieldGroup.Fields, &storagepb.Field{
				NameIndex: uint32(idx),
				Value:     pbV,
			})
		}
		writeEntry.FieldGroups = []*storagepb.FieldGroup{fieldGroup}

		tuple.writeMetric.Entries = append(tuple.writeMetric.Entries, writeEntry)
	}

	writeRequest := &storagepb.WriteRequest{
		Metrics: make([]*storagepb.WriteMetric, 0, len(tuples)),
	}
	for _, tuple := range tuples {
		tuple.writeMetric.TagNames = tuple.orderedTags.toOrdered()
		tuple.writeMetric.FieldNames = tuple.orderedFields.toOrdered()
		writeRequest.Metrics = append(writeRequest.Metrics, &tuple.writeMetric)
	}
	return writeRequest, nil
}

func buildPbValue(value interface{}) (*storagepb.Value, error) {
	switch v := value.(type) {
	case bool:
		return &storagepb.Value{
			Value: &storagepb.Value_BoolValue{
				BoolValue: v,
			},
		}, nil
	case string:
		return &storagepb.Value{
			Value: &storagepb.Value_StringValue{
				StringValue: v,
			},
		}, nil
	case float64:
		return &storagepb.Value{
			Value: &storagepb.Value_Float64Value{
				Float64Value: v,
			},
		}, nil
	case float32:
		return &storagepb.Value{
			Value: &storagepb.Value_Float32Value{
				Float32Value: v,
			},
		}, nil
	case int64:
		return &storagepb.Value{
			Value: &storagepb.Value_Int64Value{
				Int64Value: v,
			},
		}, nil
	case int32:
		return &storagepb.Value{
			Value: &storagepb.Value_Int32Value{
				Int32Value: v,
			},
		}, nil
	case int16:
		return &storagepb.Value{
			Value: &storagepb.Value_Int16Value{
				Int16Value: int32(v),
			},
		}, nil
	case int8:
		return &storagepb.Value{
			Value: &storagepb.Value_Int8Value{
				Int8Value: int32(v),
			},
		}, nil
	case uint64:
		return &storagepb.Value{
			Value: &storagepb.Value_Uint64Value{
				Uint64Value: v,
			},
		}, nil
	case uint32:
		return &storagepb.Value{
			Value: &storagepb.Value_Uint32Value{
				Uint32Value: v,
			},
		}, nil
	case uint16:
		return &storagepb.Value{
			Value: &storagepb.Value_Uint16Value{
				Uint16Value: uint32(v),
			},
		}, nil
	case uint8:
		return &storagepb.Value{
			Value: &storagepb.Value_Uint8Value{
				Uint8Value: uint32(v),
			},
		}, nil
	default:
		return nil, errors.New("invalid field type in build pb")
	}
}

type writeTuple struct {
	writeMetric   storagepb.WriteMetric
	orderedTags   orderedNames
	orderedFields orderedNames
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
