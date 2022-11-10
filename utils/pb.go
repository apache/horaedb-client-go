// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package utils

import (
	"errors"

	"github.com/CeresDB/ceresdb-client-go/types"
	"github.com/CeresDB/ceresdbproto/go/ceresdbproto"
)

func BuildRowsToPb(rows []*types.Row) (*ceresdbproto.WriteRequest, error) {
	tuples := make(map[string]*writeTuple)

	for _, row := range rows {
		tuple, ok := tuples[row.Metric]
		if !ok {
			tuple = &writeTuple{
				writeMetric: ceresdbproto.WriteMetric{
					Metric:  row.Metric,
					Entries: []*ceresdbproto.WriteEntry{},
				},
				tagDict:   nameDict{0, map[string]int{}},
				fieldDict: nameDict{0, map[string]int{}},
			}
			tuples[row.Metric] = tuple
		}

		writeEntry := &ceresdbproto.WriteEntry{
			Tags:        make([]*ceresdbproto.Tag, 0, len(row.Tags)),
			FieldGroups: make([]*ceresdbproto.FieldGroup, 0, 1),
		}

		for tagK, tagV := range row.Tags {
			idx := tuple.tagDict.insert(tagK)
			writeEntry.Tags = append(writeEntry.Tags, &ceresdbproto.Tag{
				NameIndex: uint32(idx),
				Value: &ceresdbproto.Value{
					Value: &ceresdbproto.Value_StringValue{
						StringValue: tagV,
					},
				},
			})
		}

		fieldGroup := &ceresdbproto.FieldGroup{
			Timestamp: row.Timestamp,
			Fields:    make([]*ceresdbproto.Field, 0, len(row.Fields)),
		}
		for fieldK, fieldV := range row.Fields {
			idx := tuple.fieldDict.insert(fieldK)
			pbV, err := buildPbValue(fieldV)
			if err != nil {
				return nil, err
			}
			fieldGroup.Fields = append(fieldGroup.Fields, &ceresdbproto.Field{
				NameIndex: uint32(idx),
				Value:     pbV,
			})
		}
		writeEntry.FieldGroups = []*ceresdbproto.FieldGroup{fieldGroup}

		tuple.writeMetric.Entries = append(tuple.writeMetric.Entries, writeEntry)
	}

	writeRequest := &ceresdbproto.WriteRequest{
		Metrics: make([]*ceresdbproto.WriteMetric, 0, len(tuples)),
	}
	for _, tuple := range tuples {
		tuple.writeMetric.TagNames = tuple.tagDict.toOrdered()
		tuple.writeMetric.FieldNames = tuple.fieldDict.toOrdered()
		writeRequest.Metrics = append(writeRequest.Metrics, &tuple.writeMetric)
	}
	return writeRequest, nil
}

func buildPbValue(v interface{}) (*ceresdbproto.Value, error) {
	switch v.(type) {
	case bool:
		return &ceresdbproto.Value{
			Value: &ceresdbproto.Value_BoolValue{
				v.(bool),
			},
		}, nil
	case string:
		return &ceresdbproto.Value{
			Value: &ceresdbproto.Value_StringValue{
				v.(string),
			},
		}, nil
	case float64:
		return &ceresdbproto.Value{
			Value: &ceresdbproto.Value_Float64Value{
				v.(float64),
			},
		}, nil
	case float32:
		return &ceresdbproto.Value{
			Value: &ceresdbproto.Value_Float32Value{
				v.(float32),
			},
		}, nil
	case int64:
		return &ceresdbproto.Value{
			Value: &ceresdbproto.Value_Int64Value{
				v.(int64),
			},
		}, nil
	case int32:
		return &ceresdbproto.Value{
			Value: &ceresdbproto.Value_Int32Value{
				v.(int32),
			},
		}, nil
	case int16:
		return &ceresdbproto.Value{
			Value: &ceresdbproto.Value_Int16Value{
				int32(v.(int16)),
			},
		}, nil
	case int8:
		return &ceresdbproto.Value{
			Value: &ceresdbproto.Value_Int8Value{
				int32(v.(int8)),
			},
		}, nil
	case uint64:
		return &ceresdbproto.Value{
			Value: &ceresdbproto.Value_Uint64Value{
				v.(uint64),
			},
		}, nil
	case uint32:
		return &ceresdbproto.Value{
			Value: &ceresdbproto.Value_Uint32Value{
				v.(uint32),
			},
		}, nil
	case uint16:
		return &ceresdbproto.Value{
			Value: &ceresdbproto.Value_Uint16Value{
				uint32(v.(uint16)),
			},
		}, nil
	case uint8:
		return &ceresdbproto.Value{
			Value: &ceresdbproto.Value_Uint8Value{
				uint32(v.(uint8)),
			},
		}, nil
	default:
		return nil, errors.New("invalid field type in build pb")
	}
}

type writeTuple struct {
	writeMetric ceresdbproto.WriteMetric
	tagDict     nameDict
	fieldDict   nameDict
}

// for sort keys
type nameDict struct {
	curIndex    int            // cur largest curIndex
	nameIndexes map[string]int // name -> curIndex
}

func (d *nameDict) insert(name string) int {
	if idx, ok := d.nameIndexes[name]; ok {
		return idx
	} else {
		idx := d.curIndex
		d.nameIndexes[name] = idx
		d.curIndex = idx + 1
		return idx
	}
}

func (d *nameDict) toOrdered() []string {
	if d.curIndex == 0 {
		return []string{}
	}

	order := make([]string, d.curIndex)
	for name, idx := range d.nameIndexes {
		order[idx] = name
	}
	return order
}
