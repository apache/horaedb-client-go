// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package types

import (
	"fmt"
)

const (
	avroTypeBool   = "boolean"
	avroTypeBytes  = "bytes"
	avroTypeDouble = "double"
	avroTypeFloat  = "float"
	avroTypeInt    = "int"
	avroTypeLong   = "long"
	avroTypeNull   = "null"
	avroTypeString = "string"
)

type QueryRequest struct {
	Metrics []string
	Ql      string
}

type QueryResponse struct {
	Ql       string
	RowCount uint32
	Rows     []map[string]interface{}
}

func (r *QueryResponse) MapToRecord() []Record {
	records := make([]Record, 0, len(r.Rows))
	for _, row := range r.Rows {
		records = append(records, Record{row})
	}
	return records
}

// avro implementation
type Record struct {
	record map[string]interface{}
}

func (r Record) HasField(field string) bool {
	_, ok := r.record[field]
	return ok
}

func (r Record) GetFieldCount() int {
	return len(r.record)
}

func (r Record) GetTimestamp() (int64, error) {
	return r.GetInt64("timestamp")
}

func (r Record) GetBool(field string) (bool, error) {
	v, err := r.get(field, avroTypeBool)
	if err != nil {
		return false, err
	}
	if vBool, ok := v.(bool); !ok {
		return false, fmt.Errorf("Not a bool field Type %s", field)
	} else {
		return vBool, nil
	}
}

func (r Record) GetString(field string) (string, error) {
	v, err := r.get(field, avroTypeString)
	if err != nil {
		return "", err
	}
	if vString, ok := v.(string); !ok {
		return "", fmt.Errorf("Not a string field Type %s", field)
	} else {
		return vString, nil
	}
}

func (r Record) GetFloat64(field string) (float64, error) {
	v, err := r.get(field, avroTypeDouble)
	if err != nil {
		return 0, err
	}
	if vFloat64, ok := v.(float64); !ok {
		return 0, fmt.Errorf("Not a float64 field Type %s", field)
	} else {
		return vFloat64, nil
	}
}

func (r Record) GetFloat32(field string) (float32, error) {
	v, err := r.get(field, avroTypeFloat)
	if err != nil {
		return 0, err
	}
	if vFloat32, ok := v.(float32); !ok {
		return 0, fmt.Errorf("Not a float32 field Type %s", field)
	} else {
		return vFloat32, nil
	}
}

// cast with int64
func (r Record) GetInt(field string) (int, error) {
	vInt64, err := r.GetInt64(field)
	if err != nil {
		return 0, err
	}
	return int(vInt64), err
}

func (r Record) GetInt64(field string) (int64, error) {
	v, err := r.get(field, avroTypeLong)
	if err != nil {
		return 0, err
	}
	if vInt64, ok := v.(int64); !ok {
		return 0, fmt.Errorf("Not a int64 field Type %s", field)
	} else {
		return vInt64, nil
	}
}

func (r Record) GetInt32(field string) (int32, error) {
	v, err := r.get(field, avroTypeInt)
	if err != nil {
		return 0, err
	}
	if vInt32, ok := v.(int32); !ok {
		return 0, fmt.Errorf("Not a int32 field Type %s", field)
	} else {
		return vInt32, nil
	}
}

func (r Record) GetInt16(field string) (int16, error) {
	// arvo pass int16 with int32
	vInt32, err := r.GetInt32(field)
	if err != nil {
		return 0, err
	}
	return int16(vInt32), err
}

func (r Record) GetInt8(field string) (int8, error) {
	// arvo pass int8 with int32
	vInt32, err := r.GetInt32(field)
	if err != nil {
		return 0, err
	}
	return int8(vInt32), err
}

// cast with uint64
func (r Record) GetUint(field string) (uint, error) {
	vUInt64, err := r.GetUInt64(field)
	if err != nil {
		return 0, err
	}
	return uint(vUInt64), err
}

func (r Record) GetUInt64(field string) (uint64, error) {
	// arvo pass uint64 with int64
	vInt64, err := r.GetInt64(field)
	if err != nil {
		return 0, err
	}
	return uint64(vInt64), err
}

func (r Record) GetUInt32(field string) (uint32, error) {
	// arvo pass uint32 with int64
	vInt64, err := r.GetInt64(field)
	if err != nil {
		return 0, err
	}
	return uint32(vInt64), err
}

func (r Record) GetUInt16(field string) (uint16, error) {
	// arvo pass uint16 with int32
	vInt32, err := r.GetInt32(field)
	if err != nil {
		return 0, err
	}
	return uint16(vInt32), err
}

func (r Record) GetUInt8(field string) (uint8, error) {
	// arvo pass uint8 with int32
	vInt32, err := r.GetInt32(field)
	if err != nil {
		return 0, err
	}
	return uint8(vInt32), err
}

func (r Record) get(field, type_ string) (interface{}, error) {
	v, ok := r.record[field]
	if !ok {
		return nil, fmt.Errorf("Not found field %s", field)
	}

	/*
		arvo type list will be unmarshal to map[string]interface{}
		exp:
			 {
				 "name":"t1",
				 "type":[
					"null",
					"string"
				]
			},
	**/
	mapV, ok := v.(map[string]interface{})
	if !ok {
		return v, nil
	}
	realV, ok := mapV[type_]
	if !ok {
		return nil, fmt.Errorf("Not valid field type %s:%s", field, type_)
	}
	return realV, nil
}
