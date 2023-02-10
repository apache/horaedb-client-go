// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package types

type DataType int

const (
	NULL DataType = iota
	TIMESTAMP
	STRING
	DOUBLE
	FLOAT
	INT64
	INT32
	INT16
	INT8
	UINT64
	UINT32
	UINT16
	UINT8
	BOOL
	VARBINARY
)

type Value struct {
	dataType  DataType
	dataValue interface{}
}

func (v Value) DataType() DataType {
	return v.dataType
}

func (v Value) Value() interface{} {
	return v.dataValue
}

func (v Value) IsNull() bool {
	return v.dataValue == nil
}

func (v Value) TimestampValue() int64 {
	return v.Int64Value()
}

func (v Value) StringValue() string {
	if v.IsNull() {
		return ""
	}
	return v.dataValue.(string)
}

func (v Value) DoubleValue() float64 {
	if v.IsNull() {
		return 0
	}
	return v.dataValue.(float64)
}

func (v Value) FloatValue() float32 {
	if v.IsNull() {
		return 0
	}
	return v.dataValue.(float32)
}

func (v Value) Int64Value() int64 {
	if v.IsNull() {
		return 0
	}
	return v.dataValue.(int64)
}

func (v Value) Int32Value() int32 {
	if v.IsNull() {
		return 0
	}
	return v.dataValue.(int32)
}

func (v Value) Int16Value() int16 {
	if v.IsNull() {
		return 0
	}
	return v.dataValue.(int16)
}

func (v Value) Int8Value() int8 {
	if v.IsNull() {
		return 0
	}
	return v.dataValue.(int8)
}

func (v Value) Uint64Value() uint64 {
	if v.IsNull() {
		return 0
	}
	return v.dataValue.(uint64)
}

func (v Value) Uint32Value() uint32 {
	if v.IsNull() {
		return 0
	}
	return v.dataValue.(uint32)
}

func (v Value) Uint16Value() uint16 {
	if v.IsNull() {
		return 0
	}
	return v.dataValue.(uint16)
}

func (v Value) Uint8Value() uint8 {
	if v.IsNull() {
		return 0
	}
	return v.dataValue.(uint8)
}

func (v Value) BoolValue() bool {
	if v.IsNull() {
		return false
	}
	return v.dataValue.(bool)
}

func (v Value) VarbinaryValue() []byte {
	if v.IsNull() {
		return []byte{}
	}
	return v.dataValue.([]byte)
}

func NewStringValue(v string) Value {
	return Value{
		dataType:  STRING,
		dataValue: v,
	}
}

func NewStringNullValue() Value {
	return Value{
		dataType: STRING,
	}
}

func NewDoubleValue(v float64) Value {
	return Value{
		dataType:  DOUBLE,
		dataValue: v,
	}
}

func NewDoubleNullValue() Value {
	return Value{
		dataType: DOUBLE,
	}
}

func NewFloatValue(v float32) Value {
	return Value{
		dataType:  FLOAT,
		dataValue: v,
	}
}

func NewFloatNullValue() Value {
	return Value{
		dataType: FLOAT,
	}
}

func NewInt64Value(v int64) Value {
	return Value{
		dataType:  INT64,
		dataValue: v,
	}
}

func NewInt64NullValue() Value {
	return Value{
		dataType: INT64,
	}
}

func NewInt32Value(v int32) Value {
	return Value{
		dataType:  INT32,
		dataValue: v,
	}
}

func NewInt32NullValue() Value {
	return Value{
		dataType: INT32,
	}
}

func NewInt16Value(v int16) Value {
	return Value{
		dataType:  INT16,
		dataValue: v,
	}
}

func NewInt16NullValue() Value {
	return Value{
		dataType: INT16,
	}
}

func NewInt8Value(v int8) Value {
	return Value{
		dataType:  INT8,
		dataValue: v,
	}
}

func NewInt8NullValue() Value {
	return Value{
		dataType: INT8,
	}
}

func NewUint64Value(v uint64) Value {
	return Value{
		dataType:  UINT64,
		dataValue: v,
	}
}

func NewUint64NullValue() Value {
	return Value{
		dataType: UINT64,
	}
}

func NewUint32Value(v uint32) Value {
	return Value{
		dataType:  UINT32,
		dataValue: v,
	}
}

func NewUint32NullValue() Value {
	return Value{
		dataType: UINT32,
	}
}

func NewUint16Value(v uint16) Value {
	return Value{
		dataType:  UINT16,
		dataValue: v,
	}
}

func NewUint16NullValue() Value {
	return Value{
		dataType: UINT16,
	}
}

func NewUint8Value(v uint8) Value {
	return Value{
		dataType:  UINT8,
		dataValue: v,
	}
}

func NewUint8NullValue() Value {
	return Value{
		dataType: UINT8,
	}
}

func NewBoolValue(v bool) Value {
	return Value{
		dataType:  BOOL,
		dataValue: v,
	}
}

func NewBoolNullValue() Value {
	return Value{
		dataType: BOOL,
	}
}

func NewVarbinaryValue(v []byte) Value {
	return Value{
		dataType:  VARBINARY,
		dataValue: v,
	}
}

func NewVarbinaryNullValue() Value {
	return Value{
		dataType: VARBINARY,
	}
}
