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
	name      string
	dataType  DataType
	dataValue interface{}
	isNull    bool
}

func (v Value) Name() string {
	return v.name
}

func (v Value) GetDataType() DataType {
	return v.dataType
}

func (v Value) GetValue() interface{} {
	if v.isNull {
		return nil
	}
	return v.dataValue
}

func (v Value) IsNull() bool {
	return v.isNull
}
