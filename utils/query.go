// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package utils

import (
	"bytes"
	"io"

	"github.com/CeresDB/ceresdb-client-go/types"
	"github.com/CeresDB/ceresdbproto/golang/pkg/storagepb"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/klauspost/compress/zstd"
)

func ParseQueryResponse(response *storagepb.SqlQueryResponse) ([]types.Row, error) {
	arrowPayload, ok := response.Output.(*storagepb.SqlQueryResponse_Arrow)
	if !ok {
		return nil, types.ErrOnlyArrowSupport
	}
	if len(arrowPayload.Arrow.RecordBatches) == 0 {
		return nil, types.ErrNullRows
	}

	rowCount := 0
	rowBatches := make([][]types.Row, 0, len(arrowPayload.Arrow.RecordBatches))
	for _, batch := range arrowPayload.Arrow.RecordBatches {
		buffer := io.Reader(bytes.NewReader(batch))

		if arrowPayload.Arrow.Compression == storagepb.ArrowPayload_ZSTD {
			zstdReader, err := zstd.NewReader(buffer)
			if err != nil {
				return nil, err
			}
			buffer = zstdReader
		}

		reader, err := ipc.NewReader(buffer)
		if err != nil {
			return nil, err
		}
		schema := reader.Schema()
		for reader.Next() {
			record := reader.Record()
			rowsBatch := convertArrowRecordToRow(schema, record)
			rowCount += len(rowsBatch)
			rowBatches = append(rowBatches, rowsBatch)
		}
		reader.Release()
	}

	rows := make([]types.Row, 0, rowCount)
	for _, rowBatch := range rowBatches {
		rows = append(rows, rowBatch...)
	}

	return rows, nil
}

func convertArrowRecordToRow(schema *arrow.Schema, record array.Record) []types.Row {
	rows := make([]types.Row, record.NumRows())
	for rowIdx := range rows {
		rows[rowIdx] = types.Row{
			Values: make(map[string]types.Value, record.NumCols()),
		}
	}

	for colIdx, field := range schema.Fields() {
		column := record.Column(colIdx)
		switch column.DataType().ID() {
		case arrow.STRING:
			colString, _ := column.(*array.String)
			for rowIdx := 0; rowIdx < colString.Len(); rowIdx++ {
				if colString.IsNull(rowIdx) {
					rows[rowIdx].Values[field.Name] = types.NewStringNullValue()
				} else {
					rows[rowIdx].Values[field.Name] = types.NewStringValue(colString.Value(rowIdx))
				}
			}
		case arrow.FLOAT64:
			colFloat64, _ := column.(*array.Float64)
			for rowIdx := 0; rowIdx < colFloat64.Len(); rowIdx++ {
				if colFloat64.IsNull(rowIdx) {
					rows[rowIdx].Values[field.Name] = types.NewDoubleNullValue()
				} else {
					rows[rowIdx].Values[field.Name] = types.NewDoubleValue(colFloat64.Value(rowIdx))
				}
			}
		case arrow.FLOAT32:
			colFloat32, _ := column.(*array.Float32)
			for rowIdx := 0; rowIdx < colFloat32.Len(); rowIdx++ {
				if colFloat32.IsNull(rowIdx) {
					rows[rowIdx].Values[field.Name] = types.NewFloatNullValue()
				} else {
					rows[rowIdx].Values[field.Name] = types.NewFloatValue(colFloat32.Value(rowIdx))
				}
			}
		case arrow.INT64:
			colInt64, _ := column.(*array.Int64)
			for rowIdx := 0; rowIdx < colInt64.Len(); rowIdx++ {
				if colInt64.IsNull(rowIdx) {
					rows[rowIdx].Values[field.Name] = types.NewInt64NullValue()
				} else {
					rows[rowIdx].Values[field.Name] = types.NewInt64Value(colInt64.Value(rowIdx))
				}
			}
		case arrow.INT32:
			colInt32, _ := column.(*array.Int32)
			for rowIdx := 0; rowIdx < colInt32.Len(); rowIdx++ {
				if colInt32.IsNull(rowIdx) {
					rows[rowIdx].Values[field.Name] = types.NewInt32NullValue()
				} else {
					rows[rowIdx].Values[field.Name] = types.NewInt32Value(colInt32.Value(rowIdx))
				}
			}
		case arrow.INT16:
			colInt16, _ := column.(*array.Int16)
			for rowIdx := 0; rowIdx < colInt16.Len(); rowIdx++ {
				if colInt16.IsNull(rowIdx) {
					rows[rowIdx].Values[field.Name] = types.NewInt16NullValue()
				} else {
					rows[rowIdx].Values[field.Name] = types.NewInt16Value(colInt16.Value(rowIdx))
				}
			}
		case arrow.INT8:
			colInt8, _ := column.(*array.Int8)
			for rowIdx := 0; rowIdx < colInt8.Len(); rowIdx++ {
				if colInt8.IsNull(rowIdx) {
					rows[rowIdx].Values[field.Name] = types.NewInt8NullValue()
				} else {
					rows[rowIdx].Values[field.Name] = types.NewInt8Value(colInt8.Value(rowIdx))
				}
			}
		case arrow.UINT64:
			colUint64, _ := column.(*array.Uint64)
			for rowIdx := 0; rowIdx < colUint64.Len(); rowIdx++ {
				if colUint64.IsNull(rowIdx) {
					rows[rowIdx].Values[field.Name] = types.NewUint64NullValue()
				} else {
					rows[rowIdx].Values[field.Name] = types.NewUint64Value(colUint64.Value(rowIdx))
				}
			}
		case arrow.UINT32:
			colUint32, _ := column.(*array.Uint32)
			for rowIdx := 0; rowIdx < colUint32.Len(); rowIdx++ {
				if colUint32.IsNull(rowIdx) {
					rows[rowIdx].Values[field.Name] = types.NewUint32NullValue()
				} else {
					rows[rowIdx].Values[field.Name] = types.NewUint32Value(colUint32.Value(rowIdx))
				}
			}
		case arrow.UINT16:
			colUint16, _ := column.(*array.Uint16)
			for rowIdx := 0; rowIdx < colUint16.Len(); rowIdx++ {
				if colUint16.IsNull(rowIdx) {
					rows[rowIdx].Values[field.Name] = types.NewUint16NullValue()
				} else {
					rows[rowIdx].Values[field.Name] = types.NewUint16Value(colUint16.Value(rowIdx))
				}
			}
		case arrow.UINT8:
			colUint8, _ := column.(*array.Uint8)
			for rowIdx := 0; rowIdx < colUint8.Len(); rowIdx++ {
				if colUint8.IsNull(rowIdx) {
					rows[rowIdx].Values[field.Name] = types.NewUint8NullValue()
				} else {
					rows[rowIdx].Values[field.Name] = types.NewUint8Value(colUint8.Value(rowIdx))
				}
			}
		case arrow.BOOL:
			colBool, _ := column.(*array.Boolean)
			for rowIdx := 0; rowIdx < colBool.Len(); rowIdx++ {
				if colBool.IsNull(rowIdx) {
					rows[rowIdx].Values[field.Name] = types.NewBoolNullValue()
				} else {
					rows[rowIdx].Values[field.Name] = types.NewBoolValue(colBool.Value(rowIdx))
				}
			}
		case arrow.BINARY:
			colBinary, _ := column.(*array.Binary)
			for rowIdx := 0; rowIdx < colBinary.Len(); rowIdx++ {
				if colBinary.IsNull(rowIdx) {
					rows[rowIdx].Values[field.Name] = types.NewVarbinaryNullValue()
				} else {
					rows[rowIdx].Values[field.Name] = types.NewVarbinaryValue(colBinary.Value(rowIdx))
				}
			}
		case arrow.TIMESTAMP:
			colTimestamp, _ := column.(*array.Timestamp)
			for rowIdx := 0; rowIdx < colTimestamp.Len(); rowIdx++ {
				if colTimestamp.IsNull(rowIdx) {
					rows[rowIdx].Values[field.Name] = types.NewInt64NullValue()
				} else {
					rows[rowIdx].Values[field.Name] = types.NewInt64Value(int64(colTimestamp.Value(rowIdx)))
				}
			}
		default:
			//
		}
	}

	return rows
}
