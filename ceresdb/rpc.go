

package ceresdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/CeresDB/ceresdbproto/golang/pkg/storagepb"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type rpcClient struct {
	opts     options
	mutex    sync.Mutex // protect grpc conn init
	connPool sync.Map   // endpoint -> *grpc.ClientConn
}

func newRPCClient(opts options) *rpcClient {
	return &rpcClient{
		opts:     opts,
		connPool: sync.Map{},
	}
}

func (c *rpcClient) SQLQuery(ctx context.Context, endpoint string, req SQLQueryRequest) (SQLQueryResponse, error) {
	grpcConn, err := c.getGrpcConn(endpoint)
	if err != nil {
		return SQLQueryResponse{}, err
	}

	grpcClient := storagepb.NewStorageServiceClient(grpcConn)
	queryRequest := &storagepb.SqlQueryRequest{
		Context: &storagepb.RequestContext{
			Database: req.ReqCtx.Database,
		},
		Tables: req.Tables,
		Sql:    req.SQL,
	}
	queryResponse, err := grpcClient.SqlQuery(ctx, queryRequest)
	if err != nil {
		return SQLQueryResponse{}, err
	}

	if queryResponse.Header == nil {
		return SQLQueryResponse{}, &Error{
			Code: codeInternal,
			Err:  ErrResponseHeaderMiss.Error(),
		}
	}

	if queryResponse.Header.Code != codeSuccess {
		return SQLQueryResponse{}, &Error{
			Code: queryResponse.Header.Code,
			Err:  queryResponse.Header.Error,
		}
	}

	if affectedPayload, ok := queryResponse.Output.(*storagepb.SqlQueryResponse_AffectedRows); ok {
		return SQLQueryResponse{
			SQL:          req.SQL,
			AffectedRows: affectedPayload.AffectedRows,
		}, nil
	}

	rows, err := parseQueryResponse(queryResponse)
	if err != nil {
		return SQLQueryResponse{}, err
	}

	return SQLQueryResponse{
		SQL:          req.SQL,
		AffectedRows: queryResponse.GetAffectedRows(),
		Rows:         rows,
	}, nil
}

func (c *rpcClient) Write(ctx context.Context, endpoint string, reqCtx RequestContext, points []Point) (WriteResponse, error) {
	grpcConn, err := c.getGrpcConn(endpoint)
	if err != nil {
		return WriteResponse{}, err
	}

	grpcClient := storagepb.NewStorageServiceClient(grpcConn)
	writeRequest, err := buildPbWriteRequest(points)
	if err != nil {
		return WriteResponse{}, err
	}

	writeRequest.Context = &storagepb.RequestContext{
		Database: reqCtx.Database,
	}
	writeResponse, err := grpcClient.Write(ctx, writeRequest)
	if err != nil {
		return WriteResponse{}, err
	}

	if writeResponse.Header == nil {
		return WriteResponse{}, &Error{
			Code: codeInternal,
			Err:  ErrResponseHeaderMiss.Error(),
		}
	}

	if writeResponse.Header.Code != codeSuccess {
		return WriteResponse{}, &Error{
			Code: writeResponse.Header.Code,
			Err:  writeResponse.Header.Error,
		}
	}

	return WriteResponse{
		Success: writeResponse.Success,
		Failed:  writeResponse.Failed,
	}, nil
}

func (c *rpcClient) Route(endpoint string, reqCtx RequestContext, tables []string) (map[string]route, error) {
	grpcConn, err := c.getGrpcConn(endpoint)
	if err != nil {
		return nil, err
	}
	grpcClient := storagepb.NewStorageServiceClient(grpcConn)

	routeRequest := &storagepb.RouteRequest{
		Context: &storagepb.RequestContext{
			Database: reqCtx.Database,
		},
		Tables: tables,
	}
	routeResponse, err := grpcClient.Route(context.Background(), routeRequest)
	if err != nil {
		return nil, err
	}

	if routeResponse.Header == nil {
		return nil, &Error{
			Code: codeInternal,
			Err:  ErrResponseHeaderMiss.Error(),
		}
	}

	if routeResponse.Header.Code != codeSuccess {
		return nil, &Error{
			Code: routeResponse.Header.Code,
			Err:  routeResponse.Header.Error,
		}
	}

	routes := make(map[string]route, len(routeResponse.Routes))
	for _, r := range routeResponse.Routes {
		if r.Endpoint == nil {
			continue
		}

		routes[r.Table] = route{
			Table:    r.Table,
			Endpoint: fmt.Sprintf("%s:%d", r.Endpoint.Ip, r.Endpoint.Port),
		}
	}
	return routes, nil
}

func (c *rpcClient) getGrpcConn(endpoint string) (*grpc.ClientConn, error) {
	if conn, ok := c.connPool.Load(endpoint); ok {
		return conn.(*grpc.ClientConn), nil
	}

	return c.newGrpcConn(endpoint)
}

func (c *rpcClient) newGrpcConn(endpoint string) (*grpc.ClientConn, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if conn, ok := c.connPool.Load(endpoint); ok {
		return conn.(*grpc.ClientConn), nil
	}

	conn, err := grpc.Dial(endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(c.opts.RPCMaxRecvMsgSize)))
	if err != nil {
		return nil, err
	}

	c.connPool.Store(endpoint, conn)
	return conn, nil
}

func buildPbWriteRequest(points []Point) (*storagepb.WriteRequest, error) {
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

func buildPbValue(v Value) (*storagepb.Value, error) {
	switch v.DataType() {
	case BOOL:
		return &storagepb.Value{
			Value: &storagepb.Value_BoolValue{
				BoolValue: v.BoolValue(),
			},
		}, nil
	case STRING:
		return &storagepb.Value{
			Value: &storagepb.Value_StringValue{
				StringValue: v.StringValue(),
			},
		}, nil
	case DOUBLE:
		return &storagepb.Value{
			Value: &storagepb.Value_Float64Value{
				Float64Value: v.DoubleValue(),
			},
		}, nil
	case FLOAT:
		return &storagepb.Value{
			Value: &storagepb.Value_Float32Value{
				Float32Value: v.FloatValue(),
			},
		}, nil
	case INT64:
		return &storagepb.Value{
			Value: &storagepb.Value_Int64Value{
				Int64Value: v.Int64Value(),
			},
		}, nil
	case INT32:
		return &storagepb.Value{
			Value: &storagepb.Value_Int32Value{
				Int32Value: v.Int32Value(),
			},
		}, nil
	case INT16:
		return &storagepb.Value{
			Value: &storagepb.Value_Int16Value{
				Int16Value: int32(v.Int16Value()),
			},
		}, nil
	case INT8:
		return &storagepb.Value{
			Value: &storagepb.Value_Int8Value{
				Int8Value: int32(v.Int8Value()),
			},
		}, nil
	case UINT64:
		return &storagepb.Value{
			Value: &storagepb.Value_Uint64Value{
				Uint64Value: v.Uint64Value(),
			},
		}, nil
	case UINT32:
		return &storagepb.Value{
			Value: &storagepb.Value_Uint32Value{
				Uint32Value: v.Uint32Value(),
			},
		}, nil
	case UINT16:
		return &storagepb.Value{
			Value: &storagepb.Value_Uint16Value{
				Uint16Value: uint32(v.Uint16Value()),
			},
		}, nil
	case UINT8:
		return &storagepb.Value{
			Value: &storagepb.Value_Uint8Value{
				Uint8Value: uint32(v.Uint8Value()),
			},
		}, nil
	case VARBINARY:
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

func parseQueryResponse(response *storagepb.SqlQueryResponse) ([]Row, error) {
	arrowPayload, ok := response.Output.(*storagepb.SqlQueryResponse_Arrow)
	if !ok {
		return nil, ErrOnlyArrowSupport
	}
	if len(arrowPayload.Arrow.RecordBatches) == 0 {
		return nil, ErrNullRows
	}

	rowCount := 0
	rowBatches := make([][]Row, 0, len(arrowPayload.Arrow.RecordBatches))
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

	rows := make([]Row, 0, rowCount)
	for _, rowBatch := range rowBatches {
		rows = append(rows, rowBatch...)
	}

	return rows, nil
}

func convertArrowRecordToRow(schema *arrow.Schema, record array.Record) []Row {
	rows := make([]Row, record.NumRows())
	for rowIdx := range rows {
		rows[rowIdx] = Row{
			values: make([]Value, record.NumCols()),
		}
	}

	fields := make([]string, len(schema.Fields()))
	for colIdx, field := range schema.Fields() {
		fields[colIdx] = field.Name
		column := record.Column(colIdx)
		switch column.DataType().ID() {
		case arrow.STRING:
			colString := column.(*array.String)
			for rowIdx := 0; rowIdx < colString.Len(); rowIdx++ {
				if colString.IsNull(rowIdx) {
					rows[rowIdx].values[colIdx] = NewStringNullValue()
				} else {
					rows[rowIdx].values[colIdx] = NewStringValue(colString.Value(rowIdx))
				}
			}
		case arrow.FLOAT64:
			colFloat64 := column.(*array.Float64)
			for rowIdx := 0; rowIdx < colFloat64.Len(); rowIdx++ {
				if colFloat64.IsNull(rowIdx) {
					rows[rowIdx].values[colIdx] = NewDoubleNullValue()
				} else {
					rows[rowIdx].values[colIdx] = NewDoubleValue(colFloat64.Value(rowIdx))
				}
			}
		case arrow.FLOAT32:
			colFloat32 := column.(*array.Float32)
			for rowIdx := 0; rowIdx < colFloat32.Len(); rowIdx++ {
				if colFloat32.IsNull(rowIdx) {
					rows[rowIdx].values[colIdx] = NewFloatNullValue()
				} else {
					rows[rowIdx].values[colIdx] = NewFloatValue(colFloat32.Value(rowIdx))
				}
			}
		case arrow.INT64:
			colInt64 := column.(*array.Int64)
			for rowIdx := 0; rowIdx < colInt64.Len(); rowIdx++ {
				if colInt64.IsNull(rowIdx) {
					rows[rowIdx].values[colIdx] = NewInt64NullValue()
				} else {
					rows[rowIdx].values[colIdx] = NewInt64Value(colInt64.Value(rowIdx))
				}
			}
		case arrow.INT32:
			colInt32 := column.(*array.Int32)
			for rowIdx := 0; rowIdx < colInt32.Len(); rowIdx++ {
				if colInt32.IsNull(rowIdx) {
					rows[rowIdx].values[colIdx] = NewInt32NullValue()
				} else {
					rows[rowIdx].values[colIdx] = NewInt32Value(colInt32.Value(rowIdx))
				}
			}
		case arrow.INT16:
			colInt16 := column.(*array.Int16)
			for rowIdx := 0; rowIdx < colInt16.Len(); rowIdx++ {
				if colInt16.IsNull(rowIdx) {
					rows[rowIdx].values[colIdx] = NewInt16NullValue()
				} else {
					rows[rowIdx].values[colIdx] = NewInt16Value(colInt16.Value(rowIdx))
				}
			}
		case arrow.INT8:
			colInt8 := column.(*array.Int8)
			for rowIdx := 0; rowIdx < colInt8.Len(); rowIdx++ {
				if colInt8.IsNull(rowIdx) {
					rows[rowIdx].values[colIdx] = NewInt8NullValue()
				} else {
					rows[rowIdx].values[colIdx] = NewInt8Value(colInt8.Value(rowIdx))
				}
			}
		case arrow.UINT64:
			colUint64 := column.(*array.Uint64)
			for rowIdx := 0; rowIdx < colUint64.Len(); rowIdx++ {
				if colUint64.IsNull(rowIdx) {
					rows[rowIdx].values[colIdx] = NewUint64NullValue()
				} else {
					rows[rowIdx].values[colIdx] = NewUint64Value(colUint64.Value(rowIdx))
				}
			}
		case arrow.UINT32:
			colUint32 := column.(*array.Uint32)
			for rowIdx := 0; rowIdx < colUint32.Len(); rowIdx++ {
				if colUint32.IsNull(rowIdx) {
					rows[rowIdx].values[colIdx] = NewUint32NullValue()
				} else {
					rows[rowIdx].values[colIdx] = NewUint32Value(colUint32.Value(rowIdx))
				}
			}
		case arrow.UINT16:
			colUint16 := column.(*array.Uint16)
			for rowIdx := 0; rowIdx < colUint16.Len(); rowIdx++ {
				if colUint16.IsNull(rowIdx) {
					rows[rowIdx].values[colIdx] = NewUint16NullValue()
				} else {
					rows[rowIdx].values[colIdx] = NewUint16Value(colUint16.Value(rowIdx))
				}
			}
		case arrow.UINT8:
			colUint8 := column.(*array.Uint8)
			for rowIdx := 0; rowIdx < colUint8.Len(); rowIdx++ {
				if colUint8.IsNull(rowIdx) {
					rows[rowIdx].values[colIdx] = NewUint8NullValue()
				} else {
					rows[rowIdx].values[colIdx] = NewUint8Value(colUint8.Value(rowIdx))
				}
			}
		case arrow.BOOL:
			colBool := column.(*array.Boolean)
			for rowIdx := 0; rowIdx < colBool.Len(); rowIdx++ {
				if colBool.IsNull(rowIdx) {
					rows[rowIdx].values[colIdx] = NewBoolNullValue()
				} else {
					rows[rowIdx].values[colIdx] = NewBoolValue(colBool.Value(rowIdx))
				}
			}
		case arrow.BINARY:
			colBinary := column.(*array.Binary)
			for rowIdx := 0; rowIdx < colBinary.Len(); rowIdx++ {
				if colBinary.IsNull(rowIdx) {
					rows[rowIdx].values[colIdx] = NewVarbinaryNullValue()
				} else {
					rows[rowIdx].values[colIdx] = NewVarbinaryValue(colBinary.Value(rowIdx))
				}
			}
		case arrow.TIMESTAMP:
			colTimestamp := column.(*array.Timestamp)
			for rowIdx := 0; rowIdx < colTimestamp.Len(); rowIdx++ {
				if colTimestamp.IsNull(rowIdx) {
					rows[rowIdx].values[colIdx] = NewInt64NullValue()
				} else {
					rows[rowIdx].values[colIdx] = NewInt64Value(int64(colTimestamp.Value(rowIdx)))
				}
			}
		default:
		}
	}

	for rowIdx := range rows {
		rows[rowIdx].fields = fields
	}

	return rows
}
