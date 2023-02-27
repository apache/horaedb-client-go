// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ceresdb

type RequestContext struct {
	Database string
}

type WriteRequest struct {
	ReqCtx RequestContext
	Points []Point
}

type WriteResponse struct {
	Success uint32
	Failed  uint32
}

type SQLQueryRequest struct {
	ReqCtx RequestContext
	Tables []string
	SQL    string
}

type SQLQueryResponse struct {
	SQL          string
	AffectedRows uint32
	Rows         []Row
}

type Row struct {
	Values map[string]Value
}

func (r Row) HasColumn(column string) bool {
	_, ok := r.Values[column]
	return ok
}

func (r Row) ColumnValue(column string) Value {
	if v, ok := r.Values[column]; ok {
		return v
	}
	return Value{}
}
