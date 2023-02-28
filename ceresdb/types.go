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

type Column struct {
	name  string
	value Value
}

func (c Column) Name() string {
	return c.name
}

func (c Column) Value() Value {
	return c.value
}

type Row struct {
	fields []string
	values []Value
}

func (r Row) HasColumn(name string) bool {
	return r.getColumnIdx(name) > -1
}

func (r Row) Column(name string) Column {
	if idx := r.getColumnIdx(name); idx > -1 {
		return Column{name, r.values[idx]}
	}
	return Column{}
}

func (r Row) Columns() []Column {
	columns := make([]Column, 0, len(r.values))
	for idx, field := range r.fields {
		columns = append(columns, Column{field, r.values[idx]})
	}
	return columns
}

func (r Row) getColumnIdx(name string) int {
	for idx, field := range r.fields {
		if field == name {
			return idx
		}
	}
	return -1
}
