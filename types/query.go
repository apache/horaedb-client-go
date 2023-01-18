// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package types

type SqlQueryRequest struct {
	Tables []string
	Sql    string
}

type SqlQueryResponse struct {
	Sql          string
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
