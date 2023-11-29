

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
	Message string
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
	_, ok := r.getColumnIdx(name)
	return ok
}

func (r Row) Column(name string) (Column, bool) {
	if idx, ok := r.getColumnIdx(name); ok {
		return Column{name, r.values[idx]}, true
	}
	return Column{}, false
}

func (r Row) Columns() []Column {
	columns := make([]Column, 0, len(r.values))
	for idx, field := range r.fields {
		columns = append(columns, Column{field, r.values[idx]})
	}
	return columns
}

func (r Row) getColumnIdx(name string) (int, bool) {
	for idx, field := range r.fields {
		if field == name {
			return idx, true
		}
	}
	return -1, false
}
