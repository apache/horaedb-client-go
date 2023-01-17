// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package types

type QueryRequest struct {
	Metrics []string
	Ql      string
}

type QueryResponse struct {
	Ql       string
	RowCount uint32
	Rows     []Row
}

type Row struct {
	Values []Value
}
