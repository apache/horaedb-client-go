// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package types

type Series struct {
	Metric string
	Tags   map[string]string
}

type Row struct {
	Series
	Timestamp int64
	Fields    map[string]interface{}
}
