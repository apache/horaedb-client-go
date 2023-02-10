// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package types

type WriteRequest struct {
	ReqCtx   RequestContext
	Database string
	Points   []Point
}

type WriteResponse struct {
	Success uint32
	Failed  uint32
}

type Point struct {
	Table     string
	Timestamp int64
	Tags      map[string]Value
	Fields    map[string]Value
}
