// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ceresdb

import (
	"context"
	"github.com/CeresDB/ceresdb-client-go/types"
)

/*
null type data type support
https://github.com/CeresDB/ceresdb-client-go/issues/8
*/
type CeresDBClient interface {
	Query(context.Context, types.QueryRequest) (types.QueryResponse, error)
	// Note: Rows currently writing to the same timeline will be overwritten; this restriction will be removed shortly
	Write(context.Context, []*types.Row) (types.WriteResponse, error)
	Close() error
}

func NewClient(endpoint string, opts ...Option) (CeresDBClient, error) {
	dopts := defaultOptions()
	for _, opt := range opts {
		opt.apply(dopts)
	}
	return newClient(endpoint, dopts)
}
