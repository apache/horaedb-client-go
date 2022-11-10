// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ceresdb

import (
	"context"
	"github.com/CeresDB/ceresdb-client-go/types"
)

type CeresDBClient interface {
	Query(context.Context, types.QueryRequest) (types.QueryResponse, error)
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
