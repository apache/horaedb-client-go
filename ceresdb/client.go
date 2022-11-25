// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ceresdb

import (
	"context"

	"github.com/CeresDB/ceresdb-client-go/types"
)

type Client interface {
	Query(context.Context, types.QueryRequest) (types.QueryResponse, error)
	// Note: Rows currently writing to the same timeline will be overwritten, this restriction will be removed shortly.
	Write(context.Context, []*types.Row) (types.WriteResponse, error)
}

func NewClient(endpoint string, opts ...Option) (Client, error) {
	defaultOpts := defaultOptions()
	for _, opt := range opts {
		opt.apply(defaultOpts)
	}
	return newClient(endpoint, *defaultOpts)
}
