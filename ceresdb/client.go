// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ceresdb

import (
	"context"

	"github.com/CeresDB/ceresdb-client-go/types"
)

type Client interface {
	Query(context.Context, types.QueryRequest) (types.QueryResponse, error)
	Write(context.Context, types.WriteRequest) (types.WriteResponse, error)
}

func NewClient(endpoint string, opts ...Option) (Client, error) {
	defaultOpts := defaultOptions()
	for _, opt := range opts {
		opt.apply(defaultOpts)
	}
	return newClient(endpoint, *defaultOpts)
}
