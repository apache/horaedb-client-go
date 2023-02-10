// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ceresdb

import (
	"context"

	"github.com/CeresDB/ceresdb-client-go/types"
)

type Client interface {
	Write(context.Context, types.WriteRequest) (types.WriteResponse, error)
	SQLQuery(context.Context, types.SQLQueryRequest) (types.SQLQueryResponse, error)
}

func NewClient(endpoint string, routeMode types.RouteMode, opts ...Option) (Client, error) {
	defaultOpts := defaultOptions()
	for _, opt := range opts {
		opt.apply(defaultOpts)
	}
	return newClient(endpoint, routeMode, *defaultOpts)
}
