

package ceresdb

import (
	"context"
)

type RouteMode int

const (
	Direct RouteMode = iota
	Proxy
)

type Client interface {
	Write(context.Context, WriteRequest) (WriteResponse, error)
	SQLQuery(context.Context, SQLQueryRequest) (SQLQueryResponse, error)
}

func NewClient(endpoint string, routeMode RouteMode, opts ...Option) (Client, error) {
	defaultOpts := defaultOptions()
	for _, opt := range opts {
		opt.apply(defaultOpts)
	}
	return newClient(endpoint, routeMode, *defaultOpts)
}
