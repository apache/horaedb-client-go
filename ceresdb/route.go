// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ceresdb

import (
	"fmt"

	"github.com/CeresDB/ceresdb-client-go/types"
	lru "github.com/hashicorp/golang-lru"
)

type RouteClient interface {
	RouteFor(types.RequestContext, []string) (map[string]types.Route, error)
	ClearRouteFor([]string)
}

func newRouteClient(endpoint string, routeMode types.RouteMode, rpcClient *rpcClient, opts options) (RouteClient, error) {
	switch routeMode {
	case types.Direct:
		routeClient := &directRouteClient{
			opts:      opts,
			endpoint:  endpoint,
			rpcClient: rpcClient,
		}
		routeCache, err := lru.NewWithEvict(opts.RouteMaxCacheSize, routeClient.OnEvict)
		if err != nil {
			return nil, err
		}
		routeClient.routeCache = routeCache
		return routeClient, nil
	case types.Proxy:
		routeClient := &proxyRouteClient{
			endpoint:  endpoint,
			rpcClient: rpcClient,
		}
		return routeClient, nil
	default:
		return nil, fmt.Errorf("invalid arguments routeMode with %v", routeMode)
	}
}

type directRouteClient struct {
	opts       options
	endpoint   string
	rpcClient  *rpcClient
	routeCache *lru.Cache // table -> *Route
}

func (c *directRouteClient) RouteFor(reqCtx types.RequestContext, tables []string) (map[string]types.Route, error) {
	if len(tables) == 0 {
		return nil, types.ErrNullRouteTables
	}

	local := make(map[string]types.Route, len(tables))
	misses := make([]string, 0, len(tables))

	for _, table := range tables {
		if v, ok := c.routeCache.Get(table); ok {
			local[table] = v.(types.Route)
		} else {
			misses = append(misses, table)
		}
	}

	if len(misses) == 0 {
		return local, nil
	}

	if err := c.routeFreshFor(reqCtx, misses); err != nil {
		return nil, err
	}

	for _, table := range misses {
		if v, ok := c.routeCache.Get(table); ok {
			local[table] = v.(types.Route)
		} else {
			local[table] = types.Route{
				Table:    table,
				Endpoint: c.endpoint,
			}
		}
	}
	return local, nil
}

func (c *directRouteClient) routeFreshFor(reqCtx types.RequestContext, tables []string) error {
	routes, err := c.rpcClient.Route(c.endpoint, reqCtx, tables)
	if err != nil {
		return err
	}

	for _, route := range routes {
		c.routeCache.Add(route.Table, route)
	}
	return nil
}

func (c *directRouteClient) ClearRouteFor(tables []string) {
	if c.opts.LoggerDebug {
		_, _ = c.opts.Logger.Write([]byte(fmt.Sprintf("Clear tables route for refresh code, tables:%v\n", tables)))
	}
	for _, table := range tables {
		c.routeCache.Remove(table)
	}
}

func (c *directRouteClient) OnEvict(table, _ interface{}) {
	if c.opts.LoggerDebug {
		_, _ = c.opts.Logger.Write([]byte(fmt.Sprintf("Clear table route for evict, table:%s\n", table)))
	}
}

type proxyRouteClient struct {
	endpoint  string
	rpcClient *rpcClient
}

func (c *proxyRouteClient) RouteFor(_ types.RequestContext, tables []string) (map[string]types.Route, error) {
	if len(tables) == 0 {
		return nil, types.ErrNullRouteTables
	}

	routes := make(map[string]types.Route, len(tables))
	for _, table := range tables {
		routes[table] = types.Route{
			Table:    table,
			Endpoint: c.endpoint,
		}
	}
	return routes, nil
}

func (c *proxyRouteClient) ClearRouteFor([]string) {
	// do noting
}
