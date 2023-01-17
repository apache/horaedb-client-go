// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ceresdb

import (
	"fmt"

	"github.com/CeresDB/ceresdb-client-go/types"
	lru "github.com/hashicorp/golang-lru"
)

type routeClient struct {
	opts       options
	endpoint   string
	rpcClient  *rpcClient
	routeCache *lru.Cache // table -> *Route
}

func newRouteClient(endpoint string, rpcClient *rpcClient, opts options) (*routeClient, error) {
	routeClient := &routeClient{
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
}

func (c *routeClient) RouteFor(tables []string) (map[string]types.Route, error) {
	if len(tables) == 0 {
		return nil, types.ErrNullRouteMetrics
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

	if err := c.RouteFreshFor(misses); err != nil {
		return nil, err
	}

	for _, table := range misses {
		if v, ok := c.routeCache.Get(table); ok {
			local[table] = v.(types.Route)
		} else {
			return nil, fmt.Errorf("Route not found for table:%s", table)
		}
	}
	return local, nil
}

func (c *routeClient) RouteFreshFor(tables []string) error {
	routes, err := c.rpcClient.Route(c.endpoint, tables)
	if err != nil {
		return err
	}

	for _, route := range routes {
		c.routeCache.Add(route.Table, route)
	}
	return nil
}

func (c *routeClient) ClearRouteFor(tables []string) {
	if c.opts.LoggerDebug {
		_, _ = c.opts.Logger.Write([]byte(fmt.Sprintf("Clear tables route for refresh code, tables:%v\n", tables)))
	}
	for _, table := range tables {
		c.routeCache.Remove(table)
	}
}

func (c *routeClient) OnEvict(table, _ interface{}) {
	if c.opts.LoggerDebug {
		_, _ = c.opts.Logger.Write([]byte(fmt.Sprintf("Clear table route for evict, table:%s\n", table)))
	}
}
