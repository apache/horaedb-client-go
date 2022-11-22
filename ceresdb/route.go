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
	routeCache *lru.Cache // metric -> *Route
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

func (c *routeClient) RouteFor(metrics []string) (map[string]types.Route, error) {
	if len(metrics) == 0 {
		return nil, types.ErrNullRouteMetrics
	}

	local := make(map[string]types.Route, len(metrics))
	misses := make([]string, 0, len(metrics))

	for _, metric := range metrics {
		if v, ok := c.routeCache.Get(metric); ok {
			local[metric] = v.(types.Route)
		} else {
			misses = append(misses, metric)
		}
	}

	if len(misses) == 0 {
		return local, nil
	}

	if err := c.RouteFreshFor(misses); err != nil {
		return nil, err
	}

	for _, metric := range misses {
		if v, ok := c.routeCache.Get(metric); ok {
			local[metric] = v.(types.Route)
		} else {
			return nil, fmt.Errorf("Route not found for metric:%s", metric)
		}
	}
	return local, nil
}

func (c *routeClient) RouteFreshFor(metrics []string) error {
	routes, err := c.rpcClient.Route(c.endpoint, metrics)
	if err != nil {
		return err
	}

	for _, route := range routes {
		c.routeCache.Add(route.Metric, route)
	}
	return nil
}

func (c *routeClient) ClearRouteFor(metrics []string) {
	if c.opts.LoggerDebug {
		c.opts.Logger.Write([]byte(fmt.Sprintf("Clear metrics route for refresh code, metrics:%v\n", metrics)))
	}
	for _, metric := range metrics {
		c.routeCache.Remove(metric)
	}
}

func (c *routeClient) OnEvict(metric, _ interface{}) {
	if c.opts.LoggerDebug {
		c.opts.Logger.Write([]byte(fmt.Sprintf("Clear metric route for evict, metric:%s\n", metric)))
	}
}
