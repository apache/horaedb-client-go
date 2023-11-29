/*
 * Copyright 2022 The HoraeDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ceresdb

import (
	"fmt"

	lru "github.com/hashicorp/golang-lru"
)

type route struct {
	Table    string
	Endpoint string
}

type routeClient interface {
	RouteFor(RequestContext, []string) (map[string]route, error)
	ClearRouteFor([]string)
}

func newRouteClient(endpoint string, routeMode RouteMode, rpcClient *rpcClient, opts options) (routeClient, error) {
	switch routeMode {
	case Direct:
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
	case Proxy:
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
	routeCache *lru.Cache // table -> *route
}

func (c *directRouteClient) RouteFor(reqCtx RequestContext, tables []string) (map[string]route, error) {
	if len(tables) == 0 {
		return nil, ErrNullRouteTables
	}

	local := make(map[string]route, len(tables))
	misses := make([]string, 0, len(tables))

	for _, table := range tables {
		if v, ok := c.routeCache.Get(table); ok {
			local[table] = v.(route)
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
			local[table] = v.(route)
		} else {
			local[table] = route{
				Table:    table,
				Endpoint: c.endpoint,
			}
		}
	}
	return local, nil
}

func (c *directRouteClient) routeFreshFor(reqCtx RequestContext, tables []string) error {
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

func (c *proxyRouteClient) RouteFor(_ RequestContext, tables []string) (map[string]route, error) {
	if len(tables) == 0 {
		return nil, ErrNullRouteTables
	}

	routes := make(map[string]route, len(tables))
	for _, table := range tables {
		routes[table] = route{
			Table:    table,
			Endpoint: c.endpoint,
		}
	}
	return routes, nil
}

func (c *proxyRouteClient) ClearRouteFor([]string) {
	// do noting
}
