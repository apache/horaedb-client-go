// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ceresdb

import (
	"context"
	"fmt"

	"github.com/CeresDB/ceresdb-client-go/types"
	"github.com/CeresDB/ceresdb-client-go/utils"
)

type clientImpl struct {
	rpcClient   *rpcClient
	routeClient *routeClient
}

func newClient(endpoint string, opts options) (CeresDBClient, error) {
	rpcClient := newRpcClient(opts)
	routeClient, err := newRouteClient(endpoint, rpcClient, opts)
	if err != nil {
		return nil, err
	}
	return &clientImpl{
		rpcClient:   rpcClient,
		routeClient: routeClient,
	}, nil
}

func (c *clientImpl) Query(ctx context.Context, req types.QueryRequest) (types.QueryResponse, error) {
	if len(req.Metrics) == 0 {
		return types.QueryResponse{}, types.ErrNullRequestMetrics
	}

	routes, err := c.routeClient.RouteFor(req.Metrics)
	if err != nil {
		return types.QueryResponse{}, fmt.Errorf("Route metrics failed, metrics:%v, err:%v", req.Metrics, err)
	}
	for _, route := range routes {
		queryResponse, err := c.rpcClient.Query(route.Endpoint, ctx, req)
		if ceresdbErr, ok := err.(*types.CeresdbError); ok && ceresdbErr.ShouldClearRoute() {
			c.routeClient.ClearRouteFor(req.Metrics)
		}
		return queryResponse, err
	}
	return types.QueryResponse{}, types.ErrEmptyRoute
}

func (c *clientImpl) Write(ctx context.Context, rows []*types.Row) (types.WriteResponse, error) {
	if len(rows) == 0 {
		return types.WriteResponse{}, types.ErrNullRows
	}

	metrics := utils.GetMetricsFromRows(rows)

	routes, err := c.routeClient.RouteFor(metrics)
	if err != nil {
		return types.WriteResponse{}, err
	}
	rowsByRoute, err := utils.SplitRowsByRoute(rows, routes)
	if err != nil {
		return types.WriteResponse{}, err
	}

	// TODO
	// Convert to parallel write
	ret := types.WriteResponse{}
	for endpoint, rows := range rowsByRoute {
		response, err := c.rpcClient.Write(endpoint, ctx, rows)
		if err != nil {
			if ceresdbErr, ok := err.(*types.CeresdbError); ok && ceresdbErr.ShouldClearRoute() {
				c.routeClient.ClearRouteFor(utils.GetMetricsFromRows(rows))
			}

			ret = utils.CombineWriteResponse(ret, types.WriteResponse{Failed: uint32(len(rows))})
			continue
		}
		ret = utils.CombineWriteResponse(ret, response)
	}
	return ret, nil
}
