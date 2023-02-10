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
	routeClient RouteClient
}

func newClient(endpoint string, routeMode types.RouteMode, opts options) (Client, error) {
	rpcClient := newRPCClient(opts)
	routeClient, err := newRouteClient(endpoint, routeMode, rpcClient, opts)
	if err != nil {
		return nil, err
	}
	return &clientImpl{
		rpcClient:   rpcClient,
		routeClient: routeClient,
	}, nil
}

func (c *clientImpl) SQLQuery(ctx context.Context, req types.SQLQueryRequest) (types.SQLQueryResponse, error) {
	if err := c.withDefaultRequestContext(&req.ReqCtx); err != nil {
		return types.SQLQueryResponse{}, err
	}

	if len(req.Tables) == 0 {
		return types.SQLQueryResponse{}, types.ErrNullRequestTables
	}

	routes, err := c.routeClient.RouteFor(req.ReqCtx, req.Tables)
	if err != nil {
		return types.SQLQueryResponse{}, fmt.Errorf("Route tables failed, tables:%v, err:%v", req.Tables, err)
	}
	for _, route := range routes {
		queryResponse, err := c.rpcClient.SQLQuery(ctx, route.Endpoint, req)
		if ceresdbErr, ok := err.(*types.CeresdbError); ok && ceresdbErr.ShouldClearRoute() {
			c.routeClient.ClearRouteFor(req.Tables)
		}
		return queryResponse, err
	}
	return types.SQLQueryResponse{}, types.ErrEmptyRoute
}

func (c *clientImpl) Write(ctx context.Context, req types.WriteRequest) (types.WriteResponse, error) {
	if err := c.withDefaultRequestContext(&req.ReqCtx); err != nil {
		return types.WriteResponse{}, err
	}

	if len(req.Points) == 0 {
		return types.WriteResponse{}, types.ErrNullRows
	}

	tables := utils.GetTablesFromPoints(req.Points)

	routes, err := c.routeClient.RouteFor(req.ReqCtx, tables)
	if err != nil {
		return types.WriteResponse{}, err
	}

	pointsByRoute, err := utils.SplitPointsByRoute(req.Points, routes)
	if err != nil {
		return types.WriteResponse{}, err
	}

	// TODO
	// Convert to parallel write
	ret := types.WriteResponse{}
	for endpoint, points := range pointsByRoute {
		response, err := c.rpcClient.Write(ctx, endpoint, req.ReqCtx, points)
		if err != nil {
			if ceresdbErr, ok := err.(*types.CeresdbError); ok && ceresdbErr.ShouldClearRoute() {
				c.routeClient.ClearRouteFor(utils.GetTablesFromPoints(points))
			}

			ret = utils.CombineWriteResponse(ret, types.WriteResponse{Failed: uint32(len(points))})
			continue
		}
		ret = utils.CombineWriteResponse(ret, response)
	}
	return ret, nil
}

func (c *clientImpl) withDefaultRequestContext(reqCtx *types.RequestContext) error {
	// use default
	if reqCtx.Database == "" {
		reqCtx.Database = c.rpcClient.opts.Database
	}

	// check Request Context
	if reqCtx.Database == "" {
		return types.ErrNoDatabaseSelected
	}
	return nil
}
