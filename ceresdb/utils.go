// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ceresdb

import (
	"fmt"
)

func getTablesFromPoints(points []Point) []string {
	dict := make(map[string]byte)
	tables := make([]string, 0, len(points))
	for _, point := range points {
		if _, ok := dict[point.Table]; !ok {
			dict[point.Table] = 0
			tables = append(tables, point.Table)
		}
	}
	return tables
}

func splitPointsByRoute(points []Point, routes map[string]route) (map[string][]Point, error) {
	pointsByRoute := make(map[string][]Point, len(routes))
	for _, point := range points {
		route, ok := routes[point.Table]
		if !ok {
			return nil, fmt.Errorf("route for table %s not found", point.Table)
		}
		if rows, ok := pointsByRoute[route.Endpoint]; ok {
			pointsByRoute[route.Endpoint] = append(rows, point)
		} else {
			pointsByRoute[route.Endpoint] = []Point{point}
		}
	}

	return pointsByRoute, nil
}

func combineWriteResponse(r1 WriteResponse, r2 WriteResponse) WriteResponse {
	r1.Success += r2.Success
	r1.Failed += r2.Failed
	return r1
}

func combineWriteError(e1 CeresdbWriteError, e2 CeresdbWriteError) CeresdbWriteError {
	if len(e2.SuccessTables) > 0 {
		e1.SuccessTables = append(e1.SuccessTables, e2.SuccessTables...)
		e1.SuccessOk = append(e1.SuccessOk, e2.SuccessOk...)
	}
	if len(e2.FailedTables) > 0 {
		e1.FailedTables = append(e1.FailedTables, e2.FailedTables...)
		e1.Errors = append(e1.Errors, e2.Errors...)
	}
	return e1
}
