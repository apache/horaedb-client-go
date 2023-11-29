

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
