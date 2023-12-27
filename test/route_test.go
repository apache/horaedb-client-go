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

package test

import (
	"testing"
	"time"

	"github.com/apache/horaedb-client-go/horaedb"
	"github.com/stretchr/testify/require"
)

func TestRouteGc(t *testing.T) {
	t.Skip("ignore local test")

	client, err := horaedb.NewClient(clusterEndpoint, horaedb.Direct,
		horaedb.EnableLoggerDebug(true),
		horaedb.WithRouteMaxCacheSize(3),
	)
	require.NoError(t, err, "init horaedb client failed")

	timestamp := currentMS()

	testBaseWrite(t, client, "horaedb_route_test1", timestamp, 1)
	time.Sleep(time.Second)
	testBaseWrite(t, client, "horaedb_route_test2", timestamp, 2)
	time.Sleep(time.Second)
	testBaseWrite(t, client, "horaedb_route_test3", timestamp, 3)
	testBaseQuery(t, client, "horaedb_route_test2", timestamp, 2)
	time.Sleep(time.Second)
	testBaseWrite(t, client, "horaedb_route_test4", timestamp, 4)
	time.Sleep(time.Second)
	testBaseWrite(t, client, "horaedb_route_test5", timestamp, 5)

	// Under single-threaded test conditions,
	// the expected result is that the routes of horaedb_route_test1 and table horaedb_route_test3 are cleaned up

	time.Sleep(12 * time.Second)
}

func TestRouteProxy(t *testing.T) {
	t.Skip("ignore local test")

	client, err := horaedb.NewClient(clusterEndpoint, horaedb.Proxy,
		horaedb.EnableLoggerDebug(true),
		horaedb.WithRouteMaxCacheSize(3),
	)
	require.NoError(t, err, "init horaedb client failed")

	timestamp := currentMS()
	testBaseWrite(t, client, "horaedb_route_test1", timestamp, 1)
}
