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

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/apache/horaedb-client-go/horaedb"
)

var endpoint = "127.0.0.1:8831"

func init() {
	if v := os.Getenv("HORAEDB_ADDR"); v != "" {
		endpoint = v
	}
}

func existsTable(client horaedb.Client) error {
	req := horaedb.SQLQueryRequest{
		Tables: []string{"demo"},
		SQL:    "EXISTS TABLE demo",
	}
	resp, err := client.SQLQuery(context.Background(), req)
	if err != nil {
		fmt.Printf("exists table fail, err: %v\n", err)
		return err
	}
	fmt.Printf("exists table success, resp: %+v\n", resp)
	return nil
}

func createTable(client horaedb.Client) error {
	createTableSQL := `CREATE TABLE IF NOT EXISTS demo (
	name string TAG,
	value double,
	t timestamp NOT NULL,
	TIMESTAMP KEY(t)) ENGINE=Analytic with (enable_ttl=false)`

	req := horaedb.SQLQueryRequest{
		Tables: []string{"demo"},
		SQL:    createTableSQL,
	}
	resp, err := client.SQLQuery(context.Background(), req)
	if err != nil {
		fmt.Printf("create table fail, err: %v\n", err)
		return err
	}
	fmt.Printf("create table success, resp: %+v\n", resp)
	return nil
}

func dropTable(client horaedb.Client) error {
	dropTableSQL := `DROP TABLE demo`
	req := horaedb.SQLQueryRequest{
		Tables: []string{"demo"},
		SQL:    dropTableSQL,
	}
	resp, err := client.SQLQuery(context.Background(), req)
	if err != nil {
		fmt.Printf("drop table fail, err: %v\n", err)
		return err
	}
	fmt.Printf("drop table success, resp: %+v\n", resp)
	return nil
}

func writeTable(client horaedb.Client) error {
	nowInMs := time.Now().UnixNano() / int64(time.Millisecond)
	points := make([]horaedb.Point, 0, 2)
	for i := 0; i < 2; i++ {
		point, err := horaedb.NewPointBuilder("demo").
			SetTimestamp(nowInMs).
			AddTag("name", horaedb.NewStringValue("test_tag1")).
			AddField("value", horaedb.NewDoubleValue(0.4242)).
			Build()
		if err != nil {
			return err
		}
		points = append(points, point)
	}
	req := horaedb.WriteRequest{
		Points: points,
	}
	resp, err := client.Write(context.Background(), req)
	if err != nil {
		fmt.Printf("write table fail, err: %v\n", err)
		return err
	}
	if resp.Success != 2 {
		fmt.Printf("write table fail, upexpected response Success: %v\n", resp)
		return fmt.Errorf("upexpected response: %+v", resp)
	}
	fmt.Printf("write table success, response: %+v\n", resp)
	return nil
}

func queryTable(client horaedb.Client) error {
	querySQL := `SELECT * FROM demo`
	req := horaedb.SQLQueryRequest{
		Tables: []string{"demo"},
		SQL:    querySQL,
	}
	resp, err := client.SQLQuery(context.Background(), req)
	if err != nil {
		fmt.Printf("query table fail, err:%v\n", err)
		return err
	}
	fmt.Printf("query table success, rows:%+v\n", resp.Rows)
	return nil
}

func main() {
	fmt.Println("------------------------------------------------------------------")
	fmt.Println("### new client:")
	client, err := horaedb.NewClient(endpoint, horaedb.Direct,
		horaedb.WithDefaultDatabase("public"),
		horaedb.EnableLoggerDebug(true),
	)
	if err != nil {
		fmt.Printf("new client fail, err: %v\n", err)
		return
	}

	fmt.Println("------------------------------------------------------------------")
	fmt.Println("### exists table:")
	if err := existsTable(client); err != nil {
		return
	}

	fmt.Println("------------------------------------------------------------------")
	fmt.Println("### create table:")
	if err := createTable(client); err != nil {
		return
	}

	fmt.Println("------------------------------------------------------------------")
	fmt.Println("### write table:")
	if err := writeTable(client); err != nil {
		return
	}

	fmt.Println("------------------------------------------------------------------")
	fmt.Println("### query table:")
	if err := queryTable(client); err != nil {
		return
	}

	fmt.Println("------------------------------------------------------------------")
	fmt.Println("### drop table:")
	if err := dropTable(client); err != nil {
		return
	}

	fmt.Println("------------------------------------------------------------------")
}
