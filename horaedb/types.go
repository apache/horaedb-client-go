/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package horaedb

type RequestContext struct {
	Database string
}

type WriteRequest struct {
	ReqCtx RequestContext
	Points []Point
}

type WriteResponse struct {
	Success uint32
	Failed  uint32
	Message string
}

type SQLQueryRequest struct {
	ReqCtx RequestContext
	Tables []string
	SQL    string
}

type SQLQueryResponse struct {
	SQL          string
	AffectedRows uint32
	Rows         []Row
}

type Column struct {
	name  string
	value Value
}

func (c Column) Name() string {
	return c.name
}

func (c Column) Value() Value {
	return c.value
}

type Row struct {
	fields []string
	values []Value
}

func (r Row) HasColumn(name string) bool {
	_, ok := r.getColumnIdx(name)
	return ok
}

func (r Row) Column(name string) (Column, bool) {
	if idx, ok := r.getColumnIdx(name); ok {
		return Column{name, r.values[idx]}, true
	}
	return Column{}, false
}

func (r Row) Columns() []Column {
	columns := make([]Column, 0, len(r.values))
	for idx, field := range r.fields {
		columns = append(columns, Column{field, r.values[idx]})
	}
	return columns
}

func (r Row) getColumnIdx(name string) (int, bool) {
	for idx, field := range r.fields {
		if field == name {
			return idx, true
		}
	}
	return -1, false
}
