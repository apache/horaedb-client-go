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

	"github.com/apache/horaedb-client-go/horaedb"
	"github.com/stretchr/testify/require"
)

func TestPointBuilder(t *testing.T) {
	point, err := horaedb.NewPointBuilder("test").
		SetTimestamp(currentMS()).
		AddTag("tagA", horaedb.NewStringValue("a")).
		AddField("filedA", horaedb.NewFloatValue(0.24)).
		Build()

	require.NoError(t, err)
	require.Equal(t, point.Table, "test")
	require.Equal(t, point.Tags["tagA"].StringValue(), "a")
	require.Equal(t, point.Fields["filedA"].FloatValue(), float32(0.24))
}

func TestPointBuilderWithEmptyTableErr(t *testing.T) {
	_, err := horaedb.NewPointBuilder("").
		SetTimestamp(currentMS()).
		AddTag("tagA", horaedb.NewStringValue("a")).
		AddField("filedA", horaedb.NewFloatValue(0.24)).
		Build()

	require.ErrorIs(t, err, horaedb.ErrPointEmptyTable)
}

func TestPointBuilderWithEmptyTimestampErr(t *testing.T) {
	_, err := horaedb.NewPointBuilder("test").
		AddTag("tagA", horaedb.NewStringValue("a")).
		AddField("filedA", horaedb.NewFloatValue(0.24)).
		Build()

	require.ErrorIs(t, err, horaedb.ErrPointEmptyTimestamp)
}

func TestPointBuilderWithEmptyTagsErr(t *testing.T) {
	_, err := horaedb.NewPointBuilder("test").
		SetTimestamp(currentMS()).
		AddField("filedA", horaedb.NewFloatValue(0.24)).
		Build()

	require.ErrorIs(t, err, horaedb.ErrPointEmptyTags)
}

func TestPointBuilderWithEmptyFieldsErr(t *testing.T) {
	_, err := horaedb.NewPointBuilder("test").
		SetTimestamp(currentMS()).
		AddTag("tagA", horaedb.NewStringValue("a")).
		Build()

	require.ErrorIs(t, err, horaedb.ErrPointEmptyFields)
}

func TestPointBuilderWithReservedColumn(t *testing.T) {
	_, err := horaedb.NewPointBuilder("test").
		SetTimestamp(currentMS()).
		AddTag("tsid", horaedb.NewStringValue("a")).
		AddField("filedA", horaedb.NewFloatValue(0.24)).
		Build()

	require.ErrorContains(t, err, "tag name is reserved column name in horaedb")
}
