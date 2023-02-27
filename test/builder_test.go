// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package test

import (
	"testing"

	"github.com/CeresDB/ceresdb-client-go/ceresdb"
	"github.com/stretchr/testify/require"
)

func TestPointBuilder(t *testing.T) {
	point, err := ceresdb.NewPointBuilder("test").
		SetTimestamp(currentMS()).
		AddTag("tagA", ceresdb.NewStringValue("a")).
		AddField("filedA", ceresdb.NewFloatValue(0.24)).
		Build()

	require.NoError(t, err)
	require.Equal(t, point.Table, "test")
	require.Equal(t, point.Tags["tagA"].StringValue(), "a")
	require.Equal(t, point.Fields["filedA"].FloatValue(), float32(0.24))
}

func TestPointBuilderWithEmptyTableErr(t *testing.T) {
	_, err := ceresdb.NewPointBuilder("").
		SetTimestamp(currentMS()).
		AddTag("tagA", ceresdb.NewStringValue("a")).
		AddField("filedA", ceresdb.NewFloatValue(0.24)).
		Build()

	require.ErrorIs(t, err, ceresdb.ErrPointEmptyTable)
}

func TestPointBuilderWithEmptyTimestampErr(t *testing.T) {
	_, err := ceresdb.NewPointBuilder("test").
		AddTag("tagA", ceresdb.NewStringValue("a")).
		AddField("filedA", ceresdb.NewFloatValue(0.24)).
		Build()

	require.ErrorIs(t, err, ceresdb.ErrPointEmptyTimestamp)
}

func TestPointBuilderWithEmptyTagsErr(t *testing.T) {
	_, err := ceresdb.NewPointBuilder("test").
		SetTimestamp(currentMS()).
		AddField("filedA", ceresdb.NewFloatValue(0.24)).
		Build()

	require.ErrorIs(t, err, ceresdb.ErrPointEmptyTags)
}

func TestPointBuilderWithEmptyFieldsErr(t *testing.T) {
	_, err := ceresdb.NewPointBuilder("test").
		SetTimestamp(currentMS()).
		AddTag("tagA", ceresdb.NewStringValue("a")).
		Build()

	require.ErrorIs(t, err, ceresdb.ErrPointEmptyFields)
}

func TestPointBuilderWithReservedColumn(t *testing.T) {
	_, err := ceresdb.NewPointBuilder("test").
		SetTimestamp(currentMS()).
		AddTag("tsid", ceresdb.NewStringValue("a")).
		AddField("filedA", ceresdb.NewFloatValue(0.24)).
		Build()

	require.ErrorContains(t, err, "tag name is reserved column name in ceresdb")
}
