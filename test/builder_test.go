// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package test

import (
	"testing"

	"github.com/CeresDB/ceresdb-client-go/ceresdb"
	"github.com/CeresDB/ceresdb-client-go/types"
	"github.com/CeresDB/ceresdb-client-go/utils"
	"github.com/stretchr/testify/require"
)

func TestPointBuilder(t *testing.T) {
	point, err := ceresdb.NewPointBuilder("test").
		SetTimestamp(utils.CurrentMS()).
		AddTag("tagA", types.NewStringValue("a")).
		AddField("filedA", types.NewFloatValue(0.24)).
		Build()

	require.NoError(t, err)
	require.Equal(t, point.Table, "test")
	require.Equal(t, point.Tags["tagA"].StringValue(), "a")
	require.Equal(t, point.Fields["filedA"].FloatValue(), float32(0.24))
}

func TestPointBuilderWithEmptyTableErr(t *testing.T) {
	_, err := ceresdb.NewPointBuilder("").
		SetTimestamp(utils.CurrentMS()).
		AddTag("tagA", types.NewStringValue("a")).
		AddField("filedA", types.NewFloatValue(0.24)).
		Build()

	require.ErrorIs(t, err, types.ErrPointEmptyTable)
}

func TestPointBuilderWithEmptyTimestampErr(t *testing.T) {
	_, err := ceresdb.NewPointBuilder("test").
		AddTag("tagA", types.NewStringValue("a")).
		AddField("filedA", types.NewFloatValue(0.24)).
		Build()

	require.ErrorIs(t, err, types.ErrPointEmptyTimestamp)
}

func TestPointBuilderWithEmptyTagsErr(t *testing.T) {
	_, err := ceresdb.NewPointBuilder("test").
		SetTimestamp(utils.CurrentMS()).
		AddField("filedA", types.NewFloatValue(0.24)).
		Build()

	require.ErrorIs(t, err, types.ErrPointEmptyTags)
}

func TestPointBuilderWithEmptyFieldsErr(t *testing.T) {
	_, err := ceresdb.NewPointBuilder("test").
		SetTimestamp(utils.CurrentMS()).
		AddTag("tagA", types.NewStringValue("a")).
		Build()

	require.ErrorIs(t, err, types.ErrPointEmptyFields)
}

func TestPointBuilderWithReservedColumn(t *testing.T) {
	_, err := ceresdb.NewPointBuilder("test").
		SetTimestamp(utils.CurrentMS()).
		AddTag("tsid", types.NewStringValue("a")).
		AddField("filedA", types.NewFloatValue(0.24)).
		Build()

	require.ErrorContains(t, err, "tag name is reserved column name in ceresdb")
}
