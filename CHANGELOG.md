## 1.1.0 [2023-02-28]
- Features
  - Update proto to v1.0.0 [37](https://github.com/CeresDB/ceresdb-client-go/pull/37)
  - Add API `Row.columns() []Column` [36](https://github.com/CeresDB/ceresdb-client-go/pull/36)
- Fixes
  - Fix `Route not found for table` [33](https://github.com/CeresDB/ceresdb-client-go/pull/33)
- Breaking Changes
  - Move package `types` to `ceresdb` [34](https://github.com/CeresDB/ceresdb-client-go/pull/34)
  - Move `Row.ColumnValue(string) Value` to `Row.Column(string) (Column, bool)`
  - Move `Value.Value() interface{}` to `Value.AnyValue() interface{}`

## 1.0.1 [2023-02-23]
- Fixes
  - Remove `tools.go` to fix [#27](https://github.com/CeresDB/ceresdb-client-go/pull/27)

## 1.0.0 [2023-02-22]
- Features
    - The [Ceresdb](https://github.com/CeresDB/ceresdb/tree/main) Golang client version of the API allowing for the reading, writing, and managing of data tables.
