package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/assembly-hub/db"
)

// row func
type row struct {
	r driver.Row
}

func (r *row) Scan(dest ...any) error {
	return r.r.Scan(dest...)
}

type columnType struct {
	col driver.ColumnType
}

func (c *columnType) Name() string {
	return c.col.Name()
}

func (c *columnType) Nullable() (nullable, ok bool) {
	return c.col.Nullable(), true
}

func (c *columnType) ScanType() reflect.Type {
	return c.col.ScanType()
}

func (c *columnType) DatabaseTypeName() string {
	return c.col.DatabaseTypeName()
}

func (c *columnType) DecimalSize() (precision, scale int64, ok bool) {
	panic("not support")
}

func (c *columnType) Length() (length int64, ok bool) {
	panic("not support")
}

// rows func
type rows struct {
	rows driver.Rows
}

func (rows *rows) ColumnTypes() ([]db.ColumnType, error) {
	types := rows.rows.ColumnTypes()
	if len(types) <= 0 {
		return nil, fmt.Errorf("no column")
	}
	colType := make([]db.ColumnType, len(types))
	for i, col := range types {
		colType[i] = &columnType{col: col}
	}
	return colType, nil
}

func (rows *rows) Columns() ([]string, error) {
	cols := rows.rows.Columns()
	if len(cols) <= 0 {
		return nil, fmt.Errorf("no column")
	}
	return cols, nil
}

func (rows *rows) Err() error {
	return rows.rows.Err()
}

func (rows *rows) Next() bool {
	return rows.rows.Next()
}

func (rows *rows) NextResultSet() bool {
	return false
}

func (rows *rows) Close() error {
	return rows.rows.Close()
}

func (rows *rows) Scan(dest ...any) error {
	return rows.rows.Scan(dest...)
}

// stmt func
type stmt struct {
	// s *sql.Stmt
}

func (s *stmt) Close() error {
	panic("not support")
}

func (s *stmt) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	panic("not support")
}

func (s *stmt) QueryContext(ctx context.Context, args ...any) (db.Rows, error) {
	panic("not support")
}

func (s *stmt) QueryRowContext(ctx context.Context, args ...any) db.Row {
	panic("not support")
}

type sqlDB struct {
	db driver.Conn
}

func (db *sqlDB) PrepareContext(ctx context.Context, query string) (db.Stmt, error) {
	panic("not support")
}

type nullResult struct {
}

func (n nullResult) LastInsertId() (int64, error) {
	return -1, nil
}

func (n nullResult) RowsAffected() (int64, error) {
	return -1, nil
}

func (db *sqlDB) ExecContext(ctx context.Context, query string, args ...any) (db.Result, error) {
	err := db.db.Exec(ctx, query, args...)
	return &nullResult{}, err
}

func (db *sqlDB) QueryContext(ctx context.Context, query string, args ...any) (db.Rows, error) {
	rs, err := db.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &rows{rs}, nil
}

func (db *sqlDB) QueryRowContext(ctx context.Context, query string, args ...any) db.Row {
	r := db.db.QueryRow(ctx, query, args...)
	if r == nil {
		return nil
	}
	return &row{r}
}

func (db *sqlDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (db.Tx, error) {
	panic("not support")
}

func NewDB(db driver.Conn) db.Executor {
	return &sqlDB{
		db: db,
	}
}

func (db *sqlDB) GetRaw() driver.Conn {
	return db.db
}
