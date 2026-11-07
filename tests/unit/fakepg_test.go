package unit

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// pgResult is one canned answer of the fake querier.
type pgResult struct {
	// values is the single row returned by QueryRow.
	values []any
	// rows are the rows returned by Query.
	rows [][]any
	// tag is the command tag returned by Exec.
	tag pgconn.CommandTag
	// err is returned instead of data when set.
	err error
}

// fakeQuerier is an in-memory repository.Querier: it replays canned answers in
// order so the SQL wrappers of the repository package can be exercised without
// a PostgreSQL server.
type fakeQuerier struct {
	results   []pgResult
	index     int
	queries   []string
	beginErr  error
	commitErr error
}

// next returns the next canned answer.
func (q *fakeQuerier) next() pgResult {
	if q.index >= len(q.results) {
		q.index++
		return pgResult{err: fmt.Errorf("fake querier: unexpected statement")}
	}
	result := q.results[q.index]
	q.index++
	return result
}

// Begin implements repository.Querier.
func (q *fakeQuerier) Begin(context.Context) (pgx.Tx, error) {
	if q.beginErr != nil {
		return nil, q.beginErr
	}
	return &fakeTx{querier: q}, nil
}

// Exec implements repository.Querier.
func (q *fakeQuerier) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	q.queries = append(q.queries, sql)
	result := q.next()
	if result.err != nil {
		return pgconn.CommandTag{}, result.err
	}
	return result.tag, nil
}

// Query implements repository.Querier.
func (q *fakeQuerier) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	q.queries = append(q.queries, sql)
	result := q.next()
	if result.err != nil {
		return nil, result.err
	}
	return &fakeRows{rows: result.rows}, nil
}

// QueryRow implements repository.Querier.
func (q *fakeQuerier) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	q.queries = append(q.queries, sql)
	result := q.next()
	return &fakeRow{values: result.values, err: result.err}
}

// fakeTx is an in-memory pgx.Tx.
type fakeTx struct {
	querier *fakeQuerier
}

// Begin implements pgx.Tx.
func (t *fakeTx) Begin(context.Context) (pgx.Tx, error) { return t, nil }

// Commit implements pgx.Tx.
func (t *fakeTx) Commit(context.Context) error { return t.querier.commitErr }

// Rollback implements pgx.Tx.
func (t *fakeTx) Rollback(context.Context) error { return nil }

// CopyFrom implements pgx.Tx.
func (t *fakeTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, nil
}

// SendBatch implements pgx.Tx.
func (t *fakeTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { return nil }

// LargeObjects implements pgx.Tx.
func (t *fakeTx) LargeObjects() pgx.LargeObjects { return pgx.LargeObjects{} }

// Prepare implements pgx.Tx.
func (t *fakeTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return &pgconn.StatementDescription{}, nil
}

// Exec implements pgx.Tx.
func (t *fakeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return t.querier.Exec(ctx, sql, args...)
}

// Query implements pgx.Tx.
func (t *fakeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.querier.Query(ctx, sql, args...)
}

// QueryRow implements pgx.Tx.
func (t *fakeTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return t.querier.QueryRow(ctx, sql, args...)
}

// Conn implements pgx.Tx.
func (t *fakeTx) Conn() *pgx.Conn { return nil }

// fakeRow is an in-memory pgx.Row.
type fakeRow struct {
	values []any
	err    error
}

// Scan implements pgx.Row.
func (r *fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return assign(dest, r.values)
}

// fakeRows is an in-memory pgx.Rows.
type fakeRows struct {
	rows  [][]any
	index int
	err   error
}

// Close implements pgx.Rows.
func (r *fakeRows) Close() {}

// Err implements pgx.Rows.
func (r *fakeRows) Err() error { return r.err }

// CommandTag implements pgx.Rows.
func (r *fakeRows) CommandTag() pgconn.CommandTag { return pgconn.NewCommandTag("SELECT 0") }

// FieldDescriptions implements pgx.Rows.
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

// Next implements pgx.Rows.
func (r *fakeRows) Next() bool {
	if r.index >= len(r.rows) {
		return false
	}
	r.index++
	return true
}

// Scan implements pgx.Rows.
func (r *fakeRows) Scan(dest ...any) error {
	if r.index == 0 || r.index > len(r.rows) {
		return fmt.Errorf("fake rows: Scan without Next")
	}
	return assign(dest, r.rows[r.index-1])
}

// Values implements pgx.Rows.
func (r *fakeRows) Values() ([]any, error) { return nil, nil }

// RawValues implements pgx.Rows.
func (r *fakeRows) RawValues() [][]byte { return nil }

// Conn implements pgx.Rows.
func (r *fakeRows) Conn() *pgx.Conn { return nil }

// TypeMap implements pgx.Rows.
func (r *fakeRows) TypeMap() *pgtype.Map { return nil }

// assign copies values into the scan destinations, mirroring pgx's type
// conversion for the types used by the repositories.
func assign(dest []any, values []any) error {
	if len(dest) != len(values) {
		return fmt.Errorf("fake rows: cannot scan %d values into %d destinations", len(values), len(dest))
	}
	for i, destination := range dest {
		target := reflect.ValueOf(destination)
		if target.Kind() != reflect.Pointer || target.IsNil() {
			return fmt.Errorf("fake rows: destination %d is not a pointer", i)
		}
		element := target.Elem()
		value := reflect.ValueOf(values[i])
		if !value.IsValid() {
			element.Set(reflect.Zero(element.Type()))
			continue
		}
		if element.Kind() == reflect.Pointer {
			if value.Kind() == reflect.Pointer && value.IsNil() {
				element.Set(reflect.Zero(element.Type()))
				continue
			}
			inner := value
			if inner.Kind() == reflect.Pointer {
				inner = inner.Elem()
			}
			holder := reflect.New(element.Type().Elem())
			if !inner.Type().AssignableTo(holder.Elem().Type()) {
				return fmt.Errorf("fake rows: cannot scan %s into %s", inner.Type(), element.Type())
			}
			holder.Elem().Set(inner)
			element.Set(holder)
			continue
		}
		if value.Type().AssignableTo(element.Type()) {
			element.Set(value)
			continue
		}
		if value.Type().ConvertibleTo(element.Type()) {
			element.Set(value.Convert(element.Type()))
			continue
		}
		return fmt.Errorf("fake rows: cannot scan %s into %s", value.Type(), element.Type())
	}
	return nil
}

// sampleTime is the fixed instant used by the repository tests.
var sampleTime = time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
