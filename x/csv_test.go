package x

import (
	"bytes"
	"context"
	"database/sql"
	//"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode"

	_ "github.com/godror/godror"
	"github.com/google/renameio/v2"
	"github.com/kortschak/utter"
	"golang.org/x/exp/slog"
	"golang.org/x/tools/txtar"
)

var (
	flagConnect = flag.String("connect", os.Getenv("BRUNO_ID"), "DB to connect to")

	dump = utter.NewDefaultConfig()

	testFn = filepath.Join("testdata", "db.txtar")
)

func init() {
	dump.IgnoreUnexported = true
	dump.OmitZero = true
}
func testFuncs(t *testing.T, funcs []Function) {
	t.Log("funcs", dump.Sdump(funcs))
	var buf bytes.Buffer
	seen := make(map[string]struct{})
	for _, f := range funcs {
		buf.Reset()
		for _, dir := range []bool{false, true} {

			if err := f.saveProtobufDir(&buf, seen, dir); err != nil {
				t.Error(err)
			}
		}
	}
}

func TestReadDB(t *testing.T) {
	slog.SetDefault(slog.New(slog.HandlerOptions{Level: slog.LevelDebug}.
		NewTextHandler(testWriter{t})))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := sql.Open("godror", *flagConnect)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.PingContext(ctx); err != nil {
		t.Fatal(fmt.Errorf("connect to %q: %w", *flagConnect, err))
	}
	defer db.ExecContext(context.Background(), `DROP PACKAGE GT_gen_o_call`)
	{
		for _, qry := range []string{
			`CREATE OR REPLACE PACKAGE GT_gen_o_call IS
TYPE vc_tt IS TABLE OF VARCHAR2(32767) INDEX BY PLS_INTEGER;
TYPE rec_rt IS RECORD (F_txt VARCHAR2(100), F_vc_tt vc_tt);
TYPE rec_tt IS TABLE OF rec_rt INDEX BY PLS_INTEGER;

TYPE cur_ct IS REF CURSOR RETURN rec_rt;

TYPE obj_rt IS RECORD (
  F_int PLS_INTEGER,
  F_dt  DATE,
  F_num NUMBER,
  F_clob CLOB,
  F_str VARCHAR2(1000),
  F_vc_tt vc_tt,
  F_rec rec_rt,
  F_rec_tt rec_tt,
  F_cursor cur_ct
);
TYPE obj_tt IS TABLE OF obj_rt INDEX BY PLS_INTEGER;

FUNCTION fun(p_int IN PLS_INTEGER, p_dt IN DATE, p_num IN NUMBER, p_clob IN CLOB, p_str IN VARCHAR2)
RETURN obj_tt;
END;`,

			`CREATE OR REPLACE PACKAGE BODY GT_gen_o_Call IS 
FUNCTION fun(p_int IN PLS_INTEGER, p_dt IN DATE, p_num IN NUMBER, p_clob IN CLOB, p_str IN VARCHAR2)
RETURN obj_tt IS
  v_obj obj_tt;
BEGIN
  v_obj(1) := obj_rt(F_int=>p_int, F_dt=>p_dt, F_num=>p_num, F_clob=>p_Clob, F_str=>p_Str);
  RETURN(v_obj);
END fun;
END;`,
		} {
			if _, err := db.ExecContext(ctx, qry); err != nil {
				t.Fatalf("%s: %+v", qry, err)
			}
		}
	}

	{
		var files []txtar.File
		db := DB{DB: SqlDB{
			DB: db,
			LogQry: func(qry string, args ...any) (
				func(...any),
				func(),
			) {
				f := txtar.File{Name: qryArgsString(qry, args)}
				var buf bytes.Buffer
				w := json.NewEncoder(&buf)
				next := func(args ...any) {
					_ = w.Encode(args)
				}
				finish := func() {
					f.Data = buf.Bytes()
					files = append(files, f)
				}
				return next, finish
			},
		}}
		funcs, err := db.ReadPackage(ctx, "GT_gen_o_call")
		if err != nil {
			t.Fatal(err)
		}
		testFuncs(t, funcs)
		if err := renameio.WriteFile(
			testFn,
			txtar.Format(&txtar.Archive{Files: files}),
			0644,
		); err != nil {
			t.Error(err)
		}
	}
}

func TestParseCSV(t *testing.T) {
	slog.SetDefault(slog.New(slog.HandlerOptions{Level: slog.LevelDebug}.
		NewTextHandler(testWriter{t})))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ar, err := txtar.ParseFile(testFn)
	if err != nil {
		t.Fatal(err)
	}
	qrys := make(map[string][][]any, len(ar.Files))
	for _, f := range ar.Files {
		dec := json.NewDecoder(bytes.NewReader(f.Data))
		for {
			var row []any
			err := dec.Decode(&row)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				t.Error(err)
			}
			qrys[f.Name] = append(qrys[f.Name], row)
		}
	}

	db := DB{DB: TestDB{qrys: qrys}}
	funcs, err := db.ReadPackage(ctx, "GT_gen_o_call")
	if err != nil {
		t.Fatal(err)
	}
	testFuncs(t, funcs)
}
func qryArgsString(qry string, args []any) string {
	b, _ := json.Marshal(args)
	var prev rune
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			r = ' '
			if r == prev {
				return -1
			}
		}
		prev = r
		return r
	},
		qry) + "\t" + string(b)
}

type testWriter struct {
	testing.TB
}

func (t testWriter) Write(p []byte) (int, error) {
	t.TB.Log(string(p))
	return len(p), nil
}

type TestDB struct {
	qrys map[string][][]any
}

func (db TestDB) QueryContext(ctx context.Context, qry string, args ...any) (rowser, error) {
	k := qryArgsString(qry, args)
	rows, ok := db.qrys[k]
	if !ok {
		return nil, fmt.Errorf("unknown query %q %v", qry, args)
	}
	delete(db.qrys, k)
	return &testRows{rows: rows}, nil
}

func (db TestDB) QueryRowContext(ctx context.Context, qry string, args ...any) rower {
	k := qryArgsString(qry, args)
	rows, ok := db.qrys[k]
	if !ok {
		return &testRows{err: fmt.Errorf("unknown query %q %v", qry, args)}
	}
	tRows := &testRows{rows: rows}
	_ = tRows.Next()
	return tRows
}

type testRows struct {
	rows [][]any
	row  []any
	err  error
}

func (rows *testRows) Err() error { return rows.err }
func (rows *testRows) Next() bool {
	if len(rows.rows) == 0 || rows.err != nil {
		return false
	}
	rows.row = rows.rows[0]
	rows.rows = rows.rows[1:]
	return true
}
func (rows *testRows) Scan(args ...any) error {
	if rows.err != nil {
		return rows.err
	}
	row := rows.row
	if rows.row == nil { // sql.Row does not call Next
		rows.Next()
		row = rows.row
	}
	if len(rows.row) != len(args) {
		return fmt.Errorf("got %d columns (%#v), wanted %d (%#v)",
			len(args), args, len(row), row,
		)
	}
	for i, a := range args {
		//slog.Info("scan", "i", i, "row[i]", row[i])
		v := reflect.ValueOf(row[i])
		if m, ok := row[i].(map[string]any); ok {
			//slog.Debug("map", "m", m, "i", fmt.Sprintf("%#v", m["Int32"]), "b", fmt.Sprintf("%#v", m["Valid"]))
			if i, ok := m["Int32"].(float64); ok {
				//slog.Debug("Int32", "i", i)
				if b, ok := m["Valid"].(bool); ok {
					reflect.ValueOf(a).Elem().Set(reflect.ValueOf(sql.NullInt32{Int32: int32(i), Valid: b}))
					continue
				}
			}
		} else if f, ok := row[i].(float64); ok {
			ok = true
			switch a.(type) {
			case *int, *int8, *int16, *int32, *int64:
				reflect.ValueOf(a).Elem().SetInt(int64(f))
			case *float32, *float64:
				reflect.ValueOf(a).Elem().SetFloat(f)
			case *uint, *uint8, *uint16, *uint32, *uint64:
				reflect.ValueOf(a).Elem().SetUint(uint64(f))
			default:
				ok = false
			}
			if ok {
				continue
			}
		}
		slog.Debug("Set", "from", fmt.Sprintf("%[1]T %#[1]v", v.Interface()), "to", fmt.Sprintf("%[1]T %#[1]v", a))
		reflect.ValueOf(a).Elem().Set(v)
	}
	return nil
}
func (rows testRows) Close() error {
	return nil
}
