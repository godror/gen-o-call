package x

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/godror/godror"
	"github.com/kr/pretty"
)

var flagConnect = flag.String("connect", os.Getenv("BRUNO_ID"), "DB to connect to")

func TestReadDB(t *testing.T) {
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
	//defer db.ExecContext(context.Background(), `DROP PACKAGE GT_gen_o_call`)
	{
		for _, qry := range []string{
			`CREATE OR REPLACE PACKAGE GT_gen_o_call IS
TYPE obj_rt IS RECORD (
  F_int PLS_INTEGER,
  F_dt  DATE,
  F_num NUMBER,
  F_clob CLOB,
  F_str VARCHAR2(1000)
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

	funcs, err := ReadPackage(ctx, &DB{DB: db}, "GT_gen_o_call")
	if err != nil {
		t.Fatal(err)
	}
	t.Log("funcs", pretty.Sprint(funcs))
}

func TestParseCSV(t *testing.T) {
}
