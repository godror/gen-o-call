/*
Copyright 2013 Tamás Gulácsi

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/tgulacsi/goracle/oracle"
)

// TestGen tests the generation - for this, it needs a dsn with privileges
// if you get "ORA-01031: insufficient privileges", then you need
// GRANT CREATE PROCEDURE TO username;
func TestGen(t *testing.T) {
	conn := getConnection(t)

	cu := conn.NewCursor()
	defer cu.Close()

	err := cu.Execute(`CREATE OR REPLACE PACKAGE TST_oracall AS
PROCEDURE char_in(txt IN VARCHAR2);
FUNCTION char_out RETURN VARCHAR2;
PROCEDURE num_in(num IN NUMBER);
FUNCTION num_out RETURN NUMBER;
PROCEDURE date_in(dat IN DATE);
FUNCTION date_out RETURN DATE;
FUNCTION char_in_char_ret(txt IN VARCHAR2) RETURN VARCHAR2;
PROCEDURE all_inout(
    txt1 IN VARCHAR2, int1 IN PLS_INTEGER, num1 IN NUMBER, dt1 IN DATE,
    txt2 OUT VARCHAR2, int2 OUT PLS_INTEGER, num2 OUT NUMBER, dt2 OUT DATE,
    txt3 IN OUT VARCHAR2, int3 IN OUT PLS_INTEGER, num3 IN OUT NUMBER, dt3 IN OUT DATE);
END TST_oracall;
    `, nil, nil)
	if err != nil {
		t.Fatalf("error creating package head: %v", err)
	}
	if err = cu.Execute(`CREATE OR REPLACE PACKAGE BODY TST_oracall AS
PROCEDURE char_in(txt IN VARCHAR2) IS
  v_txt VARCHAR2(1000) := SUBSTR(txt, 1, 100);
BEGIN NULL; END char_in;
FUNCTION char_out RETURN VArCHAR2 IS BEGIN RETURN('A'); END char_out;

PROCEDURE num_in(num IN NUMBER) IS
  v_num NUMBER := num;
BEGIN NULL; END num_in;

PROCEDURE date_in(dat IN DATE) IS
  v_dat DATE := dat;
BEGIN NULL; END date_in;

FUNCTION char_in_char_ret(txt IN VARCHAR2) RETURN VARCHAR2 IS
  v_txt CONSTANT VARCHAR2(4000) := SUBSTR(txt, 1, 4000);
  v_ret VARCHAR2(4000);
BEGIN
  SELECT DUMP(txt) INTO v_ret FROM DUAL;
  RETURN v_ret;
END char_in_char_ret;

FUNCTION date_out RETURN DATE IS BEGIN RETURN SYSDATE; END date_out;
FUNCTION num_out RETURN NUMBER IS BEGIN RETURN 2/3; END num_out;

PROCEDURE all_inout(
    txt1 IN VARCHAR2, int1 IN PLS_INTEGER, num1 IN NUMBER, dt1 IN DATE,
    txt2 OUT VARCHAR2, int2 OUT PLS_INTEGER, num2 OUT NUMBER, dt2 OUT DATE,
    txt3 IN OUT VARCHAR2,
    int3 IN OUT PLS_INTEGER, num3 IN OUT NUMBER, dt3 IN OUT DATE) IS
BEGIN
  txt2 := txt1||'#'; int2 := NVL(int1, 0) + 1;
  num2 := NVL(num1, 0) + 1/3; dt2 := ADD_MONTHS(NVL(dt1, SYSDATE), 1);
  txt3 := txt3||'#'; int3 := NVL(int3, 0) + 1;
  num3 := NVL(num3, 0) + 1; dt3 := ADD_MONTHS(NVL(dt3, SYSDATE), 1);
END all_inout;

END TST_oracall;
    `, nil, nil); err != nil {
		t.Fatalf("error creating package body: %v", err)
	}
	if err = cu.Execute("SELECT text FROM user_errors WHERE name = :1", []interface{}{"TST_ORACALL"}, nil); err != nil {
		t.Fatalf("error querying errors: %v", err)
	}
	rows, err := cu.FetchAll()
	if err != nil {
		t.Fatalf("error fetching errors: %v", err)
	}
	if len(rows) > 0 {
		errTexts := make([]string, len(rows))
		for i := range rows {
			errTexts[i] = rows[i][0].(string)
		}
		t.Fatalf("error with package: %s", strings.Join(errTexts, "\n"))
	}

	var (
		out   []byte
		outFn string
	)
	run := func(prog string, args ...string) {
		if out, err = exec.Command(prog, args...).CombinedOutput(); err != nil {
			t.Errorf("error '%q %s': %v\n%s", prog, args, err, out)
			t.FailNow()
		} else {
			t.Logf("%q %s:\n%s", prog, args, out)
		}
	}

	run("go", "build")
	run("sh", "-c", "./oracall -F -connect='"+*dsn+"' TST_ORACALL.% > ./testdata/integration_test/generated_functions.go")

	if outFh, err := ioutil.TempFile("", "oracall-integration_test"); err != nil {
		t.Errorf("cannot create temp file: %v", err)
		t.FailNow()
	} else {
		outFn = outFh.Name()
		outFh.Close()
	}
	os.Remove(outFn)
	run("go", "build", "-o="+outFn, "./testdata/integration_test")

	for _, todo := range [][2]string{
		{"char_in", `{"txt": "abraka dabra"}`},
		{"char_out", `{}`},
		{"num_in", `{"num": 33}`},
		{"num_out", `{}`},
		{"date_in", `{"dat": "2013-12-25T21:15:00+01:00"}`},
		{"date_out", `{}`},
		{"char_in_char_ret", `{"txt": "abraka dabra"}`},
		{"all_inout", `{"txt1": "abraka", "txt3": "A", "int1": -1, "int3": -2, "num1": 0.1, "num3": 0.3, "dt1": null}`},
	} {
		run(outFn, "-connect="+*dsn, "TST_oracall."+todo[0], todo[1])
	}
}

var dsn = flag.String("dsn", "", "Oracle DSN (user/passw@sid)")
var dbg = flag.Bool("debug", false, "print debug messages?")

func init() {
	flag.Parse()
}

var conn oracle.Connection

func getConnection(t *testing.T) oracle.Connection {
	if conn.IsConnected() {
		return conn
	}

	if !(dsn != nil && *dsn != "") {
		t.Logf("cannot test connection without dsn!")
		return conn
	}
	user, passw, sid := oracle.SplitDSN(*dsn)
	var err error
	conn, err = oracle.NewConnection(user, passw, sid, false)
	if err != nil {
		log.Panicf("error creating connection to %s: %s", *dsn, err)
	}
	if err = conn.Connect(0, false); err != nil {
		log.Panicf("error connecting: %s", err)
	}
	return conn
}