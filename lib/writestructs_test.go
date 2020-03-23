/*
Copyright 2015 Tamás Gulácsi

// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0
*/

package genocall

import (
	"bytes"
	"errors"
	"flag"
	"testing"
)

var flagKeep = flag.Bool("keep", false, "keep temp files")

func TestGoName(t *testing.T) {
	for eltNum, elt := range [][2]string{
		{"a", "A"},
		{"a_b", "AB"},
		{"a__b", "A_B"},
		{"db_web__calculate_31101__input", "DbWeb_Calculate_31101_Input"},
		{"db_web__calculate_242xx__output", "DbWeb_Calculate_242Xx_Output"},
		{"Db_dealer__zaradek_rec_typ__bruno", "DbDealer_ZaradekRecTyp_Bruno"},
		{"*Db_dealer__zaradek_rec_typ__bruno", "*DbDealer_ZaradekRecTyp_Bruno"},
	} {
		if got := CamelCase(elt[0]); got != elt[1] {
			t.Errorf("%d. %q => got %q, awaited %q.", eltNum, elt[0], got, elt[1])
		}
	}
}
func TestJSONSaveFunctions(t *testing.T) {
	funcs := readJSONFuncs(nil, t)
	for i := range funcs {
		f := funcs[i]
		t.Logf("%+v", f)
		t.Run(f.FullName(), func(t *testing.T) {
			var buf bytes.Buffer
			if err := SaveFunctions(&buf, []Function{f}, f.Package, "test", true); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestJSONSaveStruct(t *testing.T) {
	funcs := readJSONFuncs(nil, t)

	var buf bytes.Buffer
	for _, fun := range funcs {
		for _, dir := range []bool{false, true} {
			buf.Reset()
			if err := fun.SaveStruct(&buf, dir); err != nil {
				if errors.Is(err, ErrMissingTableOf) || errors.Is(err, UnknownSimpleType) {
					t.Error("msg", "SKIP function, missing TableOf info", "function", fun.FullName(), "error", err)
				}
				t.Fatal(err)
			}
		}
	}
}

func TestJSONPlsqlBlock(t *testing.T) {
	funcs := readJSONFuncs(nil, t)
	for _, fun := range funcs {
		plsBlock, callFun := fun.PlsqlBlock(fun.Name)
		t.Log(plsBlock, callFun)
	}
}
