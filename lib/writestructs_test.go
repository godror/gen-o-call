/*
Copyright 2015 Tamás Gulácsi

// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0
*/

package genocall

import (
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
