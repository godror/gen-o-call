/*
Copyright 2015 Tamás Gulácsi

// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0
*/

package genocall

import (
	"strings"
	"testing"
)

//var flagConnect = flag.String("connect", "", "database DSN to connect to")

func TestParseCsv(t *testing.T) {
	for i, tc := range testCases {
		functions, err := ParseCsv(strings.NewReader(tc.Csv), nil)
		if err != nil {
			t.Errorf("%d. error parsing csv: %v", i, err)
			t.FailNow()
		}
		if len(functions) != 1 {
			t.Errorf("%d. parsed %d functions, wanted %d!", i, len(functions), 1)
		}
	}
}
