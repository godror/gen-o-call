// Copyright 2017, 2023 Tamás Gulácsi
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package x

import (
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"io"
	"strings"
)

// FromOra retrieves the value of the argument with arg type, from src variable to dst variable.
func (arg Attribute) FromOra(dst, src, varName string) string {
	switch arg.Name {
	case "BLOB":
		if varName != "" {
			return fmt.Sprintf("{ if %s.Reader != nil { %s, err = ioutil.ReadAll(%s) }", varName, dst, varName)
		}
		return fmt.Sprintf("%s = godror.Lob{Reader:bytes.NewReader(%s)}", dst, src)
	case "CLOB":
		if varName != "" {
			return fmt.Sprintf("{var b []byte; if %s.Reader != nil {b, err = ioutil.ReadAll(%s); %s = string(b)}}", varName, varName, dst)
		}
		return fmt.Sprintf("%s = godror.Lob{IsClob:true, Reader:strings.NewReader(%s)}", dst, src)
	case "DATE", "TIMESTAMP":
		return fmt.Sprintf("%s = (%s)", dst, src)
	case "PLS_INTEGER":
		return fmt.Sprintf("%s = int32(%s)", dst, src)
	case "NUMBER":
		return fmt.Sprintf("%s = string(%s)", dst, src)
	case "":
		panic(fmt.Sprintf("empty \"ora\" type: %#v", arg))
	}
	return fmt.Sprintf("%s = %s // %s fromOra", dst, src, arg.Name)
}

// ToOra adds the value of the argument with arg type, from src variable to dst variable.
func (arg Attribute) ToOra(dst, src string, dir direction) (expr string, variable string) {
	dstVar := mkVarName(dst)
	var inTrue string
	if dir.IsInput() {
		inTrue = ",In:true"
	}
	switch arg.Name {
	case "PLS_INTEGER":
		if src[0] != '&' {
			return fmt.Sprintf("var %s sql.NullInt64; if %s != 0 { %s.Int64, %s.Valid = int64(%s), true }; %s = int32(%s.Int64)", dstVar, src, dstVar, dstVar, src, dst, dstVar), dstVar
		}
	case "NUMBER":
		if src[0] != '&' {
			return fmt.Sprintf("%s := godror.Number(%s); %s = %s", dstVar, src, dst, dstVar), dstVar
		}
	case "CLOB":
		if dir.IsOutput() {
			return fmt.Sprintf("%s := godror.Lob{IsClob:true}; %s = sql.Out{Dest:&%s}", dstVar, dst, dstVar), dstVar
		}
		return fmt.Sprintf("%s := godror.Lob{IsClob:true,Reader:strings.NewReader(%s)}; %s = %s", dstVar, src, dst, dstVar), dstVar
	}
	if dir.IsOutput() && !(strings.HasSuffix(dst, "]") && !strings.HasPrefix(dst, "params[")) {
		if arg.Name == "NUMBER" {
			return fmt.Sprintf("%s = sql.Out{Dest:(*godror.Number)(unsafe.Pointer(%s))%s} // NUMBER",
				dst, src, inTrue), ""
		}
		return fmt.Sprintf("%s = sql.Out{Dest:%s%s} // %s", dst, src, inTrue, arg.Name), ""
	}
	return fmt.Sprintf("%s = %s // %s", dst, src, arg.Name), ""
}
func mkVarName(dst string) string {
	h := fnv.New64()
	io.WriteString(h, dst)
	var raw [8]byte
	var enc [8 * 2]byte
	hex.Encode(enc[:], h.Sum(raw[:0]))
	return fmt.Sprintf("var_%s", enc[:])
}
