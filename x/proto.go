// Copyright 2019, 2034 Tamás Gulácsi. All rights reserved.

// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package x

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"unicode"

	"golang.org/x/exp/slog"
)

// go:generate sh ./download-protoc.sh
// go:generate go install github.com/bufbuild/buf/cmd/...@latest
// build: protoc --go_out=plugins=grpc:. my.proto

var UnknownScalarType = errors.New("unknown scalar type")

func SaveProtobuf(dst io.Writer, functions []Function, pkg string) error {
	var err error
	w := errWriter{Writer: dst, err: &err}

	io.WriteString(w, `syntax = "proto3";`+"\n\n")

	if pkg != "" {
		fmt.Fprintf(w, "package %s;\n", pkg)
	}
	io.WriteString(w, `import "google/protobuf/timestamp.proto";`+"\n")
	fmt.Fprintf(w, `option go_package = %q;`+"\n", pkg)

	seen := make(map[string]struct{}, 16)
	services := make([]string, 0, len(functions))

	for _, fun := range functions {
		//b, _ := json.Marshal(struct{Name, Documentation string}{Name:fun.FullName(), Documentation:fun.Documentation})
		//fmt.Println(string(b))
		fName := fun.AliasedName()
		fName = strings.ToLower(fName)
		if err := fun.SaveProtobuf(w, seen); err != nil {
			return fmt.Errorf("%s: %w", fun.Name, err)
		}
		var streamQual string
		if fun.HasCursorOut() {
			streamQual = "stream "
		}
		name := CamelCase(dot2D.Replace(fName))
		var comment string
		if fun.Documentation != "" {
			comment = asComment(fun.Documentation, "")
		}
		services = append(services,
			fmt.Sprintf(`%srpc %s (%s) returns (%s%s) {}`,
				comment,
				name,
				CamelCase(fun.getStructName(false, false)),
				streamQual,
				CamelCase(fun.getStructName(true, false)),
			),
		)
	}

	fmt.Fprintf(w, "\nservice %s {\n", CamelCase(pkg))
	for _, s := range services {
		fmt.Fprintf(w, "\t%s\n", s)
	}
	w.Write([]byte("}"))

	return nil
}

func (f Function) SaveProtobuf(dst io.Writer, seen map[string]struct{}) error {
	var buf bytes.Buffer
	if err := f.saveProtobufDir(&buf, seen, false); err != nil {
		return fmt.Errorf("%s: %w", "input", err)
	}
	if err := f.saveProtobufDir(&buf, seen, true); err != nil {
		return fmt.Errorf("%s: %w", "output", err)
	}
	_, err := dst.Write(buf.Bytes())
	return err
}
func (f Function) saveProtobufDir(dst io.Writer, seen map[string]struct{}, out bool) error {
	dirmap, dirname := DirIn, "Request"
	if out {
		dirmap, dirname = DirOut, "Response"
	}
	args := make([]Argument, 0, len(f.Args)+1)
	for _, arg := range f.Args {
		if arg.Direction&dirmap != 0 && arg.Name != "" {
			args = append(args, arg)
		}
	}
	// return variable for function out structs
	if out && f.Returns != nil {
		args = append(args, Argument{Attribute: *f.Returns})
	}
	slog.Info("saveProtobufDir", "fun", f.Name, "dir", dirmap, "args", len(args), "funArgs", len(f.Args))

	nm := f.Name
	if f.Alias != "" {
		nm = f.Alias
	}
	return protoWriteMessageTyp(dst,
		CamelCase(dot2D.Replace(strings.ToLower(nm))+"__"+dirname),
		seen, getDirDoc(f.Documentation, dirmap), args)
}

var dot2D = strings.NewReplacer(".", "__")

func (t Triplet) ProtoName(dirname string) string {
	var buf strings.Builder
	for _, nm := range []string{t.Owner, t.Package, t.Name, dirname} {
		if nm == "" {
			continue
		}
		if buf.Len() != 0 {
			buf.WriteString("__")
		}
		buf.WriteString(strings.ToLower(nm))
	}
	return CamelCase(buf.String())
}

func protoWriteMessageTyp(dst io.Writer, msgName string, seen map[string]struct{}, D argDocs, args []Argument) error {
	if _, ok := seen[msgName]; ok {
		return nil
	}
	seen[msgName] = struct{}{}

	var err error
	w := &errWriter{Writer: dst, err: &err}
	fmt.Fprintf(w, "%smessage %s {\n", asComment(strings.TrimRight(D.Pre+D.Post, " \n\t"), ""), msgName)

	buf := Buffers.Get()
	defer Buffers.Put(buf)
	{
		b, _ := json.Marshal(args)
		fmt.Fprintf(w, "// %s.args=[%d] %s\n", msgName, len(args), string(b))
	}
	for i, arg := range args {
		var rule string
		if arg.Type.IsCollection {
			rule = "repeated "
		}
		if strings.HasSuffix(arg.Name, "#") {
			arg.Name = replHidden(arg.Name)
		}
		aName := arg.Name
		if aName == "" {
			aName = "ret"
		}

		if arg.Type.IsObject && !arg.Type.IsCollection || arg.Type.IsCollection && arg.Type.CollectionOf.IsObject {
			var o Object
			if arg.Type.IsCollection && arg.Type.CollectionOf.IsObject {
				o = arg.Type.CollectionOf.Object
			} else {
				o = arg.Type.Object
			}
			subArgs := make([]Argument, len(o.Attributes))
			for i, v := range o.Attributes {
				subArgs[i] = Argument{Attribute: v, Direction: arg.Direction}
			}
			subName := o.Triplet.ProtoName("")
			if err := protoWriteMessageTyp(buf, subName, seen, D, subArgs); err != nil {
				return err
			}

			fmt.Fprintf(w, "\t%s%s %s = %d; //b\n", rule, subName, aName, i+1)
			continue
		}

		// simple type, maybe collection
		got, err := arg.goType()
		if err != nil {
			return fmt.Errorf("%s: %w", msgName, err)
		}
		got = strings.TrimPrefix(got, "*")
		if strings.HasPrefix(got, "[]") {
			rule = "repeated "
			got = got[2:]
		}
		got = strings.TrimPrefix(got, "*")
		typ, pOpts := protoType(got, arg.Name, arg.AbsType)
		var optS string
		if s := pOpts.String(); s != "" {
			optS = " " + s
		}
		if !arg.Type.IsCollection {
			fmt.Fprintf(w, "%s\t// %s\n\t%s%s %s = %d%s; //b\n", asComment(D.Map[aName], "\t"), arg.AbsType, rule, typ, aName, i+1, optS)
			continue
		}

		fmt.Fprintf(w, "\t%s%s %s = %d%s; //c\n", rule, typ, aName, i+1, optS)
	}
	io.WriteString(w, "}\n")
	w.Write(buf.Bytes())

	return err
}

func protoType(got, aName, absType string) (string, protoOptions) {
	switch trimmed := strings.ToLower(strings.TrimPrefix(strings.TrimPrefix(got, "[]"), "*")); trimmed {
	case "string":
		return "string", nil

	case "int32":
		return "sint32", nil
	case "float64", "sql.nullfloat64":
		return "double", nil

	case "godror.number":
		return "string", nil //protoOptions{ "gogoproto.jsontag": aName + ",omitempty", }

	case "custom.date", "time.time":
		return "google.protobuf.Timestamp", nil /*protoOptions{
			//"gogoproto.stdtime":    true,
			"gogoproto.customtype": "github.com/godror/gen-o-call/custom.DateTime",
			"gogoproto.moretags":   `xml:",omitempty"`,
		}*/
	case "n":
		return "string", nil
	case "raw":
		return "bytes", nil
	case "godror.lob", "ora.lob":
		if absType == "CLOB" {
			return "string", nil
		}
		return "bytes", nil
	default:
		return trimmed, nil
	}
}

type protoOptions map[string]interface{}

func (opts protoOptions) String() string {
	if len(opts) == 0 {
		return ""
	}
	var buf bytes.Buffer
	buf.WriteByte('[')
	for k, v := range opts {
		if buf.Len() != 1 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "(%s)=", k)
		switch v.(type) {
		case bool:
			fmt.Fprintf(&buf, "%t", v)
		default:
			fmt.Fprintf(&buf, "%q", v)
		}
	}
	buf.WriteByte(']')
	return buf.String()
}

func mkRecTypName(name string) string { return strings.ToLower(name) + "_rt" }

func asComment(s, prefix string) string {
	return "\n" + prefix + "// " + strings.Replace(s, "\n", "\n"+prefix+"// ", -1) + "\n"
}

type errWriter struct {
	io.Writer
	err *error
}

func (ew errWriter) Write(p []byte) (int, error) {
	if ew.err != nil && *ew.err != nil {
		return 0, *ew.err
	}
	n, err := ew.Writer.Write(p)
	if err != nil {
		*ew.err = err
	}
	return n, err
}

func replHidden(text string) string {
	if text == "" {
		return text
	}
	if text[len(text)-1] == '#' {
		return text[:len(text)-1] + MarkHidden
	}
	return text
}

var digitUnder = strings.NewReplacer(
	"_0", "__0",
	"_1", "__1",
	"_2", "__2",
	"_3", "__3",
	"_4", "__4",
	"_5", "__5",
	"_6", "__6",
	"_7", "__7",
	"_8", "__8",
	"_9", "__9",
)

func CamelCase(text string) string {
	text = replHidden(text)
	if text == "" {
		return text
	}
	var prefix string
	if text[0] == '*' {
		prefix, text = "*", text[1:]
	}

	text = digitUnder.Replace(text)
	var last rune
	return prefix + strings.Map(func(r rune) rune {
		defer func() { last = r }()
		if r == '_' {
			if last != '_' {
				return -1
			}
			return '_'
		}
		if last == 0 || last == '_' || last == '.' || '0' <= last && last <= '9' {
			return unicode.ToUpper(r)
		}
		return unicode.ToLower(r)
	},
		text,
	)
}
func (f Function) getPlsqlConstName() string {
	nm := f.AliasedName()
	return capitalize(f.Package + "__" + nm + "__plsql")
}

func (f Function) getStructName(out, withPackage bool) string {
	dirname := "Request"
	if out {
		dirname = "Response"
	}
	nm := f.AliasedName()
	if !withPackage {
		return nm + "__" + dirname
	}
	return capitalize(f.Package+"__"+nm) + "__" + dirname
}

var SBuffers = newSBufPool()
var Buffers = newBufPool(1 << 16)

type bufPool struct {
	sync.Pool
}

func newBufPool(size int) *bufPool {
	return &bufPool{sync.Pool{New: func() interface{} { return bytes.NewBuffer(make([]byte, 0, 1<<16)) }}}
}
func (bp *bufPool) Get() *bytes.Buffer {
	return bp.Pool.Get().(*bytes.Buffer)
}
func (bp *bufPool) Put(b *bytes.Buffer) {
	if b == nil {
		return
	}
	b.Reset()
	bp.Pool.Put(b)
}

type sbufPool struct {
	sync.Pool
}

func newSBufPool() *sbufPool {
	return &sbufPool{sync.Pool{New: func() interface{} { return &strings.Builder{} }}}
}
func (bp *sbufPool) Get() *strings.Builder {
	return bp.Pool.Get().(*strings.Builder)
}
func (bp *sbufPool) Put(b *strings.Builder) {
	if b == nil {
		return
	}
	b.Reset()
	bp.Pool.Put(b)
}

var rIdentifier = regexp.MustCompile(`:([0-9a-zA-Z][a-zA-Z0-9_]*)`)

func (arg *Argument) goType() (typName string, err error) {
	defer func() {
		if strings.HasPrefix(typName, "**") {
			typName = typName[1:]
		}
	}()
	// cached?
	if arg.goTypeName != "" {
		if strings.Index(arg.goTypeName, "__") > 0 {
			return "*" + arg.goTypeName, nil
		}
		return arg.goTypeName, nil
	}
	defer func() {
		// cache it
		arg.goTypeName = typName
	}()
	if arg.Type.IsScalar() {
		typ := arg.Type.PlsType
		if typ == "" {
			typ = arg.Type.Name
		}
		switch typ {
		case "CHAR", "VARCHAR2", "ROWID":
			if !arg.Type.IsCollection && arg.IsOutput() {
				//return "*string", nil
				return "string", nil
			}
			return "string", nil // NULL is the same as the empty string for Oracle
		case "RAW":
			return "[]byte", nil
		case "NUMBER":
			return "godror.Number", nil
		case "INTEGER":
			if !arg.Type.IsCollection && arg.IsOutput() {
				return "*int64", nil
			}
			return "int64", nil
		case "PL/SQL PLS INTEGER", "PLS_INTEGER", "BINARY_INTEGER":
			if !arg.Type.IsCollection && arg.IsOutput() {
				//return "*int32", nil
				return "int32", nil
			}
			return "int32", nil
		case "BOOLEAN", "PL/SQL BOOLEAN":
			if !arg.Type.IsCollection && arg.IsOutput() {
				return "*bool", nil
			}
			return "bool", nil
		case "DATE", "DATETIME", "TIME", "TIMESTAMP":
			return "time.Time", nil
		case "REF CURSOR":
			return "*sql.Rows", nil
		case "BLOB":
			return "[]byte", nil
		case "CLOB":
			return "string", nil
		case "BFILE":
			return "ora.Bfile", nil
		default:
			return "", fmt.Errorf("%#v: %w", arg, UnknownScalarType)
		}
	}
	typName = arg.Type.Name
	chunks := strings.Split(typName, ".")
	switch len(chunks) {
	case 1:
	case 2:
		typName = chunks[1] + "__" + chunks[0]
	default:
		typName = strings.Join(chunks[1:], "__") + "__" + chunks[0]
	}
	//typName = goName(capitalize(typName))
	typName = capitalize(typName)

	if arg.Type.IsCollection {
		//Log("msg", "TABLE", "arg", arg, "tableOf", arg.TableOf)
		targ := Argument{Attribute: Attribute{Type: *arg.Type.CollectionOf}, Direction: DirIn}
		tn, err := targ.goType()
		if err != nil {
			return tn, err
		}
		tn = "[]" + tn
		if arg.Type.Name != "REF CURSOR" {
			if arg.IsOutput() && !arg.Type.CollectionOf.IsObject && !arg.Type.CollectionOf.IsCollection {
				return "*" + tn, nil
			}
			return tn, nil
		}
		cn := tn[2:]
		if cn[0] == '*' {
			cn = cn[1:]
		}
		return cn, nil
	}

	return "*" + typName, nil
}
