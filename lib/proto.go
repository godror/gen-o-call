/*
Copyright 2019 Tamás Gulácsi

// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0
*/

package genocall

import (
	"bytes"
	"fmt"
	//"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode"

	fstructs "github.com/fatih/structs"
	"golang.org/x/exp/slog"
)

var (
	logger *slog.Logger

	SkipMissingTableOf = true

	NumberAsString bool
)

func SetLogger(lgr *slog.Logger) { logger = lgr }

// build: protoc --go_out=plugins=grpc:. my.proto

func SaveProtobuf(dst io.Writer, functions []Function, pkg string) error {
	var err error
	w := errWriter{Writer: dst, err: &err}

	io.WriteString(w, `syntax = "proto3";`+"\n\n")

	if pkg != "" {
		fmt.Fprintf(w, "package %s;\n", pkg)
	}
	seen := make(map[string]struct{}, 16)

	services := make([]string, 0, len(functions))

FunLoop:
	for _, fun := range functions {
		//b, _ := json.Marshal(struct{Name, Documentation string}{Name:fun.FullName(), Documentation:fun.Documentation})
		//fmt.Println(string(b))
		fName := fun.AliasedName()
		fName = strings.ToLower(fName)
		if err := fun.SaveProtobuf(w, seen); err != nil {
			if SkipMissingTableOf && (errors.Is(err, ErrMissingTableOf) ||
				errors.Is(err, UnknownSimpleType)) {
				logger.Warn("SKIP function, missing TableOf info", "function", fName)
				continue FunLoop
			}
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
	dirmap, dirname := DIR_IN, "input"
	if out {
		dirmap, dirname = DIR_OUT, "output"
	}
	args := make([]Argument, 0, len(f.Args)+1)
	for _, arg := range f.Args {
		if arg.Direction&dirmap > 0 {
			args = append(args, arg)
		}
	}
	// return variable for function out structs
	if out && f.Returns != nil {
		args = append(args, *f.Returns)
	}

	nm := f.Name
	if f.Alias != "" {
		nm = f.Alias
	}
	return protoWriteMessageTyp(dst,
		CamelCase(dot2D.Replace(strings.ToLower(nm))+"__"+dirname),
		seen, getDirDoc(f.Documentation, dirmap), args...)
}

var dot2D = strings.NewReplacer(".", "__")

func protoWriteMessageTyp(dst io.Writer, msgName string, seen map[string]struct{}, D argDocs, args ...Argument) error {
	for _, arg := range args {
		if arg.Flavor == FLAVOR_TABLE && arg.TableOf == nil {
			return fmt.Errorf("no table of data for %s.%s (%v): %w", msgName, arg, arg, ErrMissingTableOf)
		}
	}

	var err error
	w := &errWriter{Writer: dst, err: &err}
	fmt.Fprintf(w, "%smessage %s {\n", asComment(strings.TrimRight(D.Pre+D.Post, " \n\t"), ""), msgName)

	buf := Buffers.Get()
	defer Buffers.Put(buf)
	for i, arg := range args {
		var rule string
		if strings.HasSuffix(arg.Name, "#") {
			arg.Name = replHidden(arg.Name)
		}
		if arg.Flavor == FLAVOR_TABLE {
			if arg.TableOf == nil {
				return fmt.Errorf("no table of data for %s.%s (%v): %w", msgName, arg, arg, ErrMissingTableOf)
			}
			rule = "repeated "
		}
		aName := arg.Name
		got, err := arg.goType(false)
		if err != nil {
			return fmt.Errorf("%s: %w", msgName, err)
		}
		got = strings.TrimPrefix(got, "*")
		if strings.HasPrefix(got, "[]") {
			rule = "repeated "
			got = got[2:]
		}
		got = strings.TrimPrefix(got, "*")
		if got == "" {
			got = mkRecTypName(arg.Name)
		}
		typ, pOpts := protoType(got, arg.Name, arg.AbsType)
		var optS string
		if s := pOpts.String(); s != "" {
			optS = " " + s
		}
		if arg.Flavor == FLAVOR_SIMPLE || arg.Flavor == FLAVOR_TABLE && arg.TableOf.Flavor == FLAVOR_SIMPLE {
			fmt.Fprintf(w, "%s\t// %s\n\t%s%s %s = %d%s;\n", asComment(D.Map[aName], "\t"), arg.AbsType, rule, typ, aName, i+1, optS)
			continue
		}
		typ = CamelCase(typ)
		if _, ok := seen[typ]; !ok {
			seen[typ] = struct{}{}
			//lName := strings.ToLower(arg.Name)
			subArgs := make([]Argument, 0, 16)
			if arg.TableOf == nil {
				for _, v := range arg.RecordOf {
					subArgs = append(subArgs, *v.Argument)
				}
			} else {
				if arg.TableOf.RecordOf == nil {
					subArgs = append(subArgs, *arg.TableOf)
				} else {
					for _, v := range arg.TableOf.RecordOf {
						subArgs = append(subArgs, *v.Argument)
					}
				}
			}
			if err = protoWriteMessageTyp(buf, typ, seen, argDocs{Pre: D.Map[aName]}, subArgs...); err != nil {
				logger.Error("protoWriteMessageTyp", "error", err)
				return err
			}
		}
		fmt.Fprintf(w, "\t%s%s %s = %d%s;\n", rule, typ, aName, i+1, optS)
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
		if NumberAsString {
			return "sint32", protoOptions{
				"gogoproto.jsontag": aName + ",string,omitempty",
			}
		}
		return "sint32", nil
	case "float64", "sql.nullfloat64":
		if NumberAsString {
			return "double", protoOptions{
				"gogoproto.jsontag": aName + ",string,omitempty",
			}
		}
		return "double", nil

	case "godror.number":
		return "string", protoOptions{
			"gogoproto.jsontag": aName + ",omitempty",
		}

	case "custom.date", "time.time":
		return "google.protobuf.Timestamp", protoOptions{
			//"gogoproto.stdtime":    true,
			"gogoproto.customtype": "github.com/godror/gen-o-call/custom.DateTime",
			"gogoproto.moretags":   `xml:",omitempty"`,
		}
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

func CopyStruct(dest interface{}, src interface{}) error {
	ds := fstructs.New(dest)
	ss := fstructs.New(src)
	snames := ss.Names()
	svalues := ss.Values()
	for _, df := range ds.Fields() {
		dnm := df.Name()
		for i, snm := range snames {
			if snm == dnm || dnm == CamelCase(snm) || CamelCase(dnm) == snm {
				svalue := svalues[i]
				if err := df.Set(svalue); err != nil {
					return fmt.Errorf("set %q to %q (%v %T): %w", dnm, snm, svalue, svalue, err)
				}
			}
		}
	}
	return nil
}
func mkRecTypName(name string) string { return strings.ToLower(name) + "_rek_typ" }

func asComment(s, prefix string) string {
	return "\n" + prefix + "// " + strings.Replace(s, "\n", "\n"+prefix+"// ", -1) + "\n"
}

type argDocs struct {
	Pre, Post string
	Docs      []string
	Map       map[string]string
}

func (D *argDocs) Parse(doc string) {
	D.Docs = splitByOffset(doc)
	for _, line := range D.Docs {
		sline := strings.TrimSpace(line)
		if sline == "" || !(sline[0] == '-' || sline[0] == '*') {
			continue
		}
		sline = strings.TrimLeft(sline[1:], " \t")
		i := strings.IndexAny(sline, "-:")
		if i < 0 {
			continue
		}
		if D.Map == nil {
			D.Map = make(map[string]string)
		}
		D.Map[strings.TrimRight(sline[:i], " \t")] = strings.TrimLeft(sline[i+1:], " \t")
	}
}
func firstNotSpace(doc string) int {
	return strings.IndexFunc(doc, func(r rune) bool { return !unicode.IsSpace(r) })
}
