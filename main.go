/*
Copyright 2017 Tamás Gulácsi

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
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"golang.org/x/sync/errgroup"

	"github.com/go-kit/kit/log"
	custom "github.com/godror/gen-o-call/custom"
	genocall "github.com/godror/gen-o-call/lib"
	"github.com/tgulacsi/go/loghlp/kitloghlp"
	errors "golang.org/x/xerrors"

	// for Oracle-specific drivers
	godror "github.com/godror/godror"
)

//go:generate go generate ./lib
// Should install protobuf-compiler to use it, like
// curl -L https://github.com/google/protobuf/releases/download/v3.0.0-beta-2/protoc-3.0.0-beta-2-linux-x86_64.zip -o /tmp/protoc-3.0.0-beta-2-linux-x86_64.zip && unzip -p /tmp/protoc-3.0.0-beta-2-linux-x86_64.zip protoc >$HOME/bin/protoc

var logger = kitloghlp.New(os.Stderr)

var flagConnect = flag.String("connect", "", "connect to DB for retrieving function arguments")

func main() {
	genocall.Log = log.With(logger, "lib", "genocall").Log
	if err := Main(os.Args); err != nil {
		logger.Log("error", fmt.Sprintf("%+v", err))
		os.Exit(1)
	}
}

func Main(args []string) error {
	os.Args = args

	gopSrc := filepath.Join(os.Getenv("GOPATH"), "src")

	flag.BoolVar(&genocall.SkipMissingTableOf, "skip-missing-table-of", true, "skip functions with missing TableOf info")
	flagBaseDir := flag.String("base-dir", gopSrc, "base dir for the -pb-out, -db-out flags")
	flagPbOut := flag.String("pb-out", "", "package import path for the Protocol Buffers files, optionally with the package name, like \"my/pb-pkg:main\"")
	flagDbOut := flag.String("db-out", "-:main", "package name of the generated functions, optionally with the package name, like \"my/db-pkg:main\"")
	flagGenerator := flag.String("protoc-gen", "gogofast", "use protoc-gen-<generator>")
	flag.BoolVar(&genocall.NumberAsString, "number-as-string", false, "add ,string to json tags")
	flag.BoolVar(&custom.ZeroIsAlmostZero, "zero-is-almost-zero", false, "zero should be just almost zero, to distinguish 0 and non-set field")
	flagVerbose := flag.Bool("v", false, "verbose logging")
	flagExcept := flag.String("except", "", "except these functions")
	flagReplace := flag.String("replace", "", "funcA=>funcB")
	flag.IntVar(&genocall.MaxTableSize, "max-table-size", genocall.MaxTableSize, "maximum table size for PL/SQL associative arrays")

	flag.Parse()
	if *flagPbOut == "" {
		if *flagDbOut == "" {
			return errors.New("-pb-out or -db-out is required!")
		}
		*flagPbOut = *flagDbOut
	} else if *flagDbOut == "" {
		*flagDbOut = *flagPbOut
	}
	pbPath, pbPkg := parsePkgFlag(*flagPbOut)
	dbPath, dbPkg := parsePkgFlag(*flagDbOut)

	Log := logger.Log
	pattern := flag.Arg(0)
	if pattern == "" {
		pattern = "%"
	}
	genocall.Gogo = *flagGenerator != "go"

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var functions []genocall.Function
	var err error

	filters := [](func(string) bool){func(string) bool { return true }}
	filter := func(s string) bool {
		for _, f := range filters {
			if !f(s) {
				return false
			}
		}
		return true
	}
	if *flagExcept != "" {
		except := strings.FieldsFunc(*flagExcept, func(r rune) bool { return r == ',' || unicode.IsSpace(r) })
		Log("except", except)
		filters = append(filters, func(s string) bool {
			for _, e := range except {
				if strings.EqualFold(e, s) {
					return false
				}
			}
			return true
		})
	}

	var annotations []genocall.Annotation
	db, err := sql.Open("godror", *flagConnect)
	if err != nil {
		return errors.Errorf("connect to %s: %w", *flagConnect, err)
	}
	defer db.Close()
	if *flagVerbose {
		godror.Log = log.With(logger, "lib", "godror").Log
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	functions, annotations, err = parseDB(ctx, tx, pattern, filter)
	if err != nil {
		return errors.Errorf("read %s: %w", flag.Arg(0), err)
	}

	defer os.Stdout.Sync()
	out := os.Stdout
	var testOut *os.File
	if dbPath != "" && dbPath != "-" {
		fn := "genocall.go"
		if dbPkg != "main" {
			fn = dbPkg + ".go"
		}
		fn = filepath.Join(*flagBaseDir, dbPath, fn)
		Log("msg", "Writing generated functions", "file", fn)
		os.MkdirAll(filepath.Dir(fn), 0775)
		if out, err = os.Create(fn); err != nil {
			return errors.Errorf("create %s: %w", fn, err)
		}
		testFn := fn[:len(fn)-3] + "_test.go"
		if testOut, err = os.Create(testFn); err != nil {
			return errors.Errorf("create %s: %w", testFn, err)
		}
		defer func() {
			if err := out.Close(); err != nil {
				Log("msg", "close", "file", out.Name(), "error", err)
			}
			if err := testOut.Close(); err != nil {
				Log("msg", "close", "file", testOut.Name(), "error", err)
			}
		}()
	}

	*flagReplace = strings.TrimSpace(*flagReplace)
	for _, elt := range strings.FieldsFunc(
		rReplace.ReplaceAllLiteralString(*flagReplace, "=>"),
		func(r rune) bool { return r == ',' || unicode.IsSpace(r) }) {
		i := strings.Index(elt, "=>")
		if i < 0 {
			continue
		}
		a := genocall.Annotation{Type: "replace", Name: elt[:i], Other: elt[i+2:]}
		if i = strings.IndexByte(a.Name, '.'); i >= 0 {
			a.Package, a.Name = a.Name[:i], a.Name[i+1:]
			a.Other = strings.TrimPrefix(a.Other, a.Package)
		}
		annotations = append(annotations, a)
	}
	Log("annotations", annotations)
	functions = genocall.ApplyAnnotations(functions, annotations)
	sort.Slice(functions, func(i, j int) bool { return functions[i].Name() < functions[j].Name() })

	var grp errgroup.Group
	grp.Go(func() error {
		pbPath := pbPath
		if pbPath == dbPath {
			pbPath = ""
		}
		if err := genocall.SaveFunctions(
			out, functions,
			dbPkg, pbPath, false,
		); err != nil {
			return errors.Errorf("save functions: %w", err)
		}
		return nil
	})
	if testOut != nil {
		grp.Go(func() error {
			pbPath := pbPath
			if pbPath == dbPath {
				pbPath = ""
			}
			if err := genocall.SaveFunctionTests(
				testOut, functions,
				dbPkg, pbPath, false,
			); err != nil {
				return errors.Errorf("save function tests: %w", err)
			}
			return nil
		})
	}

	grp.Go(func() error {
		fn := "genocall.proto"
		if pbPkg != "main" {
			fn = pbPkg + ".proto"
		}
		fn = filepath.Join(*flagBaseDir, pbPath, fn)
		os.MkdirAll(filepath.Dir(fn), 0775)
		Log("msg", "Writing Protocol Buffers", "file", fn)
		fh, err := os.Create(fn)
		if err != nil {
			return errors.Errorf("create proto: %w", err)
		}
		err = genocall.SaveProtobuf(fh, functions, pbPkg)
		if closeErr := fh.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		if err != nil {
			return errors.Errorf("SaveProtobuf: %w", err)
		}

		goOut := *flagGenerator + "_out"
		cmd := exec.Command(
			"protoc",
			"--proto_path="+*flagBaseDir+":.",
			"--"+goOut+"=Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,plugins=grpc:"+*flagBaseDir,
			fn,
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return errors.Errorf("%q: %w", cmd.Args, err)
		}
		return nil
	})

	if err := grp.Wait(); err != nil {
		return err
	}
	return nil
}

type dbRow struct {
	Owner, Package, Object, InOut string
	dbType
}

func (r dbRow) String() string {
	return fmt.Sprintf("%s.%s %s", r.Package, r.Object, r.dbType)
}

type dbType struct {
	Argument                                       string
	Data, PLS, Owner, Name, Subname, Link, Charset string
	Level                                          int
	Prec, Scale, Length                            sql.NullInt64
}

func (t dbType) String() string {
	return fmt.Sprintf("%s{%s}[%d](%s/%s.%s.%s@%s)", t.Argument, t.Data, t.Level, t.PLS, t.Owner, t.Name, t.Subname, t.Link)
}

func parseDB(ctx context.Context, db *sql.Tx, pattern string, filter func(string) bool) (functions []genocall.Function, annotations []genocall.Annotation, err error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	const objTimeQry = `SELECT last_ddl_time FROM all_objects WHERE object_name = :1 AND object_type <> 'PACKAGE BODY'`

	objTimeStmt, err := db.PrepareContext(ctx, objTimeQry)
	if err != nil {
		return nil, nil, errors.Errorf("%s: %w", objTimeQry, err)
	}
	defer objTimeStmt.Close()

	getObjTime := func(name string) (time.Time, error) {
		var t time.Time
		if err := objTimeStmt.QueryRowContext(ctx, name).Scan(&t); err != nil {
			return t, errors.Errorf("%s [%q]: %w", objTimeQry, name, err)
		}
		return t, nil
	}

	tr, err := newTypeResolver(ctx, db)
	if err != nil {
		return nil, nil, err
	}
	defer tr.Close()

	const argumentsQry = `SELECT owner, package_name, object_name,
           argument_name, in_out,
           data_type, data_precision, data_scale, character_set_name,
           pls_type, char_length, type_owner, type_name, type_subname, type_link
      FROM all_arguments
      WHERE package_name||'.'||object_name LIKE UPPER(:pat)
      ORDER BY 1, 2, 3, sequence`
	rows, err := db.QueryContext(ctx, argumentsQry, sql.Named("pat", pattern))
	if err != nil {
		logger.Log("qry", argumentsQry, "error", err)
		return nil, nil, errors.Errorf("%s: %w", argumentsQry, err)
	}
	defer rows.Close()

	docs := make(map[string]string)
	userArgs := make(chan genocall.UserArgument, 16)
	for rows.Next() {
		var row dbRow
		if err = rows.Scan(&row.Owner, &row.Package, &row.Object,
			&row.Level, &row.Argument, &row.InOut,
			&row.Data, &row.Prec, &row.Scale, &row.Charset,
			&row.PLS, &row.Length, &row.Owner, &row.Name, &row.Subname, &row.Link,
		); err != nil {
			return nil, nil, errors.Errorf("reading row=%v: %w", rows, err)
		}
		if row.Data == "OBJECT" || row.Data == "PL/SQL TABLE" || row.Data == "PL/SQL RECORD" || row.Data == "REF CURSOR" || row.Data == "TABLE" {
			plus, err := tr.Resolve(ctx, row.Data, row.Owner, row.Name, row.Subname)
			if err != nil {
				return nil, nil, err
			}
			if plus, err = tr.ExpandArgs(ctx, plus); err != nil {
				return nil, nil, err
			}
			for _, p := range plus {
				row.Argument, row.Data, row.Length, row.Prec, row.Scale, row.Charset = p.Argument, p.Data, p.Length, p.Prec, p.Scale, p.Charset
				row.Owner, row.Name, row.Subname, row.Link = p.Owner, p.Name, p.Subname, p.Link
				row.Level = p.Level
				//logger.Log("arg", row.Argument, "row", row.Length, "p", p.Length)
			}
				if row.Name == "" {
					row.PLS = row.Data
				} else {
					row.PLS = row.Owner + "." + row.Name + "." + row.Subname
					if row.Link != "" {
						row.PLS += "@" + row.Link
					}
				}
		}

	}
	if err != nil {
		return nil, nil, errors.Errorf("walking rows: %w", err)
	}

	var prevPackage string
	var docsMu sync.Mutex
	var replMu sync.Mutex
				//logger.Log("arg", row.Argument, "name", row.Name, "sub", row.Subname, "data", row.Data, "pls", row.PLS)
			}
			//logger.Log("row", row)
			var ua genocall.UserArgument
			ua.DataType = row.Data
			ua.InOut = row.InOut.String
			if !row.Package.Valid {
				continue
			}
			ua.PackageName = row.Package.String
			if ua.PackageName != prevPackage {
				if pkgTime, err = getObjTime(ua.PackageName); err != nil {
					return err
				}
				prevPackage = ua.PackageName
				grp.Go(func() error {
					buf := bufPool.Get().(*bytes.Buffer)
					defer bufPool.Put(buf)
					buf.Reset()

					Log := log.With(logger, "package", ua.PackageName).Log
					if srcErr := getSource(ctx, buf, cx, ua.PackageName); srcErr != nil {
						Log("WARN", "getSource", "error", srcErr)
						return nil
					}
					replMu.Lock()
					for _, b := range rAnnotation.FindAll(buf.Bytes(), -1) {
						b = bytes.TrimSpace(bytes.TrimPrefix(b, []byte("--genocall:")))
						a := genocall.Annotation{Package: ua.PackageName}
						if i := bytes.IndexByte(b, ' '); i < 0 {
							continue
						} else {
							a.Type, b = string(b[:i]), b[i+1:]
						}
						if i := bytes.Index(b, []byte("=>")); i < 0 {
							if i = bytes.IndexByte(b, '='); i < 0 {
								a.Name = string(bytes.TrimSpace(b))
							} else {
								a.Name = string(bytes.TrimSpace(b[:i]))
								if a.Size, err = strconv.Atoi(string(bytes.TrimSpace(b[i+1:]))); err != nil {
									return err
								}
							}
						} else {
							a.Name, a.Other = string(bytes.TrimSpace(b[:i])), string(bytes.TrimSpace(b[i+2:]))
						}
						annotations = append(annotations, a)
					}
					bb := buf.Bytes()
					if len(annotations) != 0 {
						Log("annotations", annotations)
						bb = rAnnotation.ReplaceAll(bb, nil)
					}
					replMu.Unlock()
					subCtx, subCancel := context.WithTimeout(ctx, 1*time.Second)
					funDocs, docsErr := parseDocs(subCtx, string(bb))
					subCancel()
					Log("msg", "parseDocs", "docs", len(funDocs), "error", docsErr)
					docsMu.Lock()
					pn := genocall.UnoCap(ua.PackageName) + "."
					for nm, doc := range funDocs {
						docs[pn+strings.ToLower(nm)] = doc
					}
					docsMu.Unlock()
					if docsErr == context.DeadlineExceeded {
						docsErr = nil
					}
					return docsErr
				})
			}
			ua.LastDDL = pkgTime
			if row.Object.Valid {
				ua.ObjectName = row.Object.String
			}
			if row.Argument != "" {
				ua.ArgumentName = row.Argument
			}
			if row.Charset != "" {
				ua.CharacterSetName = row.Charset
			}
			if row.PLS != "" {
				ua.PlsType = row.PLS
			}
			if row.Owner != "" {
				ua.TypeOwner = row.Owner
			}
			if row.Name != "" {
				ua.TypeName = row.Name
			}
			if row.Subname != "" {
				ua.TypeSubname = row.Subname
			}
			if row.Link != "" {
				ua.TypeLink = row.Link
			}
			ua.ObjectID = uint(row.OID)
			if row.SubID.Valid {
				ua.SubprogramID = uint(row.SubID.Int64)
			}
			ua.DataLevel = uint8(row.Level)
			ua.Position = uint(row.Seq)
			if row.Prec.Valid {
				ua.DataPrecision = uint8(row.Prec.Int64)
			}
			if row.Scale.Valid {
				ua.DataScale = uint8(row.Scale.Int64)
			}
			if row.Length.Valid {
				ua.CharLength = uint(row.Length.Int64)
			}
			userArgs <- ua
		}
		return nil
	})
	filteredArgs := make(chan []genocall.UserArgument, 16)
	grp.Go(func() error { genocall.FilterAndGroup(filteredArgs, userArgs, filter); return nil })
	functions, err = genocall.ParseArguments(filteredArgs, filter)
	if grpErr := grp.Wait(); grpErr != nil {
		if err == nil {
			err = grpErr
		}
		logger.Log("msg", "ParseArguments", "error", grpErr)
	}
	docNames := make([]string, 0, len(docs))
	for k := range docs {
		docNames = append(docNames, k)
	}
	sort.Strings(docNames)
	var any bool
	for i, f := range functions {
		if f.Documentation == "" {
			if f.Documentation = docs[f.Name()]; f.Documentation == "" {
				//logger.Log("msg", "No documentation", "function", f.Name())
				any = true
			} else {
				functions[i] = f
			}
		}
	}
	if any {
		logger.Log("has", docNames)
	}
	return functions, annotations, nil
}

var bufPool = sync.Pool{New: func() interface{} { return bytes.NewBuffer(make([]byte, 1024)) }}

func getSource(ctx context.Context, w io.Writer, cx *sql.DB, packageName string) error {
	qry := "SELECT text FROM user_source WHERE name = UPPER(:1) AND type = 'PACKAGE' ORDER BY line"
	rows, err := cx.QueryContext(ctx, qry, packageName)
	if err != nil {
		return errors.Errorf("%s [%q]: %w", qry, packageName, err)
	}
	defer rows.Close()
	for rows.Next() {
		var line sql.NullString
		if err := rows.Scan(&line); err != nil {
			return errors.Errorf("%s: %w", qry, err)
		}
		if _, err := io.WriteString(w, line.String); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return errors.Errorf("%s: %w", qry, err)
	}
	return nil
}

func i64ToString(n sql.NullInt64) string {
	if n.Valid {
		return strconv.FormatInt(n.Int64, 10)
	}
	return ""
}

func parsePkgFlag(s string) (string, string) {
	if i := strings.LastIndexByte(s, ':'); i >= 0 {
		return s[:i], s[i+1:]
	}
	pkg := path.Base(s)
	if pkg == "" {
		pkg = "main"
	}
	return s, pkg
}

var rReplace = regexp.MustCompile(`\s*=>\s*`)
var rAnnotation = regexp.MustCompile(`--(oracall|gen-?o-?call):(?:(replace(_json)?|rename)\s+[a-zA-Z0-9_#]+\s*=>\s*[a-zA-Z0-9_#]+|(handle|private)\s+[a-zA-Z0-9_#]+|max-table-size\s+[a-zA-Z0-9_$]+\s*=\s*[0-9]+)`)

type typeResolver map[string]*sql.Stmt

func newTypeResolver(ctx context.Context, tx *sql.Tx) (typeResolver, error) {
	stmts := make(typeResolver, 4)
	for nm, qry := range map[string]string{
		"coll": `SELECT coll_type, elem_type_owner, elem_type_name, elem_type_package,
			   length, precision, scale, character_set_name, index_by,
			   (SELECT MIN(typecode) FROM all_plsql_types B
				  WHERE B.owner = A.elem_type_owner AND
						B.type_name = A.elem_type_name AND
						B.package_name = A.elem_type_package) typecode
		  FROM all_plsql_coll_types A
		  WHERE owner = :owner AND package_name = :pkg AND type_name = :sub
		UNION
		SELECT coll_type, elem_type_owner, elem_type_name, NULL elem_type_package,
			   length, precision, scale, character_set_name, NULL index_by,
			   (SELECT MIN(typecode) FROM all_types B
				  WHERE B.owner = A.elem_type_owner AND
						B.type_name = A.elem_type_name) typecode
		  FROM all_coll_types A
		  WHERE (owner, type_name) IN (
			SELECT :owner, :pkg FROM DUAL
			UNION
			SELECT table_owner, table_name||NVL2(db_link, '@'||db_link, NULL)
			  FROM user_synonyms
			  WHERE synonym_name = :pkg)`,

		"plsTyp": `SELECT attr_name, attr_type_owner, attr_type_name, attr_type_package,
		  length, precision, scale, character_set_name, attr_no,
		  (SELECT MIN(typecode) FROM all_plsql_types B
			 WHERE B.owner = A.attr_type_owner AND B.type_name = A.attr_type_name AND B.package_name = A.attr_type_package) typecode
	 FROM all_plsql_type_attrs A
	 WHERE owner = :owner AND package_name = :pkg AND type_name = :sub
	 ORDER BY attr_no`,

		"objTyp": `SELECT B.attr_name, B.ATTR_TYPE_NAME, B.PRECISION, B.scale, B.character_set_name,
            NVL2(B.ATTR_TYPE_OWNER, B.attr_type_owner||'.', '')||B.attr_type_name, B.length
       FROM all_type_attrs B
	   WHERE B.owner = :owner AND B.type_name = :sub`,
	} {
		var err error
		if stmts[nm], err = tx.PrepareContext(ctx, qry); err != nil {
			stmts.Close()
			return nil, errors.Errorf("%s: %w", qry, err)
		}
	}
	return stmts, nil
}

func (tr typeResolver) Close() error {
	var firstErr error
	for _, stmt := range tr {
		if stmt != nil {
			if err := stmt.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (tr typeResolver) Resolve(ctx context.Context, typ, owner, pkg, sub string) ([]dbType, error) {
	plus := make([]dbType, 0, 4)
	var rows *sql.Rows
	var err error

	switch typ {
	case "PL/SQL TABLE", "PL/SQL INDEX TABLE", "TABLE":
		/*SELECT coll_type, elem_type_owner, elem_type_name, elem_type_package,
			   length, precision, scale, character_set_name, index_by
		  FROM all_plsql_coll_types
		  WHERE owner = :1 AND package_name = :2 AND type_name = :3*/
		if rows, err = tr["coll"].QueryContext(ctx,
			sql.Named("owner", owner), sql.Named("pkg", pkg), sql.Named("sub", sub),
		); err != nil {
			return plus, err
		}
		defer rows.Close()
		for rows.Next() {
			var t dbType
			var indexBy, typeCode string
			if err = rows.Scan(&t.Data, &t.Owner, &t.Subname, &t.Name,
				&t.Length, &t.Prec, &t.Scale, &t.Charset, &indexBy, &typeCode,
			); err != nil {
				return plus, err
			}
			if typeCode != "COLLECTION" {
				t.Data = typeCode
			}
			if t.Data == "" {
				t.Data = t.Subname
			}
			if t.Data == "PL/SQL INDEX TABLE" {
				t.Data = "PL/SQL TABLE"
			}
			t.Level = 1
			plus = append(plus, t)
		}

	case "REF CURSOR":
		/*
			ARGUMENT_NAME	SEQUENCE	DATA_LEVEL	DATA_TYPE
			        	1	0	REF CURSOR
			        	2	1	PL/SQL RECORD
			SZERZ_AZON	3	2	NUMBER
			UZENET_TIP	4	2	CHAR
			HIBAKOD  	5	2	VARCHAR2
			DATUM   	6	2	DATE
			UTOLSO_TIP	7	2	CHAR
			JAVITVA  	8	2	VARCHAR2
			P_IDO_TOL	9	0	DATE
			P_IDO_IG	10	0	DATE
		*/
		plus = append(plus, dbType{Owner: owner, Name: pkg, Subname: sub, Data: "PL/SQL RECORD", Level: 1})

	case "PL/SQL RECORD":
		/*SELECT attr_name, attr_type_owner, attr_type_name, attr_type_package,
		                      length, precision, scale, character_set_name, attr_no
					     FROM all_plsql_type_attrs
						 WHERE owner = :1 AND package_name = :2 AND type_name = :3
						 ORDER BY attr_no*/
		if rows, err = tr["plsTyp"].QueryContext(ctx,
			sql.Named("owner", owner), sql.Named("pkg", pkg), sql.Named("sub", sub),
		); err != nil {
			return plus, err
		}
		defer rows.Close()
		for rows.Next() {
			var t dbType
			var attrNo sql.NullInt64
			var typeCode string
			if err = rows.Scan(&t.Argument, &t.Owner, &t.Subname, &t.Name,
				&t.Length, &t.Prec, &t.Scale, &t.Charset, &attrNo, &typeCode,
			); err != nil {
				return plus, err
			}
			t.Data = typeCode
			if typeCode == "COLLECTION" {
				t.Data = "PL/SQL TABLE"
			}
			if t.Owner == "" && t.Subname != "" {
				t.Data = t.Subname
			}
			if t.Data == "PL/SQL INDEX TABLE" {
				t.Data = "PL/SQL TABLE"
			}
			t.Level = 1
			plus = append(plus, t)
		}
	default:
		return nil, errors.Errorf("%s: %w", typ, errors.New("unknown type"))
	}
	if rows != nil {
		err = rows.Err()
	}
	if len(plus) == 0 && err == nil {
		err = errors.Errorf("%s/%s.%s.%s: %w", typ, owner, pkg, sub, errors.New("not found"))
	}
	return plus, err
}

// SUBPROGRAM_ID	ARGUMENT_NAME	SEQUENCE	DATA_LEVEL	DATA_TYPE	IN_OUT
//	P_KARSZAM   	1	0	NUMBER	IN
//	P_TSZAM	        2	0	NUMBER	IN
//	P_OUTPUT    	3	0	PL/SQL TABLE	OUT
//           		4	1	PL/SQL RECORD	OUT
//	F_SZERZ_AZON	5	2	NUMBER	OUT

/*
ARGUMENT_NAME	SEQUENCE	DATA_LEVEL	DATA_TYPE	TYPE_OWNER	TYPE_NAME	TYPE_SUBNAME
P_SZERZ_AZON	1	0	NUMBER
P_OUTPUT    	2	0	PL/SQL TABLE	ABLAK	DB_SPOOLSYS3	TYPE_OUTLIST_078
	            3	1	PL/SQL RECORD	ABLAK	DB_SPOOLSYS3	TYPE_OUTPUT_078
TRANZ_KEZDETE	4	2	DATE
TRANZ_VEGE    	5	2	DATE
KOLTSEG	        6	2	NUMBER
ERTE..TT_ALAPOK	7	2	PL/SQL TABLE	ABLAK	DB_SPOOLSYS3	ATYPE_OUTLIST_UNIT
             	8	3	PL/SQL RECORD	ABLAK	DB_SPOOLSYS3	ATYPE_OUTPUT_UNIT
F_UNIT_RNEV  	9	4	VARCHAR2
F_UNIT_NEV  	10	4	VARCHAR2
F_ISIN       	11	4	VARCHAR2
UNIT_DB	        12	4	NUMBER
UNIT_ARF	    13	4	NUMBER
VASAROLT_ALAPOK	14	2	PL/SQL TABLE	ABLAK	DB_SPOOLSYS3	ATYPE_OUTLIST_UNIT
	            15	3	PL/SQL RECORD	ABLAK	DB_SPOOLSYS3	ATYPE_OUTPUT_UNIT
F_UNIT_RNEV	    16	4	VARCHAR2
F_UNIT_NEV    	17	4	VARCHAR2
F_ISIN        	18	4	VARCHAR2
UNIT_DB       	19	4	NUMBER
UNIT_ARF     	20	4	NUMBER
*/

func (tr typeResolver) ExpandArgs(ctx context.Context, plus []dbType) ([]dbType, error) {
	//logger.Log("expand", plus)
	for i := 0; i < len(plus); i++ {
		p := plus[i]
		if p.Data == "PL/SQL INDEX TABLE" {
			p.Data = "PL/SQL TABLE"
		}
		//logger.Log("i", i, "arg", p.Argument, "data", p.Data, "owner", p.Owner, "name", p.Name, "sub", p.Subname)
		if p.Data == "TABLE" || p.Data == "PL/SQL TABLE" || p.Data == "PL/SQL RECORD" || p.Data == "REF CURSOR" {
			q, err := tr.Resolve(ctx, p.Data, p.Owner, p.Name, p.Subname)
			if err != nil {
				return plus, errors.Errorf("%+v: %w", p, err)
			}
			//logger.Log("q", q)
			for i, x := range q {
				if x.Data == "PL/SQL INDEX TABLE" {
					x.Data = "PL/SQL TABLE"
				}
				x.Level += p.Level
				q[i] = x
			}
			plus = append(plus[:i+1], append(q, plus[i+1:]...)...)
		}
	}
	return plus, nil
}

// vim: set fileencoding=utf-8 noet:
