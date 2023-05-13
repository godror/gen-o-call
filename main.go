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
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"
	"unicode"

	"golang.org/x/sync/errgroup"

	"github.com/UNO-SOFT/zlog/v2"
	custom "github.com/godror/gen-o-call/custom"
	genocall "github.com/godror/gen-o-call/lib"

	// for Oracle-specific drivers
	godror "github.com/godror/godror"
)

//go:generate go generate ./lib
// Should install protobuf-compiler to use it, like
// curl -L https://github.com/google/protobuf/releases/download/v3.0.0-beta-2/protoc-3.0.0-beta-2-linux-x86_64.zip -o /tmp/protoc-3.0.0-beta-2-linux-x86_64.zip && unzip -p /tmp/protoc-3.0.0-beta-2-linux-x86_64.zip protoc >$HOME/bin/protoc

var verbose zlog.VerboseVar
var logger = zlog.NewLogger(zlog.MaybeConsoleHandler(&verbose, os.Stderr)).SLog()

var flagConnect *string

func main() {
	genocall.SetLogger(logger.WithGroup("genocall"))
	if err := Main(os.Args[1:]); err != nil {
		logger.Error("ERROR", "error", err)
		os.Exit(1)
	}
}

func Main(args []string) error {
	os.Args = args

	gopSrc := filepath.Join(os.Getenv("GOPATH"), "src")

	fs := flag.NewFlagSet("gen-o-call", flag.ContinueOnError)
	flagConnect = fs.String("connect", "", "connect to DB for retrieving function arguments")
	fs.BoolVar(&genocall.SkipMissingTableOf, "skip-missing-table-of", true, "skip functions with missing TableOf info")
	flagBaseDir := fs.String("base-dir", gopSrc, "base dir for the -pb-out, -db-out flags")
	flagPbOut := fs.String("pb-out", "", "package import path for the Protocol Buffers files, optionally with the package name, like \"my/pb-pkg:main\"")
	flagDbOut := fs.String("db-out", "-:main", "package name of the generated functions, optionally with the package name, like \"my/db-pkg:main\"")
	fs.BoolVar(&genocall.NumberAsString, "number-as-string", false, "add ,string to json tags")
	fs.BoolVar(&custom.ZeroIsAlmostZero, "zero-is-almost-zero", false, "zero should be just almost zero, to distinguish 0 and non-set field")
	fs.Var(&verbose, "v", "verbose logging")
	flagExcept := fs.String("except", "", "except these functions")
	flagReplace := fs.String("replace", "", "funcA=>funcB")
	flagTestOut := fs.Bool("test-out", false, "output test data")
	flagJsonIn := fs.String("json", "", "JSON input data")
	fs.IntVar(&genocall.MaxTableSize, "max-table-size", genocall.MaxTableSize, "maximum table size for PL/SQL associative arrays")

	if err := fs.Parse(args); err != nil {
		return err
	}
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

	pattern := fs.Arg(0)
	if pattern == "" {
		pattern = "%"
	}

	ctx, cancel := globalCtx(context.Background())
	defer cancel()
	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()

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
		logger.Info("funcs", "except", except)
		filters = append(filters, func(s string) bool {
			for _, e := range except {
				if strings.EqualFold(e, s) {
					return false
				}
			}
			return true
		})
	}

	var functions []genocall.Function
	if *flagJsonIn != "" {
		fh, err := os.Open(*flagJsonIn)
		if err != nil {
			return err
		}
		err = json.NewDecoder(fh).Decode(&functions)
		fh.Close()
		if err != nil {
			return err
		}
	} else {
		db, err := sql.Open("godror", *flagConnect)
		if err != nil {
			return fmt.Errorf("connect to %s: %w", *flagConnect, err)
		}
		defer db.Close()
		if verbose > 1 {
			godror.SetLogger(zlog.NewLogger(logger.WithGroup("godror").Handler()).Logr())
		}
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return err
		}
		defer tx.Rollback()

		var annotations []genocall.Annotation
		functions, annotations, err = genocall.ReadDB(ctx, tx, pattern, filter)
		if err != nil {
			return fmt.Errorf("read %s: %w", fs.Arg(0), err)
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
		logger.Info("read", "annotations", annotations)
		functions = genocall.ApplyAnnotations(functions, annotations)
		sort.Slice(functions, func(i, j int) bool { return functions[i].FullName() < functions[j].FullName() })

		if *flagTestOut {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("  ", "  ")
			enc.Encode(functions)
		}
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
		logger.Info("Writing generated functions", "file", fn)
		os.MkdirAll(filepath.Dir(fn), 0775)
		if out, err = os.Create(fn); err != nil {
			return fmt.Errorf("create %s: %w", fn, err)
		}
		testFn := fn[:len(fn)-3] + "_test.go"
		if testOut, err = os.Create(testFn); err != nil {
			return fmt.Errorf("create %s: %w", testFn, err)
		}
		defer func() {
			if err := out.Close(); err != nil {
				logger.Error("close", "file", out.Name(), "error", err)
			}
			if err := testOut.Close(); err != nil {
				logger.Error("close", "file", testOut.Name(), "error", err)
			}
		}()
	}

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
			return fmt.Errorf("save functions: %w", err)
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
				return fmt.Errorf("save function tests: %w", err)
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
		logger.Info("Writing Protocol Buffers", "file", fn)
		fh, err := os.Create(fn)
		if err != nil {
			return fmt.Errorf("create proto: %w", err)
		}
		err = genocall.SaveProtobuf(fh, functions, pbPkg)
		if closeErr := fh.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		if err != nil {
			return fmt.Errorf("SaveProtobuf: %w", err)
		}

		goOut := "go_out"
		cmd := exec.Command(
			"protoc",
			"--proto_path="+*flagBaseDir+":.",
			"--"+goOut+"=Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,plugins=grpc:"+*flagBaseDir,
			fn,
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("%q: %w", cmd.Args, err)
		}
		return nil
	})

	if err := grp.Wait(); err != nil {
		return err
	}
	return nil
}

var rReplace = regexp.MustCompile(`\s*=>\s*`)

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

func globalCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	go func() {
		<-sigCh
		signal.Stop(sigCh)
		cancel()
	}()
	return ctx, cancel
}

// vim: set fileencoding=utf-8 noet:
