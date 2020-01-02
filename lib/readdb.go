/*
Copyright 2019 Tamás Gulácsi

// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0
*/

package genocall

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	errors "golang.org/x/xerrors"
)

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
	Prec, Scale, Length                            sql.NullInt64
}

func (t dbType) String() string {
	return fmt.Sprintf("%s{%s}(%s/%s.%s.%s@%s)", t.Argument, t.Data, t.PLS, t.Owner, t.Name, t.Subname, t.Link)
}

func ReadDB(ctx context.Context, db querier, pattern string, filter func(string) bool) (functions []Function, annotations []Annotation, err error) {
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
		return nil, nil, errors.Errorf("%s: %w", argumentsQry, err)
	}
	defer rows.Close()

	grp, grpCtx := errgroup.WithContext(ctx)
	var annotPromises []<-chan []Annotation
	var docPromises []<-chan map[string]string
	userArgs := make([]UserArgument, 0, 1024)
	var prevPackage string
	var pkgTime time.Time
	for rows.Next() {
		var row dbRow
		if err = rows.Scan(&row.Owner, &row.Package, &row.Object,
			&row.Argument, &row.InOut,
			&row.Data, &row.Prec, &row.Scale, &row.Charset,
			&row.PLS, &row.Length, &row.Owner, &row.Name, &row.Subname, &row.Link,
		); err != nil {
			return nil, nil, errors.Errorf("reading row=%v: %w", rows, err)
		}
		var ua UserArgument
		ua.DataType = row.Data
		ua.InOut = row.InOut
		if row.Package == "" {
			continue
		}

		switch row.Data {
		case "OBJECT", "PL/SQL TABLE", "PL/SQL RECORD", "REF CURSOR", "TABLE":
			grp.Go(func() error {
				return tr.Resolve(grpCtx, row.Data, TypeName{Owner: row.Owner, Package: row.Name, Name: row.Subname})
			})
		}

		ua.PackageName = row.Package
		if ua.PackageName != prevPackage {
			prevPackage = ua.PackageName
			if pkgTime, err = getObjTime(ua.PackageName); err != nil {
				return nil, nil, err
			}
			aCh := make(chan []Annotation, 1)
			annotPromises = append(annotPromises, aCh)
			dCh := make(chan map[string]string, 1)
			docPromises = append(docPromises, dCh)

			// read source and parse for annotations and documentation
			grp.Go(func() error {
				buf := Buffers.Get()
				buf.Reset()
				defer func() {
					buf.Reset()
					Buffers.Put(buf)
					close(aCh)
					close(dCh)
				}()

				if err := getSource(grpCtx, buf, db, ua.PackageName); err != nil {
					return err
				}

				annotations, docs, err := ParseAnnotationsAndDocs(grpCtx, ua.PackageName, buf.String())
				aCh <- annotations
				dCh <- docs
				if err != nil {
					return err
				}
				return nil
			})
		}
		ua.LastDDL = pkgTime
		if row.Object != "" {
			ua.ObjectName = row.Object
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
		//ua.ObjectID = uint(row.OID)
		//if row.SubID.Valid {
		//ua.SubprogramID = uint(row.SubID.Int64)
		//}
		//ua.DataLevel = uint8(row.Level)
		//ua.Position = uint(row.Seq)
		if row.Prec.Valid {
			ua.DataPrecision = uint8(row.Prec.Int64)
		}
		if row.Scale.Valid {
			ua.DataScale = uint8(row.Scale.Int64)
		}
		if row.Length.Valid {
			ua.CharLength = uint(row.Length.Int64)
		}

		userArgs = append(userArgs, ua)
	}
	if grpErr := grp.Wait(); grpErr != nil {
		if err == nil {
			err = grpErr
		}
	}
	for _, aCh := range annotPromises {
		select {
		case <-ctx.Done():
			return functions, annotations, ctx.Err()
		case annot := <-aCh:
			annotations = append(annotations, annot...)
		}
	}
	filteredArgs, err := FilterAndGroup(userArgs, filter)
	if err != nil {
		return functions, annotations, err
	}
	if functions, err = ParseArguments(filteredArgs, filter); err != nil {
		return functions, annotations, err
	}
	funcs := make(map[string]int, len(functions))
	for i, f := range functions {
		funcs[f.Name()] = i
	}
	for _, dCh := range docPromises {
		select {
		case <-ctx.Done():
			return functions, annotations, ctx.Err()
		case doc := <-dCh:
			for k, s := range doc {
				functions[funcs[k]].Documentation = s
			}
		}
	}
	return functions, annotations, nil
}

func ParseAnnotationsAndDocs(ctx context.Context, packageName, src string) ([]Annotation, map[string]string, error) {
	var annotations []Annotation
	docs := make(map[string]string)
	for _, b := range rAnnotation.FindAllString(src, -1) {
		// FIXME(tgulacsi): --oracall:
		b = strings.TrimSpace(b[strings.IndexByte(b, ':')+1:])
		a := Annotation{Package: packageName}
		if i := strings.IndexByte(b, ' '); i < 0 {
			continue
		} else {
			a.Type, b = b[:i], b[i+1:]
		}
		if i := strings.Index(b, "=>"); i < 0 {
			if i = strings.IndexByte(b, '='); i < 0 {
				a.Name = strings.TrimSpace(b)
			} else {
				a.Name = strings.TrimSpace(b[:i])
				var err error
				if a.Size, err = strconv.Atoi(strings.TrimSpace(b[i+1:])); err != nil {
					return annotations, docs, err
				}
			}
		} else {
			a.Name, a.Other = strings.TrimSpace(b[:i]), strings.TrimSpace(b[i+2:])
		}
		annotations = append(annotations, a)
	}
	if len(annotations) != 0 {
		src = rAnnotation.ReplaceAllString(src, "")
	}

	funDocs, err := ParseDocs(ctx, src)
	pn := UnoCap(packageName) + "."
	for nm, doc := range funDocs {
		docs[pn+strings.ToLower(nm)] = doc
	}
	return annotations, docs, err
}

func getSource(ctx context.Context, w io.Writer, cx querier, packageName string) error {
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

//var rReplace = regexp.MustCompile(`\s*=>\s*`)
var rAnnotation = regexp.MustCompile(`--(oracall|gen-?o-?call):(?:(replace(_json)?|rename)\s+[a-zA-Z0-9_#]+\s*=>\s*[a-zA-Z0-9_#]+|(handle|private)\s+[a-zA-Z0-9_#]+|max-table-size\s+[a-zA-Z0-9_$]+\s*=\s*[0-9]+)`)

type Type struct {
	TypeName
	Attr                       string
	Charset, IndexBy, TypeCode string
	Length, Prec, Scale        sql.NullInt64
	CollectionOf               TypeName
	RecordOf                   []TypeName
}

type TypeName struct {
	Owner, Package, Name string
}
type typeResolver struct {
	types map[TypeName]Type
	stmts map[string]*sql.Stmt
}

func newTypeResolver(ctx context.Context, tx querier) (*typeResolver, error) {
	tr := typeResolver{stmts: make(map[string]*sql.Stmt, 4), types: make(map[TypeName]Type)}
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
		  length, precision, scale, character_set_name,
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
		if tr.stmts[nm], err = tx.PrepareContext(ctx, qry); err != nil {
			tr.Close()
			return nil, errors.Errorf("%s: %w", qry, err)
		}
	}
	return &tr, nil
}

func (tr *typeResolver) Close() error {
	var firstErr error
	stmts := tr.stmts
	tr.stmts = nil
	for _, stmt := range stmts {
		if stmt != nil {
			if err := stmt.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (tr *typeResolver) Resolve(ctx context.Context, data string, tn TypeName) error {
	typ := Type{TypeName: tn, TypeCode: data}
	var rows *sql.Rows
	var err error
	switch data {
	case "PL/SQL TABLE", "PL/SQL INDEX TABLE", "TABLE":
		var elem Type
		if err = tr.stmts["coll"].QueryRowContext(ctx,
			sql.Named("owner", tn.Owner), sql.Named("pkg", tn.Package), sql.Named("sub", tn.Name),
		).Scan(
			/*
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
			*/
			&typ.TypeCode,
			&elem.Owner, &elem.Name, &elem.Package,
			&elem.Length, &elem.Prec, &elem.Scale,
			&elem.Charset, &elem.IndexBy,
			&elem.TypeCode,
		); err != nil {
			return err
		}
		tr.types[elem.TypeName] = elem
		typ.CollectionOf = elem.TypeName

	case "PL/SQL RECORD":
		/*
				"plsTyp": `SELECT attr_name, attr_type_owner, attr_type_name, attr_type_package,
				  length, precision, scale, character_set_name,
				  (SELECT MIN(typecode) FROM all_plsql_types B
					 WHERE B.owner = A.attr_type_owner AND B.type_name = A.attr_type_name AND B.package_name = A.attr_type_package) typecode
			 FROM all_plsql_type_attrs A
			 WHERE owner = :owner AND package_name = :pkg AND type_name = :sub
			 ORDER BY attr_no`,
		*/
		if rows, err = tr.stmts["plsTyp"].QueryContext(ctx,
			sql.Named("owner", tn.Owner), sql.Named("pkg", tn.Package), sql.Named("sub", tn.Name),
		); err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var t Type
			if err = rows.Scan(&t.Attr, &t.Owner, &t.Name, &t.Package,
				&t.Length, &t.Prec, &t.Scale, &t.Charset, &t.TypeCode,
			); err != nil {
				return err
			}
			tr.types[t.TypeName] = t
			typ.RecordOf = append(typ.RecordOf, t.TypeName)
		}

	case "REF CURSOR":
		typ.CollectionOf = typ.TypeName
		err = tr.Resolve(ctx, "PL/SQL RECORD", tn)

	default:
		return errors.Errorf("%v: %w", typ, errors.New("unknown type"))
	}
	if rows != nil {
		if rowsErr := rows.Err(); rowsErr != nil && err == nil {
			err = rowsErr
		}
	}
	return err
}

type querier interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
	PrepareContext(context.Context, string) (*sql.Stmt, error)
}
