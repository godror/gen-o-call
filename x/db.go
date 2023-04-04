package x

import (
	"context"
	"database/sql"
	"fmt"
	"golang.org/x/exp/slog"
	"strings"
	"sync"
)

type Direction uint8

const (
	DirIn    = Direction(0)
	DirOut   = Direction(1)
	DirInOut = Direction(2)
)

type Attribute struct {
	Name string
	Type ObjectOrScalar
}
type Argument struct {
	Attribute
	InOut Direction
}
type Function struct {
	Triplet
	Args []Argument
}

type Scalar struct {
	DataType                 string
	Length, Precision, Scale sql.NullInt32
}
type Object struct {
	CollectionOf *ObjectOrScalar
	Triplet
	TypeLink                string
	TypeObjectType, PlsType string
	IndexBy                 string
	Attributes              []Attribute
	IsCollection            bool
}
type ObjectOrScalar struct {
	Object
	Scalar
	IsObject bool
}
type Triplet struct {
	Owner, Package, Name string
}

type DB struct {
	DB       querier
	objCache map[Triplet]*Object
	mu       sync.Mutex
}

func (db *DB) ReadPackage(ctx context.Context, pkg string) ([]Function, error) {
	owner, pkg := splitOwner(pkg)
	const qry = `SELECT 
	    owner, package_name, object_name, argument_name, data_type,
		in_out, data_length, data_precision, data_scale,
		type_owner, type_name, type_subname, type_link,
		type_object_type, pls_type
	  FROM all_arguments 
	  WHERE owner = NVL(:1, SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')) AND 
	        package_name = UPPER(:2)
	  ORDER BY object_id, subprogram_id, sequence`
	rows, err := db.DB.QueryContext(ctx, qry, owner, pkg)
	if err != nil {
		return nil, fmt.Errorf("%s [%q, %q]: %w", qry, owner, pkg, err)
	}
	defer rows.Close()
	var funcs []Function
	var old Function
	for rows.Next() {
		var act Function
		var arg Argument
		var inOut string
		if err = rows.Scan(
			&act.Owner, &act.Package, &act.Name,
			&arg.Name, &arg.Type.DataType, &inOut,
			&arg.Type.Length, &arg.Type.Precision, &arg.Type.Scale,
			&arg.Type.Owner, &arg.Type.Package, &arg.Type.Name, &arg.Type.TypeLink,
			&arg.Type.TypeObjectType, &arg.Type.PlsType,
		); err != nil {
			return funcs, fmt.Errorf("%s [%q, %q]: %w", qry, owner, pkg, err)
		}
		if arg.Type.IsObject = arg.Type.Owner != ""; arg.Type.IsObject {
			sub, err := db.ReadObject(ctx, arg.Type.Object.Triplet)
			if err != nil {
				return funcs, err
			}
			arg.Type.Object = *sub
		}
		switch inOut {
		case "IN":
			arg.InOut = DirIn
		case "OUT":
			arg.InOut = DirOut
		default:
			arg.InOut = DirInOut
		}
		if old.Owner == act.Owner && old.Package == act.Package && old.Name == act.Name {
			old.Args = append(old.Args, arg)
		} else {
			if old.Name != "" {
				funcs = append(funcs, old)
			}
			act.Args = append(act.Args, arg)
			old = act
		}
	}
	rows.Close()
	if old.Name != "" {
		funcs = append(funcs, old)
	}
	return funcs, rows.Err()
}
func (db *DB) ReadObject(ctx context.Context, name Triplet) (*Object, error) {
	slog.Debug("ReadObject", "obj", name)

	db.mu.Lock()
	obj := db.objCache[name]
	db.mu.Unlock()
	if obj != nil {
		return obj, nil
	}
	obj = &Object{Triplet: name}

	const qry = `SELECT typecode, attributes 
		FROM all_plsql_types 
		WHERE owner = NVL(:1, SYS_CONTEXT('BR_CTX_G','CURRENT_SCHEMA')) AND package_name = :2 AND type_name = :3`
	var typ string
	var n int32
	if err := db.DB.QueryRowContext(ctx, qry, obj.Owner, obj.Package, obj.Name).Scan(
		&typ, &n,
	); err != nil {
		return nil, fmt.Errorf("%s [%q, %q, %q]: %w", qry, obj.Owner, obj.Package, obj.Name, err)
	}
	slog.Debug("IsCollection", "typ", typ, "n", n)
	if n != 0 {
		obj.Attributes = make([]Attribute, 0, int(n))
		const qry = `SELECT attr_name, 
			attr_type_owner, attr_type_package, attr_type_name, 
			length, precision, scale
		FROM all_plsql_type_attrs
		WHERE owner = NVL(:1, SYS_CONTEXT('BR_CTX_G','CURRENT_SCHEMA')) AND package_name = :2 AND type_name = :3
		ORDER BY attr_no`
		rows, err := db.DB.QueryContext(ctx, qry, obj.Owner, obj.Package, obj.Name)
		if err != nil {
			return nil, fmt.Errorf("%s [%q, %q, %q]: %w", qry, obj.Owner, obj.Package, obj.Name, err)
		}
		defer rows.Close()
		for rows.Next() {
			var a Attribute
			if err = rows.Scan(
				&a.Name,
				&a.Type.Object.Owner, &a.Type.Object.Package, &a.Type.Object.Name,
				&a.Type.Length, &a.Type.Precision, &a.Type.Scale,
			); err != nil {
				return nil, fmt.Errorf("%s [%q]: %w", qry, name, err)
			}
			if a.Type.IsObject = a.Type.Owner != ""; a.Type.IsObject {
				sub, err := db.ReadObject(ctx, a.Type.Triplet)
				if err != nil {
					return nil, err
				}
				a.Type.Object = *sub
			}
			obj.Attributes = append(obj.Attributes, a)
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			return nil, err
		}
	} else if obj.IsCollection = typ == "COLLECTION"; obj.IsCollection {
		const qry = `SELECT coll_type, 
		elem_type_owner, elem_type_package, elem_type_name, 
		length, precision, scale, index_by 
	FROM all_plsql_coll_types
	WHERE owner = NVL(:1, SYS_CONTEXT('BR_CTX_G','CURRENT_SCHEMA')) AND package_name = :2 AND type_name = :3`
		var elem ObjectOrScalar
		if err := db.DB.QueryRowContext(ctx, qry, obj.Owner, obj.Package, obj.Name).Scan(
			&typ,
			&elem.Owner, &elem.Package, &elem.Name,
			&elem.Length, &elem.Precision, &elem.Scale, &elem.IndexBy,
		); err != nil {
			return nil, fmt.Errorf("%s [%q, %q, %q]: %w", qry, obj.Owner, obj.Package, obj.Name, err)
		}
		if elem.IsObject = elem.Owner != ""; elem.IsObject {
			sub, err := db.ReadObject(ctx, elem.Triplet)
			if err != nil {
				return nil, err
			}
			elem.Object = *sub
		}
		obj.CollectionOf = &elem
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.objCache == nil {
		db.objCache = make(map[Triplet]*Object)
	}
	db.objCache[obj.Triplet] = obj
	return obj, nil
}
func splitOwner(name string) (string, string) {
	if i := strings.IndexByte(name, '.'); i >= 0 {
		return name[:i], name[i+1:]
	}
	return "", name
}

type querier interface {
	QueryContext(context.Context, string, ...any) (rowser, error)
	QueryRowContext(context.Context, string, ...any) rower
}
type rowser interface {
	Close() error
	Next() bool
	Err() error
	Scan(...any) error
}
type rower interface {
	Scan(...any) error
}
type SqlDB struct {
	*sql.DB
	LogQry func(qry string, args ...any) (next func(...any), close func())
}

func (db SqlDB) QueryContext(ctx context.Context, qry string, args ...any) (rowser, error) {
	rows, err := db.DB.QueryContext(ctx, qry, args...)
	if err != nil || db.LogQry == nil {
		return nil, err
	}
	next, close := db.LogQry(qry, args...)
	return sqlRows{Rows: rows, logRow: next, logClose: close}, nil
}
func (db SqlDB) QueryRowContext(ctx context.Context, qry string, args ...any) rower {
	row := db.DB.QueryRowContext(ctx, qry, args...)
	if db.LogQry == nil {
		return row
	}
	next, close := db.LogQry(qry, args...)
	return sqlRow{Row: row, logRow: next, logClose: close}
}

type sqlRows struct {
	*sql.Rows
	logRow   func(...any)
	logClose func()
}

func (rows sqlRows) Scan(args ...any) error {
	if err := rows.Rows.Scan(args...); err != nil {
		return err
	}
	rows.logRow(args...)
	return nil
}
func (rows sqlRows) Close() error {
	err := rows.Rows.Close()
	rows.logClose()
	return err
}

type sqlRow struct {
	*sql.Row
	logRow   func(...any)
	logClose func()
}

func (row sqlRow) Scan(args ...any) error {
	if err := row.Row.Scan(args...); err != nil {
		return err
	}
	row.logRow(args...)
	row.logClose()
	return nil
}
	