package x

import (
	"context"
	"database/sql"
	"fmt"
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
	Attributes              []Attribute
	TypeLink                string
	TypeObjectType, PlsType string
	IndexBy                 string
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
	*sql.DB
	mu       sync.Mutex
	objCache map[Triplet]*Object
}

func ReadPackage(ctx context.Context, db *DB, pkg string) ([]Function, error) {
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
	rows, err := db.QueryContext(ctx, qry, owner, pkg)
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
			if err := arg.Type.Object.InitObject(ctx, db); err != nil {
				return funcs, err
			}
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
func (obj *Object) InitObject(ctx context.Context, db *DB) error {
	db.mu.Lock()
	{
		c := db.objCache[obj.Triplet]
		db.mu.Unlock()
		if c != nil {
			*obj = *c
			return nil
		}
	}
	const qry = `SELECT typecode, attributes 
		FROM all_plsql_types 
		WHERE owner = NVL(:1, SYS_CONTEXT('BR_CTX_G','CURRENT_SCHEMA')) AND package_name = :2 AND type_name = :3`
	var typ string
	var n int32
	if err := db.QueryRowContext(ctx, qry, obj.Owner, obj.Package, obj.Name).Scan(
		&typ, &n,
	); err != nil {
		return fmt.Errorf("%s [%q, %q, %q]: %w", qry, obj.Owner, obj.Package, obj.Name, err)
	}
	if n != 0 {
		obj.Attributes = make([]Attribute, 0, int(n))
		const qry = `SELECT attr_name, 
			attr_type_owner, attr_type_package, attr_type_name, 
			length, precision, scale
		FROM all_plsql_type_attrs
		WHERE owner = NVL(:1, SYS_CONTEXT('BR_CTX_G','CURRENT_SCHEMA')) AND package_name = :2 AND type_name = :3
		ORDER BY attr_no`
		rows, err := db.QueryContext(ctx, qry, obj.Owner, obj.Package, obj.Name)
		if err != nil {
			return fmt.Errorf("%s [%q, %q, %q]: %w", qry, obj.Owner, obj.Package, obj.Name, err)
		}
		defer rows.Close()
		for rows.Next() {
			var a Attribute
			if err = rows.Scan(
				&a.Name,
				&a.Type.Object.Owner, &a.Type.Object.Package, &a.Type.Object.Name,
				&a.Type.Length, &a.Type.Precision, &a.Type.Scale,
			); err != nil {
				return fmt.Errorf("%s [%q]: %w", qry, obj.Triplet, err)
			}
			obj.Attributes = append(obj.Attributes, a)
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			return err
		}
	} else if obj.IsCollection = typ == "COLLECTION"; obj.IsCollection {
		const qry = `SELECT coll_type, 
		elem_type_owner, elem_type_package, elem_type_name, 
		length, precision, scale, index_by 
	FROM all_plsql_coll_types
	WHERE owner = NVL(:1, SYS_CONTEXT('BR_CTX_G','CURRENT_SCHEMA')) AND package_name = :2 AND type_name = :3`
		var elem ObjectOrScalar
		if err := db.QueryRowContext(ctx, qry, obj.Owner, obj.Package, obj.Name).Scan(
			&typ,
			&elem.Owner, &elem.Package, &elem.Name,
			&elem.Length, &elem.Precision, &elem.Scale, &elem.IndexBy,
		); err != nil {
			return fmt.Errorf("%s [%q, %q, %q]: %w", qry, obj.Owner, obj.Package, obj.Name, err)
		}
		if elem.IsObject = elem.Owner != ""; elem.IsObject {
			if err := elem.InitObject(ctx, db); err != nil {
				return err
			}
		}
		obj.CollectionOf = &elem
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.objCache == nil {
		db.objCache = make(map[Triplet]*Object)
	}
	db.objCache[obj.Triplet] = obj
	return nil
}
func splitOwner(name string) (string, string) {
	if i := strings.IndexByte(name, '.'); i >= 0 {
		return name[:i], name[i+1:]
	}
	return "", name
}
