package x

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/slog"
)

func (f Function) FullName() string {
	nm := strings.ToLower(f.Name)
	if f.Alias != "" {
		nm = strings.ToLower(f.Name)
	}
	if f.Package == "" {
		return nm
	}
	return UnoCap(f.Package) + "." + nm
}
func (f Function) RealName() string {
	if f.Replacement != nil {
		return f.Replacement.RealName()
	}
	nm := strings.ToLower(f.Name)
	if f.Package == "" {
		return nm
	}
	return UnoCap(f.Package) + "." + nm
}
func (f Function) AliasedName() string {
	if f.Alias != "" {
		return f.Alias
	}
	return f.Name
}

func (f Function) String() string {
	args := make([]string, len(f.Args))
	for i := range args {
		args[i] = f.Args[i].String()
	}
	s := f.FullName() + "(" + strings.Join(args, ", ") + ")"
	if f.Documentation == "" {
		return s
	}
	return s + "\n" + f.Documentation
}

func (f Function) HasCursorOut() bool {
	if f.Returns != nil && f.Returns.Type.DataType == "REF CURSOR" {
		return true
	}
	for _, arg := range f.Args {
		if arg.IsOutput() && arg.Type.DataType == "REF CURSOR" {
			return true
		}
	}
	return false
}

type direction uint8

func (dir direction) IsInput() bool  { return dir&DirIn > 0 }
func (dir direction) IsOutput() bool { return dir&DirOut > 0 }
func (dir direction) String() string {
	switch dir {
	case DirIn:
		return "IN"
	case DirOut:
		return "OUT"
	case DirInOut:
		return "INOUT"
	}
	return fmt.Sprintf("%d", dir)
}
func (dir direction) MarshalText() ([]byte, error) {
	return []byte(dir.String()), nil
}
func (dir *direction) UnmarshalText(p []byte) error {
	switch string(p) {
	case "IN":
		*dir = DirIn
	case "OUT":
		*dir = DirOut
	case "INOUT":
		*dir = DirInOut
	default:
		return fmt.Errorf("unknown dir %q", string(p))
	}
	return nil
}

const (
	DirIn    = direction(1)
	DirOut   = direction(2)
	DirInOut = direction(3)
)

type Attribute struct {
	Name string
	Type ObjectOrScalar
}
type Argument struct {
	Attribute
	Direction direction
}

func (a Argument) String() string { return fmt.Sprintf("%s %s %s", a.Name, a.Direction, a.Attribute) }

func (a Argument) IsInput() bool {
	return a.Direction&DirIn != 0
}
func (a Argument) IsOutput() bool {
	return a.Direction&DirOut != 0
}

type Function struct {
	Triplet
	Alias             string     `json:",omitempty"`
	Args              []Argument `json:",omitempty"`
	Returns           *Attribute `json:",omitempty"`
	Replacement       *Function  `json:",omitempty"`
	ReplacementIsJSON bool       `json:",omitempty"`
	LastDDL           time.Time  `json:",omitempty"`
	handle            []string
	Documentation     string `json:",omitempty"`
	maxTableSize      int
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
			arg.Direction = DirIn
		case "OUT":
			arg.Direction = DirOut
		default:
			arg.Direction = DirInOut
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

	for _, f := range funcs {
		if len(f.Args) != 0 && f.Args[0].Name == "" {
			f.Returns = &f.Args[0].Attribute
			f.Args = f.Args[1:]
		}
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
func UnoCap(text string) string {
	i := strings.Index(text, "_")
	if i == 0 {
		return capitalize(text)
	}
	return strings.ToUpper(text[:i]) + "_" + strings.ToLower(text[i+1:])
}
func capitalize(text string) string {
	if text == "" {
		return text
	}
	return strings.ToUpper(text[:1]) + strings.ToLower(text[1:])
}
