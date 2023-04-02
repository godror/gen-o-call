package x

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type Direction uint8

const (
	DirIn    = Direction(0)
	DirOut   = Direction(1)
	DirInOut = Direction(2)
)

type Argument struct {
	Name                                       string
	DataType                                   string
	TypeOwner, TypeName, TypeSubname, TypeLink string
	TypeObjectType, PlsType                    string
	Length, Precision, Scale                   sql.NullInt32
	InOut                                      Direction
}
type Function struct {
	Owner, Package, Name string
	Args                 []Argument
}

func ReadPackage(ctx context.Context, db *sql.DB, pkg string) ([]Function, error) {
	owner, pkg := splitOwner(pkg)
	const qry = `SELECT 
	    owner, package_name, object_name, argument_name, data_type,
		in_out, data_length, data_precision, data_scale,
		type_owner, type_name, type_subname, type_link,
		type_object_type, pls_type
	  FROM all_arguments 
	  WHERE owner = NVL(SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA'), :1) AND 
	        package_name = UPPER(:2)
	  ORDER BY object_id, subprogram_id, sequence`
	rows, err := db.QueryContext(ctx, qry, owner, pkg)
	if err != nil {
		return nil, fmt.Errorf("%s [%q, %q]: %w", qry, owner, pkg, err)
	}
	var funcs []Function
	var old Function
	for rows.Next() {
		var act Function
		var arg Argument
		var inOut string
		if err = rows.Scan(
			&act.Owner, &act.Package, &act.Name,
			&arg.Name, &arg.DataType, &inOut,
			&arg.Length, &arg.Precision, &arg.Scale,
			&arg.TypeOwner, &arg.TypeName, &arg.TypeSubname, &arg.TypeLink,
			&arg.TypeObjectType, &arg.PlsType,
		); err != nil {
			return funcs, fmt.Errorf("%s [%q, %q]: %w", qry, owner, pkg, err)
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
	if old.Name != "" {
		funcs = append(funcs, old)
	}
	return funcs, nil
}

func splitOwner(name string) (string, string) {
	if i := strings.IndexByte(name, '.'); i >= 0 {
		return name[:i], name[i+1:]
	}
	return "", name
}
