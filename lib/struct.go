/*
Copyright 2013 TamĂĄs GulĂĄcsi

// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0
*/

package genocall

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// Log is discarded by default.
var Log = func(...interface{}) error { return nil }

const (
	MarkNull = "\u2400" // 0x2400 = nul
	//MarkValid  = "\u6eff" // 0x6eff = fill; full, satisfied
	MarkValid = "Valid" // 0x6eff = fill; full, satisfied
	//MarkHidden = "\u533f"     // 0x533f = hide
	MarkHidden = "_hidden"

	DefaultMaxVARCHARLength = 32767
	DefaultMaxCHARLength    = 10
)

type Function struct {
	Package, Name, Alias string
	Returns              *Argument  `json:",omitempty"`
	Args                 []Argument `json:",omitempty"`
	Documentation        string     `json:",omitempty"`
	Replacement          *Function  `json:",omitempty"`
	ReplacementIsJSON    bool       `json:",omitempty"`
	LastDDL              time.Time  `json:",omitempty"`
	handle               []string
	maxTableSize         int
}

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
	if f.Returns != nil &&
		f.Returns.IsOutput() && f.Returns.Type == "REF CURSOR" {
		return true
	}
	for _, arg := range f.Args {
		if arg.IsOutput() && arg.Type == "REF CURSOR" {
			return true
		}
	}
	return false
}

type direction uint8

func (dir direction) IsInput() bool  { return dir&DIR_IN > 0 }
func (dir direction) IsOutput() bool { return dir&DIR_OUT > 0 }
func (dir direction) String() string {
	switch dir {
	case DIR_IN:
		return "IN"
	case DIR_OUT:
		return "OUT"
	case DIR_INOUT:
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
		*dir = DIR_IN
	case "OUT":
		*dir = DIR_OUT
	case "INOUT":
		*dir = DIR_INOUT
	default:
		return fmt.Errorf("unknown dir %q", string(p))
	}
	return nil
}

const (
	DIR_IN    = direction(1)
	DIR_OUT   = direction(2)
	DIR_INOUT = direction(3)
)

type flavor uint8

func (f flavor) String() string {
	switch f {
	case FLAVOR_SIMPLE:
		return "SIMPLE"
	case FLAVOR_RECORD:
		return "RECORD"
	case FLAVOR_TABLE:
		return "TABLE"
	}
	return fmt.Sprintf("%d", f)
}
func (f flavor) MarshalText() ([]byte, error) {
	return []byte(f.String()), nil
}
func (f *flavor) UnmarshalText(p []byte) error {
	switch string(p) {
	case "SIMPLE":
		*f = FLAVOR_SIMPLE
	case "RECORD":
		*f = FLAVOR_RECORD
	case "TABLE":
		*f = FLAVOR_TABLE
	default:
		return fmt.Errorf("unknown flavor %q", string(p))
	}
	return nil
}

const (
	FLAVOR_SIMPLE = flavor(0)
	FLAVOR_RECORD = flavor(1)
	FLAVOR_TABLE  = flavor(2)
)

type Argument struct {
	RecordOf       []NamedArgument //this argument is a record (map) of this type
	Name           string
	Type, TypeName string
	AbsType        string
	Charset        string
	Charlength     uint
	TableOf        *Argument // this argument is a table (array) of this type
	goTypeName     string
	PlsType
	Flavor    flavor
	Direction direction
	Precision uint8
	Scale     uint8
	mu        *sync.Mutex
}
type NamedArgument struct {
	Name string
	*Argument
}

func (a Argument) String() string {
	typ := a.Type
	switch a.Flavor {
	case FLAVOR_RECORD:
		typ = fmt.Sprintf("%s{%v}", a.PlsType, a.RecordOf)
	case FLAVOR_TABLE:
		typ = fmt.Sprintf("%s[%v]", a.PlsType, a.TableOf)
	}
	return a.Name + " " + a.Direction.String() + " " + typ
}

func (a Argument) IsInput() bool {
	return a.Direction&DIR_IN > 0
}
func (a Argument) IsOutput() bool {
	return a.Direction&DIR_OUT > 0
}

func NewArgument(name, dataType, plsTypeName, typeName, dirName string, dir direction,
	charset string, precision, scale uint8, charlength uint, typ *PlsType) Argument {

	name = strings.ToLower(name)
	if typeName == "..@" {
		typeName = ""
	}
	if typeName != "" && typeName[len(typeName)-1] == '@' {
		typeName = typeName[:len(typeName)-1]
	}

	if dirName != "" {
		switch dirName {
		case "IN/OUT":
			dir = DIR_INOUT
		case "OUT":
			dir = DIR_OUT
		default:
			dir = DIR_IN
		}
	}
	if dir < DIR_IN {
		dir = DIR_IN
	}

	if typ == nil || typ.TypeName.Name == "" {
		typ = &PlsType{TypeName: TypeName{Name: plsTypeName}}
		if plsTypeName == "" {
			typ.TypeName.Name = typeName
		}
		if typeName == "" {
			typ.TypeName.Name = dataType
		}
	}
	arg := Argument{Name: name, Type: dataType, PlsType: *typ,
		TypeName: typeName, Direction: dir,
		Precision: precision, Scale: scale, Charlength: charlength,
		Charset: charset,
		mu:      new(sync.Mutex),
		AbsType: dataType,
	}
	if arg.PlsType.Name == "" {
		panic(fmt.Sprintf("empty PLS type of %#v, typ=%#v", arg, typ))
	}
	switch arg.Type {
	case "PL/SQL RECORD":
		arg.Flavor = FLAVOR_RECORD
		arg.RecordOf = make([]NamedArgument, 0, 1)
	case "TABLE", "PL/SQL TABLE", "REF CURSOR":
		arg.Flavor = FLAVOR_TABLE
		if typ.CollectionOf == nil {
			panic(fmt.Sprintf("empty CollectionOf type of %#v, typ=%#v", arg, typ))
		}
		arg.TableOf = &Argument{PlsType: *typ.CollectionOf}
	}

	switch arg.Type {
	case "CHAR", "NCHAR", "VARCHAR", "NVARCHAR", "VARCHAR2", "NVARCHAR2":
		if arg.Charlength == 0 {
			if strings.Contains(arg.Type, "VAR") {
				arg.Charlength = DefaultMaxVARCHARLength
			} else {
				arg.Charlength = DefaultMaxCHARLength
			}
		}
		arg.AbsType = fmt.Sprintf("%s(%d)", arg.Type, arg.Charlength)
	case "NUMBER":
		if arg.Scale > 0 {
			arg.AbsType = fmt.Sprintf("NUMBER(%d, %d)", arg.Precision, arg.Scale)
		} else if arg.Precision > 0 {
			arg.AbsType = fmt.Sprintf("NUMBER(%d)", arg.Precision)
		} else {
			arg.AbsType = "NUMBER"
		}
	case "PLS_INTEGER", "BINARY_INTEGER":
		arg.AbsType = "INTEGER(10)"
	}
	return arg
}

func UnoCap(text string) string {
	i := strings.Index(text, "_")
	if i == 0 {
		return capitalize(text)
	}
	return strings.ToUpper(text[:i]) + "_" + strings.ToLower(text[i+1:])
}
