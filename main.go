package main

import (
	"bytes"
	_ "embed"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"log"
	"os"
	"text/template"
)

//go:embed template/template.tmpl
var tmplSrc string

var (
	source      = flag.String("source", "", "")
	destination = flag.String("destination", "", "")
	typeName    = flag.String("type", "", "")
	packageName = flag.String("package", "", "")
)

type Value struct {
	Name   string
	Path   string
	Type   string
	Suffix string
	Stack  []Value
}

func (v Value) copy() Value {
	var stack = make([]Value, len(v.Stack))

	copy(stack, v.Stack)

	return Value{
		Name:   v.Name,
		Type:   v.Type,
		Suffix: v.Suffix,
		Path:   v.Path,
		Stack:  stack,
	}
}

type Info struct {
	TypeName    string
	PackageName string
	Values      []Value
}

func main() {
	checkFlags()

	file, err := parser.ParseFile(token.NewFileSet(), *source, nil, parser.ParseComments)
	if err != nil {
		log.Fatal(err)
	}

	var info *Info

	ast.Inspect(file, func(x ast.Node) bool {
		ts, ok := x.(*ast.TypeSpec)
		if !ok {
			return true
		}

		if ts.Name.String() != *typeName {
			return true
		}

		_x, ok := ts.Type.(*ast.StructType)
		if !ok {
			return true
		}

		info = &Info{TypeName: ts.Name.String(), PackageName: *packageName}

		for _, field := range _x.Fields.List {
			info.Values = append(info.Values, process(Value{}, field, field.Type)...)
		}

		return false
	})

	tmpl, err := template.New("template").Funcs(template.FuncMap{
		"inc": func(n int) int {
			return n + 1
		},
	}).Parse(tmplSrc)
	if err != nil {
		log.Fatal(err)
	}

	var goCode bytes.Buffer
	if err = tmpl.Execute(&goCode, info); err != nil {
		log.Fatal(err)
	}

	formattedGoCode, err := format.Source(goCode.Bytes())
	if err != nil {
		log.Fatal(err)
	}

	if err = os.WriteFile(*destination, formattedGoCode, 0600); err != nil {
		log.Fatal(err)
	}
}

func process(val Value, field *ast.Field, expr ast.Expr) []Value {
	ident, ok := expr.(*ast.Ident)
	if ok {
		val.Name = field.Names[0].Name
		val.Type = toParquetType(ident.Name)

		return []Value{val}
	}

	arrayType, ok := expr.(*ast.ArrayType)
	if ok {
		val.Stack = append(val.Stack, Value{
			Name: field.Names[0].Name,
			Type: "ArrayType",
		})

		return process(val, field, arrayType.Elt)
	}

	mapType, ok := expr.(*ast.MapType)
	if ok {
		var (
			keyVal = val.copy()
			valVal = val.copy()
		)

		keyVal.Suffix = "_mapKey"
		keyVal.Path += "_mapKey"
		keyVal.Stack = append(keyVal.Stack, Value{
			Name:   field.Names[0].Name,
			Type:   "MapType",
			Suffix: "_mapKey",
			Path:   "_mapKey",
		})

		valVal.Suffix = "_mapValue"
		valVal.Path += "_mapValue"
		valVal.Stack = append(valVal.Stack, Value{
			Name:   field.Names[0].Name,
			Type:   "MapType",
			Suffix: "_mapValue",
			Path:   "_mapValue",
		})

		return append(
			process(keyVal, field, mapType.Key),
			process(valVal, field, mapType.Value)...,
		)
	}

	panic(fmt.Sprintf("not supported: %T", field.Type))
}

func checkFlags() {
	flag.Parse()

	if len(*source) == 0 {
		log.Fatal("source was not provided --source file.go")
	}

	if len(*destination) == 0 {
		log.Fatal("destination was not provided --destination file_gen.go")
	}

	if len(*typeName) == 0 {
		log.Fatal("type was not provided --type TypeName")
	}

	if len(*packageName) == 0 {
		log.Fatal("package was not provided --package pkg")
	}
}

func toParquetType(v string) string {
	switch v {
	case "string":
		return "parquet.ByteArray"
	default:
		return v
	}
}
