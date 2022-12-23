package main

import (
	"bytes"
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

var source = flag.String("source", "examples/querydata/querydata.go", "")
var typeName = flag.String("type", "QueryData", "")
var destination = flag.String("destination", "examples/querydata/querydata_gen.go", "")

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
	StructName string
	Values     []Value
}

func main() {
	flag.Parse()

	var tfs = token.NewFileSet()
	file, err := parser.ParseFile(tfs, *source, nil, parser.ParseComments)
	if err != nil {
		panic(err)
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

		info = &Info{StructName: ts.Name.String()}

		for _, field := range _x.Fields.List {

			var values = process(Value{}, field, field.Type)

			info.Values = append(info.Values, values...)
		}

		return false
	})

	_tmpl := template.New("template.tmpl").Funcs(template.FuncMap{
		"inc": func(n int) int {
			return n + 1
		},
	})

	for i := range info.Values {
		fmt.Println(info.Values[i])
	}
	tmpl, err := _tmpl.ParseFiles("template/template.tmpl")
	if err != nil {
		log.Fatal(err)
	}

	var doc bytes.Buffer
	if err = tmpl.Execute(&doc, info); err != nil {
		log.Fatal(err)
	}

	//formated := doc.Bytes()

	formated, err := format.Source(doc.Bytes())
	if err != nil {
		log.Fatal(err)
	}

	if err = os.WriteFile(*destination, formated, 0600); err != nil {
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

func toParquetType(v string) string {
	switch v {
	case "string":
		return "parquet.ByteArray"
	default:
		return v
	}
}
