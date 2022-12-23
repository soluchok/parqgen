package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"html/template"
	"log"
	"os"
)

var source = flag.String("source", "examples/querydata/querydata.go", "")
var typeName = flag.String("type", "QueryData", "")
var destination = flag.String("destination", "examples/querydata/querydata_gen.go", "")

type BasicValue struct {
	Name   string
	Type   string
	IsList bool
	IsMap  bool
	Key    string
	Value  string
}

type Info struct {
	Name        string
	BasicValues []BasicValue
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

		info = &Info{Name: ts.Name.String()}

		for _, field := range _x.Fields.List {
			ident, ok := field.Type.(*ast.Ident)
			if ok {
				info.BasicValues = append(info.BasicValues, BasicValue{
					Name: field.Names[0].Name,
					Type: toParquetType(ident.Name),
				})

				continue
			}

			arrayType, ok := field.Type.(*ast.ArrayType)
			if ok {
				ident, ok := arrayType.Elt.(*ast.Ident)
				if ok {
					info.BasicValues = append(info.BasicValues, BasicValue{
						Name:   field.Names[0].Name,
						IsList: true,
						Type:   toParquetType(ident.Name),
					})

					continue
				}
			}

			mapType, ok := field.Type.(*ast.MapType)
			if ok {
				keyIdent, kOk := mapType.Key.(*ast.Ident)
				valueIdent, vOk := mapType.Value.(*ast.Ident)
				if kOk && vOk {
					info.BasicValues = append(info.BasicValues, BasicValue{
						Name:  field.Names[0].Name,
						Type:  toParquetType(keyIdent.Name),
						IsMap: true,
						Key:   toParquetType(keyIdent.Name),
					})

					info.BasicValues = append(info.BasicValues, BasicValue{
						Name:  field.Names[0].Name,
						Type:  toParquetType(valueIdent.Name),
						IsMap: true,
						Value: toParquetType(valueIdent.Name),
					})

					continue
				}
			}

			fmt.Println(fmt.Sprintf("%T", field.Type))
			panic("not supported")
			//fmt.Printf("Field: %s %T \n", field.Names[0].Name, field.Type)
			//
			//mp, ok := field.Type.(*ast.MapType)
			//if ok {
			//	fmt.Printf("map[%s]%s\n", mp.Key, mp.Value)
			//}

			//fmt.Printf("Tag:   %s\n", field.Tag.Value)
		}
		return false
	})

	tmpl, err := template.ParseFiles("template/template.tmpl")
	if err != nil {
		log.Fatal(err)
	}

	var doc bytes.Buffer
	if err = tmpl.Execute(&doc, info); err != nil {
		log.Fatal(err)
	}

	if err = os.WriteFile(*destination, doc.Bytes(), 0600); err != nil {
		log.Fatal(err)
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
