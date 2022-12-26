package recursivemap

//go:generate go run ../../main.go -source recursivemap.go -destination recursivemap_gen.go -type RecursiveMap -package recursivemap

type RecursiveMap struct {
	Col1  string
	Col2  []string
	Col3  [][]string
	Col4  [][][]string
	Col5  map[string]string
	Col6  map[string][]string
	Col7  map[string][][]string
	Col8  map[string][][][]string
	Col9  []map[string]string
	Col10 []map[string][]string
	Col11 [][]map[string][][]string
	Col12 [][][]map[string][][][]string
	Col13 map[string]map[string]string
	Col14 map[string]map[string][]string
	Col15 map[string]map[string][][]string
	Col16 map[string]map[string][]map[string]map[string]string
	Col17 map[string]map[string][]map[string]map[string][]string
	Col18 map[string]map[string][][]map[string]map[string][][]string
	Col19 []map[string]map[string][]map[string]map[string]string
	Col20 []map[string]map[string][]map[string]map[string][]string
	Col21 [][]map[string]map[string][][]map[string]map[string][][]string
}
