package querydata

import (
	"fmt"
	"io"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/apache/arrow/go/v11/parquet/schema"
)

type values struct {
	values interface{}
	defLev []int16
	repLev []int16
}

type QueryDataWriter struct {
	schema *schema.Schema
	keys   [][][]interface{}
}

func NewQueryDataWriter() *QueryDataWriter {
	_schema, err := schema.NewSchemaFromStruct(QueryData{})
	if err != nil {
		panic(err)
	}

	var nodes schema.FieldList

	for i := 0; i < _schema.Root().NumFields(); i++ {
		nodes = append(nodes, _schema.Root().Field(i))
	}

	root, err := schema.NewGroupNode(_schema.Root().Name(), parquet.Repetitions.Required, nodes, -1)
	if err != nil {
		panic(err)
	}

	return &QueryDataWriter{schema: schema.NewSchema(root)}
}

func (s *QueryDataWriter) _Col1(data []QueryData) values {
	var n = len(data)

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		_values[i] = parquet.ByteArray(data[i].Col1)

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col2(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col2
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			_ = val1
			n++

		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].Col2

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			_values[nv] = parquet.ByteArray(val1)
			_def[n] = 1
			_rep[n] = _lastRep
			_lastRep = 1

			nv++
			n++

		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col3(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col3
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				_ = val2
				n++

			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].Col3

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				_values[nv] = parquet.ByteArray(val2)
				_def[n] = 2
				_rep[n] = _lastRep
				_lastRep = 2

				nv++
				n++

			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col4(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col4
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					_ = val3
					n++

				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].Col4

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					_values[nv] = parquet.ByteArray(val3)
					_def[n] = 3
					_rep[n] = _lastRep
					_lastRep = 3

					nv++
					n++

				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col5_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col5
		if len(val0) == 0 {
			n++
		}

		for val1 := range val0 {

			_ = val1
			n++

		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col5

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		var c0 int
		for val1 := range val0 {
			if 0 >= len(s.keys[i]) {
				s.keys[i] = append(s.keys[i], make([][]interface{}, 1-len(s.keys[i]))...)
			}

			s.keys[i][0] = append(s.keys[i][0], val1)

			c0++

			_values[nv] = parquet.ByteArray(val1)
			_def[n] = 1
			_rep[n] = _lastRep
			_lastRep = 1

			nv++
			n++

		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col5_mapValue(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col5
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			_ = val1
			n++

		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].Col5

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			val1 = val0[s.keys[i][0][0].(string)]
			s.keys[i][0] = s.keys[i][0][1:]

			_values[nv] = parquet.ByteArray(val1)
			_def[n] = 1
			_rep[n] = _lastRep
			_lastRep = 1

			nv++
			n++

		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col6_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col6
		if len(val0) == 0 {
			n++
		}

		for val1 := range val0 {

			_ = val1
			n++

		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col6

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		var c0 int
		for val1 := range val0 {
			if 0 >= len(s.keys[i]) {
				s.keys[i] = append(s.keys[i], make([][]interface{}, 1-len(s.keys[i]))...)
			}

			s.keys[i][0] = append(s.keys[i][0], val1)

			c0++

			_values[nv] = parquet.ByteArray(val1)
			_def[n] = 1
			_rep[n] = _lastRep
			_lastRep = 1

			nv++
			n++

		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col6_mapValue(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col6
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				_ = val2
				n++

			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].Col6

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			val1 = val0[s.keys[i][0][0].(string)]
			s.keys[i][0] = s.keys[i][0][1:]

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				_values[nv] = parquet.ByteArray(val2)
				_def[n] = 2
				_rep[n] = _lastRep
				_lastRep = 2

				nv++
				n++

			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col7_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col7
		if len(val0) == 0 {
			n++
		}

		for val1 := range val0 {

			_ = val1
			n++

		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col7

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		var c0 int
		for val1 := range val0 {
			if 0 >= len(s.keys[i]) {
				s.keys[i] = append(s.keys[i], make([][]interface{}, 1-len(s.keys[i]))...)
			}

			s.keys[i][0] = append(s.keys[i][0], val1)

			c0++

			_values[nv] = parquet.ByteArray(val1)
			_def[n] = 1
			_rep[n] = _lastRep
			_lastRep = 1

			nv++
			n++

		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col7_mapValue(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col7
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					_ = val3
					n++

				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].Col7

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			val1 = val0[s.keys[i][0][0].(string)]
			s.keys[i][0] = s.keys[i][0][1:]

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					_values[nv] = parquet.ByteArray(val3)
					_def[n] = 3
					_rep[n] = _lastRep
					_lastRep = 3

					nv++
					n++

				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col8_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col8
		if len(val0) == 0 {
			n++
		}

		for val1 := range val0 {

			_ = val1
			n++

		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col8

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		var c0 int
		for val1 := range val0 {
			if 0 >= len(s.keys[i]) {
				s.keys[i] = append(s.keys[i], make([][]interface{}, 1-len(s.keys[i]))...)
			}

			s.keys[i][0] = append(s.keys[i][0], val1)

			c0++

			_values[nv] = parquet.ByteArray(val1)
			_def[n] = 1
			_rep[n] = _lastRep
			_lastRep = 1

			nv++
			n++

		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col8_mapValue(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col8
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for _, val4 := range val3 {

						_ = val4
						n++

					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].Col8

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			val1 = val0[s.keys[i][0][0].(string)]
			s.keys[i][0] = s.keys[i][0][1:]

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					for _, val4 := range val3 {

						_values[nv] = parquet.ByteArray(val4)
						_def[n] = 4
						_rep[n] = _lastRep
						_lastRep = 4

						nv++
						n++

					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col9_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col9
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for val2 := range val1 {

				_ = val2
				n++

			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col9

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			var c1 int
			for val2 := range val1 {
				if 1 >= len(s.keys[i]) {
					s.keys[i] = append(s.keys[i], make([][]interface{}, 2-len(s.keys[i]))...)
				}

				s.keys[i][1] = append(s.keys[i][1], val2)

				c1++

				_values[nv] = parquet.ByteArray(val2)
				_def[n] = 2
				_rep[n] = _lastRep
				_lastRep = 2

				nv++
				n++

			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col9_mapValue(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col9
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				_ = val2
				n++

			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].Col9

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				val2 = val1[s.keys[i][1][0].(string)]
				s.keys[i][1] = s.keys[i][1][1:]

				_values[nv] = parquet.ByteArray(val2)
				_def[n] = 2
				_rep[n] = _lastRep
				_lastRep = 2

				nv++
				n++

			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col10_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col10
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for val2 := range val1 {

				_ = val2
				n++

			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col10

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			var c1 int
			for val2 := range val1 {
				if 1 >= len(s.keys[i]) {
					s.keys[i] = append(s.keys[i], make([][]interface{}, 2-len(s.keys[i]))...)
				}

				s.keys[i][1] = append(s.keys[i][1], val2)

				c1++

				_values[nv] = parquet.ByteArray(val2)
				_def[n] = 2
				_rep[n] = _lastRep
				_lastRep = 2

				nv++
				n++

			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col10_mapValue(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col10
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					_ = val3
					n++

				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].Col10

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				val2 = val1[s.keys[i][1][0].(string)]
				s.keys[i][1] = s.keys[i][1][1:]

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					_values[nv] = parquet.ByteArray(val3)
					_def[n] = 3
					_rep[n] = _lastRep
					_lastRep = 3

					nv++
					n++

				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col11_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col11
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for val3 := range val2 {

					_ = val3
					n++

				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col11

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				var c2 int
				for val3 := range val2 {
					if 2 >= len(s.keys[i]) {
						s.keys[i] = append(s.keys[i], make([][]interface{}, 3-len(s.keys[i]))...)
					}

					s.keys[i][2] = append(s.keys[i][2], val3)

					c2++

					_values[nv] = parquet.ByteArray(val3)
					_def[n] = 3
					_rep[n] = _lastRep
					_lastRep = 3

					nv++
					n++

				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col11_mapValue(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col11
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							n++
						}

						for _, val5 := range val4 {

							_ = val5
							n++

						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].Col11

		var loc2 []interface{}
		if len(s.keys[i]) > 2 {
			loc2 = s.keys[i][2]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					val3 = val2[loc2[0].(string)]
					loc2 = loc2[1:]

					val3 = val2[s.keys[i][2][0].(string)]
					s.keys[i][2] = s.keys[i][2][1:]

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							_def[n] = 4
							_rep[n] = _lastRep
							_lastRep = 5
							n++
						}

						for _, val5 := range val4 {

							_values[nv] = parquet.ByteArray(val5)
							_def[n] = 5
							_rep[n] = _lastRep
							_lastRep = 5

							nv++
							n++

						}
						_lastRep--
					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col12_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col12
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for val4 := range val3 {

						_ = val4
						n++

					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col12

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					var c3 int
					for val4 := range val3 {
						if 3 >= len(s.keys[i]) {
							s.keys[i] = append(s.keys[i], make([][]interface{}, 4-len(s.keys[i]))...)
						}

						s.keys[i][3] = append(s.keys[i][3], val4)

						c3++

						_values[nv] = parquet.ByteArray(val4)
						_def[n] = 4
						_rep[n] = _lastRep
						_lastRep = 4

						nv++
						n++

					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col12_mapValue(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col12
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							n++
						}

						for _, val5 := range val4 {

							if len(val5) == 0 {
								n++
							}

							for _, val6 := range val5 {

								if len(val6) == 0 {
									n++
								}

								for _, val7 := range val6 {

									_ = val7
									n++

								}
							}
						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].Col12

		var loc3 []interface{}
		if len(s.keys[i]) > 3 {
			loc3 = s.keys[i][3]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					for _, val4 := range val3 {

						val4 = val3[loc3[0].(string)]
						loc3 = loc3[1:]

						val4 = val3[s.keys[i][3][0].(string)]
						s.keys[i][3] = s.keys[i][3][1:]

						if len(val4) == 0 {
							_def[n] = 4
							_rep[n] = _lastRep
							_lastRep = 5
							n++
						}

						for _, val5 := range val4 {

							if len(val5) == 0 {
								_def[n] = 5
								_rep[n] = _lastRep
								_lastRep = 6
								n++
							}

							for _, val6 := range val5 {

								if len(val6) == 0 {
									_def[n] = 6
									_rep[n] = _lastRep
									_lastRep = 7
									n++
								}

								for _, val7 := range val6 {

									_values[nv] = parquet.ByteArray(val7)
									_def[n] = 7
									_rep[n] = _lastRep
									_lastRep = 7

									nv++
									n++

								}
								_lastRep--
							}
							_lastRep--
						}
						_lastRep--
					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col13_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col13
		if len(val0) == 0 {
			n++
		}

		for val1 := range val0 {

			_ = val1
			n++

		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col13

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		var c0 int
		for val1 := range val0 {
			if 0 >= len(s.keys[i]) {
				s.keys[i] = append(s.keys[i], make([][]interface{}, 1-len(s.keys[i]))...)
			}

			s.keys[i][0] = append(s.keys[i][0], val1)

			c0++

			_values[nv] = parquet.ByteArray(val1)
			_def[n] = 1
			_rep[n] = _lastRep
			_lastRep = 1

			nv++
			n++

		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col13_mapValue_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col13
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for val2 := range val1 {

				_ = val2
				n++

			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col13

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			var c1 int
			for val2 := range val1 {
				if 1 >= len(s.keys[i]) {
					s.keys[i] = append(s.keys[i], make([][]interface{}, 2-len(s.keys[i]))...)
				}

				s.keys[i][1] = append(s.keys[i][1], val2)

				c1++

				_values[nv] = parquet.ByteArray(val2)
				_def[n] = 2
				_rep[n] = _lastRep
				_lastRep = 2

				nv++
				n++

			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col13_mapValue_mapValue(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col13
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				_ = val2
				n++

			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].Col13

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			val1 = val0[s.keys[i][0][0].(string)]
			s.keys[i][0] = s.keys[i][0][1:]

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				val2 = val1[s.keys[i][1][0].(string)]
				s.keys[i][1] = s.keys[i][1][1:]

				_values[nv] = parquet.ByteArray(val2)
				_def[n] = 2
				_rep[n] = _lastRep
				_lastRep = 2

				nv++
				n++

			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col14_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col14
		if len(val0) == 0 {
			n++
		}

		for val1 := range val0 {

			_ = val1
			n++

		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col14

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		var c0 int
		for val1 := range val0 {
			if 0 >= len(s.keys[i]) {
				s.keys[i] = append(s.keys[i], make([][]interface{}, 1-len(s.keys[i]))...)
			}

			s.keys[i][0] = append(s.keys[i][0], val1)

			c0++

			_values[nv] = parquet.ByteArray(val1)
			_def[n] = 1
			_rep[n] = _lastRep
			_lastRep = 1

			nv++
			n++

		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col14_mapValue_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col14
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for val2 := range val1 {

				_ = val2
				n++

			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col14

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			var c1 int
			for val2 := range val1 {
				if 1 >= len(s.keys[i]) {
					s.keys[i] = append(s.keys[i], make([][]interface{}, 2-len(s.keys[i]))...)
				}

				s.keys[i][1] = append(s.keys[i][1], val2)

				c1++

				_values[nv] = parquet.ByteArray(val2)
				_def[n] = 2
				_rep[n] = _lastRep
				_lastRep = 2

				nv++
				n++

			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col14_mapValue_mapValue(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col14
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					_ = val3
					n++

				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].Col14

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			val1 = val0[s.keys[i][0][0].(string)]
			s.keys[i][0] = s.keys[i][0][1:]

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				val2 = val1[s.keys[i][1][0].(string)]
				s.keys[i][1] = s.keys[i][1][1:]

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					_values[nv] = parquet.ByteArray(val3)
					_def[n] = 3
					_rep[n] = _lastRep
					_lastRep = 3

					nv++
					n++

				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col15_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col15
		if len(val0) == 0 {
			n++
		}

		for val1 := range val0 {

			_ = val1
			n++

		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col15

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		var c0 int
		for val1 := range val0 {
			if 0 >= len(s.keys[i]) {
				s.keys[i] = append(s.keys[i], make([][]interface{}, 1-len(s.keys[i]))...)
			}

			s.keys[i][0] = append(s.keys[i][0], val1)

			c0++

			_values[nv] = parquet.ByteArray(val1)
			_def[n] = 1
			_rep[n] = _lastRep
			_lastRep = 1

			nv++
			n++

		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col15_mapValue_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col15
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for val2 := range val1 {

				_ = val2
				n++

			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col15

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			var c1 int
			for val2 := range val1 {
				if 1 >= len(s.keys[i]) {
					s.keys[i] = append(s.keys[i], make([][]interface{}, 2-len(s.keys[i]))...)
				}

				s.keys[i][1] = append(s.keys[i][1], val2)

				c1++

				_values[nv] = parquet.ByteArray(val2)
				_def[n] = 2
				_rep[n] = _lastRep
				_lastRep = 2

				nv++
				n++

			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col15_mapValue_mapValue(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col15
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for _, val4 := range val3 {

						_ = val4
						n++

					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].Col15

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			val1 = val0[s.keys[i][0][0].(string)]
			s.keys[i][0] = s.keys[i][0][1:]

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				val2 = val1[s.keys[i][1][0].(string)]
				s.keys[i][1] = s.keys[i][1][1:]

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					for _, val4 := range val3 {

						_values[nv] = parquet.ByteArray(val4)
						_def[n] = 4
						_rep[n] = _lastRep
						_lastRep = 4

						nv++
						n++

					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col16_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col16
		if len(val0) == 0 {
			n++
		}

		for val1 := range val0 {

			_ = val1
			n++

		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col16

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		var c0 int
		for val1 := range val0 {
			if 0 >= len(s.keys[i]) {
				s.keys[i] = append(s.keys[i], make([][]interface{}, 1-len(s.keys[i]))...)
			}

			s.keys[i][0] = append(s.keys[i][0], val1)

			c0++

			_values[nv] = parquet.ByteArray(val1)
			_def[n] = 1
			_rep[n] = _lastRep
			_lastRep = 1

			nv++
			n++

		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col16_mapValue_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col16
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for val2 := range val1 {

				_ = val2
				n++

			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col16

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			var c1 int
			for val2 := range val1 {
				if 1 >= len(s.keys[i]) {
					s.keys[i] = append(s.keys[i], make([][]interface{}, 2-len(s.keys[i]))...)
				}

				s.keys[i][1] = append(s.keys[i][1], val2)

				c1++

				_values[nv] = parquet.ByteArray(val2)
				_def[n] = 2
				_rep[n] = _lastRep
				_lastRep = 2

				nv++
				n++

			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col16_mapValue_mapValue_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col16
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for val4 := range val3 {

						_ = val4
						n++

					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col16

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					var c3 int
					for val4 := range val3 {
						if 3 >= len(s.keys[i]) {
							s.keys[i] = append(s.keys[i], make([][]interface{}, 4-len(s.keys[i]))...)
						}

						s.keys[i][3] = append(s.keys[i][3], val4)

						c3++

						_values[nv] = parquet.ByteArray(val4)
						_def[n] = 4
						_rep[n] = _lastRep
						_lastRep = 4

						nv++
						n++

					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col16_mapValue_mapValue_mapValue_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col16
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							n++
						}

						for val5 := range val4 {

							_ = val5
							n++

						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col16

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		var loc3 []interface{}
		if len(s.keys[i]) > 3 {
			loc3 = s.keys[i][3]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					for _, val4 := range val3 {

						val4 = val3[loc3[0].(string)]
						loc3 = loc3[1:]

						if len(val4) == 0 {
							_def[n] = 4
							_rep[n] = _lastRep
							_lastRep = 5
							n++
						}

						var c4 int
						for val5 := range val4 {
							if 4 >= len(s.keys[i]) {
								s.keys[i] = append(s.keys[i], make([][]interface{}, 5-len(s.keys[i]))...)
							}

							s.keys[i][4] = append(s.keys[i][4], val5)

							c4++

							_values[nv] = parquet.ByteArray(val5)
							_def[n] = 5
							_rep[n] = _lastRep
							_lastRep = 5

							nv++
							n++

						}
						_lastRep--
					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col16_mapValue_mapValue_mapValue_mapValue(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col16
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							n++
						}

						for _, val5 := range val4 {

							_ = val5
							n++

						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].Col16

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		var loc3 []interface{}
		if len(s.keys[i]) > 3 {
			loc3 = s.keys[i][3]
		}

		var loc4 []interface{}
		if len(s.keys[i]) > 4 {
			loc4 = s.keys[i][4]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			val1 = val0[s.keys[i][0][0].(string)]
			s.keys[i][0] = s.keys[i][0][1:]

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				val2 = val1[s.keys[i][1][0].(string)]
				s.keys[i][1] = s.keys[i][1][1:]

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					for _, val4 := range val3 {

						val4 = val3[loc3[0].(string)]
						loc3 = loc3[1:]

						val4 = val3[s.keys[i][3][0].(string)]
						s.keys[i][3] = s.keys[i][3][1:]

						if len(val4) == 0 {
							_def[n] = 4
							_rep[n] = _lastRep
							_lastRep = 5
							n++
						}

						for _, val5 := range val4 {

							val5 = val4[loc4[0].(string)]
							loc4 = loc4[1:]

							val5 = val4[s.keys[i][4][0].(string)]
							s.keys[i][4] = s.keys[i][4][1:]

							_values[nv] = parquet.ByteArray(val5)
							_def[n] = 5
							_rep[n] = _lastRep
							_lastRep = 5

							nv++
							n++

						}
						_lastRep--
					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col17_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col17
		if len(val0) == 0 {
			n++
		}

		for val1 := range val0 {

			_ = val1
			n++

		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col17

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		var c0 int
		for val1 := range val0 {
			if 0 >= len(s.keys[i]) {
				s.keys[i] = append(s.keys[i], make([][]interface{}, 1-len(s.keys[i]))...)
			}

			s.keys[i][0] = append(s.keys[i][0], val1)

			c0++

			_values[nv] = parquet.ByteArray(val1)
			_def[n] = 1
			_rep[n] = _lastRep
			_lastRep = 1

			nv++
			n++

		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col17_mapValue_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col17
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for val2 := range val1 {

				_ = val2
				n++

			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col17

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			var c1 int
			for val2 := range val1 {
				if 1 >= len(s.keys[i]) {
					s.keys[i] = append(s.keys[i], make([][]interface{}, 2-len(s.keys[i]))...)
				}

				s.keys[i][1] = append(s.keys[i][1], val2)

				c1++

				_values[nv] = parquet.ByteArray(val2)
				_def[n] = 2
				_rep[n] = _lastRep
				_lastRep = 2

				nv++
				n++

			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col17_mapValue_mapValue_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col17
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for val4 := range val3 {

						_ = val4
						n++

					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col17

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					var c3 int
					for val4 := range val3 {
						if 3 >= len(s.keys[i]) {
							s.keys[i] = append(s.keys[i], make([][]interface{}, 4-len(s.keys[i]))...)
						}

						s.keys[i][3] = append(s.keys[i][3], val4)

						c3++

						_values[nv] = parquet.ByteArray(val4)
						_def[n] = 4
						_rep[n] = _lastRep
						_lastRep = 4

						nv++
						n++

					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col17_mapValue_mapValue_mapValue_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col17
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							n++
						}

						for val5 := range val4 {

							_ = val5
							n++

						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col17

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		var loc3 []interface{}
		if len(s.keys[i]) > 3 {
			loc3 = s.keys[i][3]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					for _, val4 := range val3 {

						val4 = val3[loc3[0].(string)]
						loc3 = loc3[1:]

						if len(val4) == 0 {
							_def[n] = 4
							_rep[n] = _lastRep
							_lastRep = 5
							n++
						}

						var c4 int
						for val5 := range val4 {
							if 4 >= len(s.keys[i]) {
								s.keys[i] = append(s.keys[i], make([][]interface{}, 5-len(s.keys[i]))...)
							}

							s.keys[i][4] = append(s.keys[i][4], val5)

							c4++

							_values[nv] = parquet.ByteArray(val5)
							_def[n] = 5
							_rep[n] = _lastRep
							_lastRep = 5

							nv++
							n++

						}
						_lastRep--
					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col17_mapValue_mapValue_mapValue_mapValue(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col17
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							n++
						}

						for _, val5 := range val4 {

							if len(val5) == 0 {
								n++
							}

							for _, val6 := range val5 {

								_ = val6
								n++

							}
						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].Col17

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		var loc3 []interface{}
		if len(s.keys[i]) > 3 {
			loc3 = s.keys[i][3]
		}

		var loc4 []interface{}
		if len(s.keys[i]) > 4 {
			loc4 = s.keys[i][4]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			val1 = val0[s.keys[i][0][0].(string)]
			s.keys[i][0] = s.keys[i][0][1:]

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				val2 = val1[s.keys[i][1][0].(string)]
				s.keys[i][1] = s.keys[i][1][1:]

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					for _, val4 := range val3 {

						val4 = val3[loc3[0].(string)]
						loc3 = loc3[1:]

						val4 = val3[s.keys[i][3][0].(string)]
						s.keys[i][3] = s.keys[i][3][1:]

						if len(val4) == 0 {
							_def[n] = 4
							_rep[n] = _lastRep
							_lastRep = 5
							n++
						}

						for _, val5 := range val4 {

							val5 = val4[loc4[0].(string)]
							loc4 = loc4[1:]

							val5 = val4[s.keys[i][4][0].(string)]
							s.keys[i][4] = s.keys[i][4][1:]

							if len(val5) == 0 {
								_def[n] = 5
								_rep[n] = _lastRep
								_lastRep = 6
								n++
							}

							for _, val6 := range val5 {

								_values[nv] = parquet.ByteArray(val6)
								_def[n] = 6
								_rep[n] = _lastRep
								_lastRep = 6

								nv++
								n++

							}
							_lastRep--
						}
						_lastRep--
					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col18_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col18
		if len(val0) == 0 {
			n++
		}

		for val1 := range val0 {

			_ = val1
			n++

		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col18

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		var c0 int
		for val1 := range val0 {
			if 0 >= len(s.keys[i]) {
				s.keys[i] = append(s.keys[i], make([][]interface{}, 1-len(s.keys[i]))...)
			}

			s.keys[i][0] = append(s.keys[i][0], val1)

			c0++

			_values[nv] = parquet.ByteArray(val1)
			_def[n] = 1
			_rep[n] = _lastRep
			_lastRep = 1

			nv++
			n++

		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col18_mapValue_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col18
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for val2 := range val1 {

				_ = val2
				n++

			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col18

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			var c1 int
			for val2 := range val1 {
				if 1 >= len(s.keys[i]) {
					s.keys[i] = append(s.keys[i], make([][]interface{}, 2-len(s.keys[i]))...)
				}

				s.keys[i][1] = append(s.keys[i][1], val2)

				c1++

				_values[nv] = parquet.ByteArray(val2)
				_def[n] = 2
				_rep[n] = _lastRep
				_lastRep = 2

				nv++
				n++

			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col18_mapValue_mapValue_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col18
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							n++
						}

						for val5 := range val4 {

							_ = val5
							n++

						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col18

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							_def[n] = 4
							_rep[n] = _lastRep
							_lastRep = 5
							n++
						}

						var c4 int
						for val5 := range val4 {
							if 4 >= len(s.keys[i]) {
								s.keys[i] = append(s.keys[i], make([][]interface{}, 5-len(s.keys[i]))...)
							}

							s.keys[i][4] = append(s.keys[i][4], val5)

							c4++

							_values[nv] = parquet.ByteArray(val5)
							_def[n] = 5
							_rep[n] = _lastRep
							_lastRep = 5

							nv++
							n++

						}
						_lastRep--
					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col18_mapValue_mapValue_mapValue_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col18
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							n++
						}

						for _, val5 := range val4 {

							if len(val5) == 0 {
								n++
							}

							for val6 := range val5 {

								_ = val6
								n++

							}
						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col18

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		var loc4 []interface{}
		if len(s.keys[i]) > 4 {
			loc4 = s.keys[i][4]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							_def[n] = 4
							_rep[n] = _lastRep
							_lastRep = 5
							n++
						}

						for _, val5 := range val4 {

							val5 = val4[loc4[0].(string)]
							loc4 = loc4[1:]

							if len(val5) == 0 {
								_def[n] = 5
								_rep[n] = _lastRep
								_lastRep = 6
								n++
							}

							var c5 int
							for val6 := range val5 {
								if 5 >= len(s.keys[i]) {
									s.keys[i] = append(s.keys[i], make([][]interface{}, 6-len(s.keys[i]))...)
								}

								s.keys[i][5] = append(s.keys[i][5], val6)

								c5++

								_values[nv] = parquet.ByteArray(val6)
								_def[n] = 6
								_rep[n] = _lastRep
								_lastRep = 6

								nv++
								n++

							}
							_lastRep--
						}
						_lastRep--
					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col18_mapValue_mapValue_mapValue_mapValue(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col18
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							n++
						}

						for _, val5 := range val4 {

							if len(val5) == 0 {
								n++
							}

							for _, val6 := range val5 {

								if len(val6) == 0 {
									n++
								}

								for _, val7 := range val6 {

									if len(val7) == 0 {
										n++
									}

									for _, val8 := range val7 {

										_ = val8
										n++

									}
								}
							}
						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].Col18

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		var loc4 []interface{}
		if len(s.keys[i]) > 4 {
			loc4 = s.keys[i][4]
		}

		var loc5 []interface{}
		if len(s.keys[i]) > 5 {
			loc5 = s.keys[i][5]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			val1 = val0[s.keys[i][0][0].(string)]
			s.keys[i][0] = s.keys[i][0][1:]

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				val2 = val1[s.keys[i][1][0].(string)]
				s.keys[i][1] = s.keys[i][1][1:]

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							_def[n] = 4
							_rep[n] = _lastRep
							_lastRep = 5
							n++
						}

						for _, val5 := range val4 {

							val5 = val4[loc4[0].(string)]
							loc4 = loc4[1:]

							val5 = val4[s.keys[i][4][0].(string)]
							s.keys[i][4] = s.keys[i][4][1:]

							if len(val5) == 0 {
								_def[n] = 5
								_rep[n] = _lastRep
								_lastRep = 6
								n++
							}

							for _, val6 := range val5 {

								val6 = val5[loc5[0].(string)]
								loc5 = loc5[1:]

								val6 = val5[s.keys[i][5][0].(string)]
								s.keys[i][5] = s.keys[i][5][1:]

								if len(val6) == 0 {
									_def[n] = 6
									_rep[n] = _lastRep
									_lastRep = 7
									n++
								}

								for _, val7 := range val6 {

									if len(val7) == 0 {
										_def[n] = 7
										_rep[n] = _lastRep
										_lastRep = 8
										n++
									}

									for _, val8 := range val7 {

										_values[nv] = parquet.ByteArray(val8)
										_def[n] = 8
										_rep[n] = _lastRep
										_lastRep = 8

										nv++
										n++

									}
									_lastRep--
								}
								_lastRep--
							}
							_lastRep--
						}
						_lastRep--
					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col19_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col19
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for val2 := range val1 {

				_ = val2
				n++

			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col19

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			var c1 int
			for val2 := range val1 {
				if 1 >= len(s.keys[i]) {
					s.keys[i] = append(s.keys[i], make([][]interface{}, 2-len(s.keys[i]))...)
				}

				s.keys[i][1] = append(s.keys[i][1], val2)

				c1++

				_values[nv] = parquet.ByteArray(val2)
				_def[n] = 2
				_rep[n] = _lastRep
				_lastRep = 2

				nv++
				n++

			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col19_mapValue_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col19
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for val3 := range val2 {

					_ = val3
					n++

				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col19

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				var c2 int
				for val3 := range val2 {
					if 2 >= len(s.keys[i]) {
						s.keys[i] = append(s.keys[i], make([][]interface{}, 3-len(s.keys[i]))...)
					}

					s.keys[i][2] = append(s.keys[i][2], val3)

					c2++

					_values[nv] = parquet.ByteArray(val3)
					_def[n] = 3
					_rep[n] = _lastRep
					_lastRep = 3

					nv++
					n++

				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col19_mapValue_mapValue_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col19
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							n++
						}

						for val5 := range val4 {

							_ = val5
							n++

						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col19

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		var loc2 []interface{}
		if len(s.keys[i]) > 2 {
			loc2 = s.keys[i][2]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					val3 = val2[loc2[0].(string)]
					loc2 = loc2[1:]

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							_def[n] = 4
							_rep[n] = _lastRep
							_lastRep = 5
							n++
						}

						var c4 int
						for val5 := range val4 {
							if 4 >= len(s.keys[i]) {
								s.keys[i] = append(s.keys[i], make([][]interface{}, 5-len(s.keys[i]))...)
							}

							s.keys[i][4] = append(s.keys[i][4], val5)

							c4++

							_values[nv] = parquet.ByteArray(val5)
							_def[n] = 5
							_rep[n] = _lastRep
							_lastRep = 5

							nv++
							n++

						}
						_lastRep--
					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col19_mapValue_mapValue_mapValue_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col19
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							n++
						}

						for _, val5 := range val4 {

							if len(val5) == 0 {
								n++
							}

							for val6 := range val5 {

								_ = val6
								n++

							}
						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col19

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		var loc2 []interface{}
		if len(s.keys[i]) > 2 {
			loc2 = s.keys[i][2]
		}

		var loc4 []interface{}
		if len(s.keys[i]) > 4 {
			loc4 = s.keys[i][4]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					val3 = val2[loc2[0].(string)]
					loc2 = loc2[1:]

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							_def[n] = 4
							_rep[n] = _lastRep
							_lastRep = 5
							n++
						}

						for _, val5 := range val4 {

							val5 = val4[loc4[0].(string)]
							loc4 = loc4[1:]

							if len(val5) == 0 {
								_def[n] = 5
								_rep[n] = _lastRep
								_lastRep = 6
								n++
							}

							var c5 int
							for val6 := range val5 {
								if 5 >= len(s.keys[i]) {
									s.keys[i] = append(s.keys[i], make([][]interface{}, 6-len(s.keys[i]))...)
								}

								s.keys[i][5] = append(s.keys[i][5], val6)

								c5++

								_values[nv] = parquet.ByteArray(val6)
								_def[n] = 6
								_rep[n] = _lastRep
								_lastRep = 6

								nv++
								n++

							}
							_lastRep--
						}
						_lastRep--
					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col19_mapValue_mapValue_mapValue_mapValue(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col19
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							n++
						}

						for _, val5 := range val4 {

							if len(val5) == 0 {
								n++
							}

							for _, val6 := range val5 {

								_ = val6
								n++

							}
						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].Col19

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		var loc2 []interface{}
		if len(s.keys[i]) > 2 {
			loc2 = s.keys[i][2]
		}

		var loc4 []interface{}
		if len(s.keys[i]) > 4 {
			loc4 = s.keys[i][4]
		}

		var loc5 []interface{}
		if len(s.keys[i]) > 5 {
			loc5 = s.keys[i][5]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				val2 = val1[s.keys[i][1][0].(string)]
				s.keys[i][1] = s.keys[i][1][1:]

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					val3 = val2[loc2[0].(string)]
					loc2 = loc2[1:]

					val3 = val2[s.keys[i][2][0].(string)]
					s.keys[i][2] = s.keys[i][2][1:]

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							_def[n] = 4
							_rep[n] = _lastRep
							_lastRep = 5
							n++
						}

						for _, val5 := range val4 {

							val5 = val4[loc4[0].(string)]
							loc4 = loc4[1:]

							val5 = val4[s.keys[i][4][0].(string)]
							s.keys[i][4] = s.keys[i][4][1:]

							if len(val5) == 0 {
								_def[n] = 5
								_rep[n] = _lastRep
								_lastRep = 6
								n++
							}

							for _, val6 := range val5 {

								val6 = val5[loc5[0].(string)]
								loc5 = loc5[1:]

								val6 = val5[s.keys[i][5][0].(string)]
								s.keys[i][5] = s.keys[i][5][1:]

								_values[nv] = parquet.ByteArray(val6)
								_def[n] = 6
								_rep[n] = _lastRep
								_lastRep = 6

								nv++
								n++

							}
							_lastRep--
						}
						_lastRep--
					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col20_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col20
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for val2 := range val1 {

				_ = val2
				n++

			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col20

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			var c1 int
			for val2 := range val1 {
				if 1 >= len(s.keys[i]) {
					s.keys[i] = append(s.keys[i], make([][]interface{}, 2-len(s.keys[i]))...)
				}

				s.keys[i][1] = append(s.keys[i][1], val2)

				c1++

				_values[nv] = parquet.ByteArray(val2)
				_def[n] = 2
				_rep[n] = _lastRep
				_lastRep = 2

				nv++
				n++

			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col20_mapValue_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col20
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for val3 := range val2 {

					_ = val3
					n++

				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col20

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				var c2 int
				for val3 := range val2 {
					if 2 >= len(s.keys[i]) {
						s.keys[i] = append(s.keys[i], make([][]interface{}, 3-len(s.keys[i]))...)
					}

					s.keys[i][2] = append(s.keys[i][2], val3)

					c2++

					_values[nv] = parquet.ByteArray(val3)
					_def[n] = 3
					_rep[n] = _lastRep
					_lastRep = 3

					nv++
					n++

				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col20_mapValue_mapValue_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col20
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							n++
						}

						for val5 := range val4 {

							_ = val5
							n++

						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col20

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		var loc2 []interface{}
		if len(s.keys[i]) > 2 {
			loc2 = s.keys[i][2]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					val3 = val2[loc2[0].(string)]
					loc2 = loc2[1:]

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							_def[n] = 4
							_rep[n] = _lastRep
							_lastRep = 5
							n++
						}

						var c4 int
						for val5 := range val4 {
							if 4 >= len(s.keys[i]) {
								s.keys[i] = append(s.keys[i], make([][]interface{}, 5-len(s.keys[i]))...)
							}

							s.keys[i][4] = append(s.keys[i][4], val5)

							c4++

							_values[nv] = parquet.ByteArray(val5)
							_def[n] = 5
							_rep[n] = _lastRep
							_lastRep = 5

							nv++
							n++

						}
						_lastRep--
					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col20_mapValue_mapValue_mapValue_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col20
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							n++
						}

						for _, val5 := range val4 {

							if len(val5) == 0 {
								n++
							}

							for val6 := range val5 {

								_ = val6
								n++

							}
						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col20

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		var loc2 []interface{}
		if len(s.keys[i]) > 2 {
			loc2 = s.keys[i][2]
		}

		var loc4 []interface{}
		if len(s.keys[i]) > 4 {
			loc4 = s.keys[i][4]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					val3 = val2[loc2[0].(string)]
					loc2 = loc2[1:]

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							_def[n] = 4
							_rep[n] = _lastRep
							_lastRep = 5
							n++
						}

						for _, val5 := range val4 {

							val5 = val4[loc4[0].(string)]
							loc4 = loc4[1:]

							if len(val5) == 0 {
								_def[n] = 5
								_rep[n] = _lastRep
								_lastRep = 6
								n++
							}

							var c5 int
							for val6 := range val5 {
								if 5 >= len(s.keys[i]) {
									s.keys[i] = append(s.keys[i], make([][]interface{}, 6-len(s.keys[i]))...)
								}

								s.keys[i][5] = append(s.keys[i][5], val6)

								c5++

								_values[nv] = parquet.ByteArray(val6)
								_def[n] = 6
								_rep[n] = _lastRep
								_lastRep = 6

								nv++
								n++

							}
							_lastRep--
						}
						_lastRep--
					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col20_mapValue_mapValue_mapValue_mapValue(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col20
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							n++
						}

						for _, val5 := range val4 {

							if len(val5) == 0 {
								n++
							}

							for _, val6 := range val5 {

								if len(val6) == 0 {
									n++
								}

								for _, val7 := range val6 {

									_ = val7
									n++

								}
							}
						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].Col20

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		var loc2 []interface{}
		if len(s.keys[i]) > 2 {
			loc2 = s.keys[i][2]
		}

		var loc4 []interface{}
		if len(s.keys[i]) > 4 {
			loc4 = s.keys[i][4]
		}

		var loc5 []interface{}
		if len(s.keys[i]) > 5 {
			loc5 = s.keys[i][5]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				val2 = val1[s.keys[i][1][0].(string)]
				s.keys[i][1] = s.keys[i][1][1:]

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					val3 = val2[loc2[0].(string)]
					loc2 = loc2[1:]

					val3 = val2[s.keys[i][2][0].(string)]
					s.keys[i][2] = s.keys[i][2][1:]

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							_def[n] = 4
							_rep[n] = _lastRep
							_lastRep = 5
							n++
						}

						for _, val5 := range val4 {

							val5 = val4[loc4[0].(string)]
							loc4 = loc4[1:]

							val5 = val4[s.keys[i][4][0].(string)]
							s.keys[i][4] = s.keys[i][4][1:]

							if len(val5) == 0 {
								_def[n] = 5
								_rep[n] = _lastRep
								_lastRep = 6
								n++
							}

							for _, val6 := range val5 {

								val6 = val5[loc5[0].(string)]
								loc5 = loc5[1:]

								val6 = val5[s.keys[i][5][0].(string)]
								s.keys[i][5] = s.keys[i][5][1:]

								if len(val6) == 0 {
									_def[n] = 6
									_rep[n] = _lastRep
									_lastRep = 7
									n++
								}

								for _, val7 := range val6 {

									_values[nv] = parquet.ByteArray(val7)
									_def[n] = 7
									_rep[n] = _lastRep
									_lastRep = 7

									nv++
									n++

								}
								_lastRep--
							}
							_lastRep--
						}
						_lastRep--
					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col21_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col21
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for val3 := range val2 {

					_ = val3
					n++

				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col21

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				var c2 int
				for val3 := range val2 {
					if 2 >= len(s.keys[i]) {
						s.keys[i] = append(s.keys[i], make([][]interface{}, 3-len(s.keys[i]))...)
					}

					s.keys[i][2] = append(s.keys[i][2], val3)

					c2++

					_values[nv] = parquet.ByteArray(val3)
					_def[n] = 3
					_rep[n] = _lastRep
					_lastRep = 3

					nv++
					n++

				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col21_mapValue_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col21
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for val4 := range val3 {

						_ = val4
						n++

					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col21

		var loc2 []interface{}
		if len(s.keys[i]) > 2 {
			loc2 = s.keys[i][2]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					val3 = val2[loc2[0].(string)]
					loc2 = loc2[1:]

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					var c3 int
					for val4 := range val3 {
						if 3 >= len(s.keys[i]) {
							s.keys[i] = append(s.keys[i], make([][]interface{}, 4-len(s.keys[i]))...)
						}

						s.keys[i][3] = append(s.keys[i][3], val4)

						c3++

						_values[nv] = parquet.ByteArray(val4)
						_def[n] = 4
						_rep[n] = _lastRep
						_lastRep = 4

						nv++
						n++

					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col21_mapValue_mapValue_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col21
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							n++
						}

						for _, val5 := range val4 {

							if len(val5) == 0 {
								n++
							}

							for _, val6 := range val5 {

								if len(val6) == 0 {
									n++
								}

								for val7 := range val6 {

									_ = val7
									n++

								}
							}
						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col21

		var loc2 []interface{}
		if len(s.keys[i]) > 2 {
			loc2 = s.keys[i][2]
		}

		var loc3 []interface{}
		if len(s.keys[i]) > 3 {
			loc3 = s.keys[i][3]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					val3 = val2[loc2[0].(string)]
					loc2 = loc2[1:]

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					for _, val4 := range val3 {

						val4 = val3[loc3[0].(string)]
						loc3 = loc3[1:]

						if len(val4) == 0 {
							_def[n] = 4
							_rep[n] = _lastRep
							_lastRep = 5
							n++
						}

						for _, val5 := range val4 {

							if len(val5) == 0 {
								_def[n] = 5
								_rep[n] = _lastRep
								_lastRep = 6
								n++
							}

							for _, val6 := range val5 {

								if len(val6) == 0 {
									_def[n] = 6
									_rep[n] = _lastRep
									_lastRep = 7
									n++
								}

								var c6 int
								for val7 := range val6 {
									if 6 >= len(s.keys[i]) {
										s.keys[i] = append(s.keys[i], make([][]interface{}, 7-len(s.keys[i]))...)
									}

									s.keys[i][6] = append(s.keys[i][6], val7)

									c6++

									_values[nv] = parquet.ByteArray(val7)
									_def[n] = 7
									_rep[n] = _lastRep
									_lastRep = 7

									nv++
									n++

								}
								_lastRep--
							}
							_lastRep--
						}
						_lastRep--
					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col21_mapValue_mapValue_mapValue_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col21
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							n++
						}

						for _, val5 := range val4 {

							if len(val5) == 0 {
								n++
							}

							for _, val6 := range val5 {

								if len(val6) == 0 {
									n++
								}

								for _, val7 := range val6 {

									if len(val7) == 0 {
										n++
									}

									for val8 := range val7 {

										_ = val8
										n++

									}
								}
							}
						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		if i >= len(s.keys) {
			s.keys = append(s.keys, [][]interface{}{})
		}

		var _lastRep int16
		var val0 = data[i].Col21

		var loc2 []interface{}
		if len(s.keys[i]) > 2 {
			loc2 = s.keys[i][2]
		}

		var loc3 []interface{}
		if len(s.keys[i]) > 3 {
			loc3 = s.keys[i][3]
		}

		var loc6 []interface{}
		if len(s.keys[i]) > 6 {
			loc6 = s.keys[i][6]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					val3 = val2[loc2[0].(string)]
					loc2 = loc2[1:]

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					for _, val4 := range val3 {

						val4 = val3[loc3[0].(string)]
						loc3 = loc3[1:]

						if len(val4) == 0 {
							_def[n] = 4
							_rep[n] = _lastRep
							_lastRep = 5
							n++
						}

						for _, val5 := range val4 {

							if len(val5) == 0 {
								_def[n] = 5
								_rep[n] = _lastRep
								_lastRep = 6
								n++
							}

							for _, val6 := range val5 {

								if len(val6) == 0 {
									_def[n] = 6
									_rep[n] = _lastRep
									_lastRep = 7
									n++
								}

								for _, val7 := range val6 {

									val7 = val6[loc6[0].(string)]
									loc6 = loc6[1:]

									if len(val7) == 0 {
										_def[n] = 7
										_rep[n] = _lastRep
										_lastRep = 8
										n++
									}

									var c7 int
									for val8 := range val7 {
										if 7 >= len(s.keys[i]) {
											s.keys[i] = append(s.keys[i], make([][]interface{}, 8-len(s.keys[i]))...)
										}

										s.keys[i][7] = append(s.keys[i][7], val8)

										c7++

										_values[nv] = parquet.ByteArray(val8)
										_def[n] = 8
										_rep[n] = _lastRep
										_lastRep = 8

										nv++
										n++

									}
									_lastRep--
								}
								_lastRep--
							}
							_lastRep--
						}
						_lastRep--
					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Col21_mapValue_mapValue_mapValue_mapValue(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Col21
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					n++
				}

				for _, val3 := range val2 {

					if len(val3) == 0 {
						n++
					}

					for _, val4 := range val3 {

						if len(val4) == 0 {
							n++
						}

						for _, val5 := range val4 {

							if len(val5) == 0 {
								n++
							}

							for _, val6 := range val5 {

								if len(val6) == 0 {
									n++
								}

								for _, val7 := range val6 {

									if len(val7) == 0 {
										n++
									}

									for _, val8 := range val7 {

										if len(val8) == 0 {
											n++
										}

										for _, val9 := range val8 {

											if len(val9) == 0 {
												n++
											}

											for _, val10 := range val9 {

												_ = val10
												n++

											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].Col21

		var loc2 []interface{}
		if len(s.keys[i]) > 2 {
			loc2 = s.keys[i][2]
		}

		var loc3 []interface{}
		if len(s.keys[i]) > 3 {
			loc3 = s.keys[i][3]
		}

		var loc6 []interface{}
		if len(s.keys[i]) > 6 {
			loc6 = s.keys[i][6]
		}

		var loc7 []interface{}
		if len(s.keys[i]) > 7 {
			loc7 = s.keys[i][7]
		}

		if len(val0) == 0 {
			_def[n] = 0
			_rep[n] = _lastRep
			_lastRep = 1
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				_def[n] = 1
				_rep[n] = _lastRep
				_lastRep = 2
				n++
			}

			for _, val2 := range val1 {

				if len(val2) == 0 {
					_def[n] = 2
					_rep[n] = _lastRep
					_lastRep = 3
					n++
				}

				for _, val3 := range val2 {

					val3 = val2[loc2[0].(string)]
					loc2 = loc2[1:]

					val3 = val2[s.keys[i][2][0].(string)]
					s.keys[i][2] = s.keys[i][2][1:]

					if len(val3) == 0 {
						_def[n] = 3
						_rep[n] = _lastRep
						_lastRep = 4
						n++
					}

					for _, val4 := range val3 {

						val4 = val3[loc3[0].(string)]
						loc3 = loc3[1:]

						val4 = val3[s.keys[i][3][0].(string)]
						s.keys[i][3] = s.keys[i][3][1:]

						if len(val4) == 0 {
							_def[n] = 4
							_rep[n] = _lastRep
							_lastRep = 5
							n++
						}

						for _, val5 := range val4 {

							if len(val5) == 0 {
								_def[n] = 5
								_rep[n] = _lastRep
								_lastRep = 6
								n++
							}

							for _, val6 := range val5 {

								if len(val6) == 0 {
									_def[n] = 6
									_rep[n] = _lastRep
									_lastRep = 7
									n++
								}

								for _, val7 := range val6 {

									val7 = val6[loc6[0].(string)]
									loc6 = loc6[1:]

									val7 = val6[s.keys[i][6][0].(string)]
									s.keys[i][6] = s.keys[i][6][1:]

									if len(val7) == 0 {
										_def[n] = 7
										_rep[n] = _lastRep
										_lastRep = 8
										n++
									}

									for _, val8 := range val7 {

										val8 = val7[loc7[0].(string)]
										loc7 = loc7[1:]

										val8 = val7[s.keys[i][7][0].(string)]
										s.keys[i][7] = s.keys[i][7][1:]

										if len(val8) == 0 {
											_def[n] = 8
											_rep[n] = _lastRep
											_lastRep = 9
											n++
										}

										for _, val9 := range val8 {

											if len(val9) == 0 {
												_def[n] = 9
												_rep[n] = _lastRep
												_lastRep = 10
												n++
											}

											for _, val10 := range val9 {

												_values[nv] = parquet.ByteArray(val10)
												_def[n] = 10
												_rep[n] = _lastRep
												_lastRep = 10

												nv++
												n++

											}
											_lastRep--
										}
										_lastRep--
									}
									_lastRep--
								}
								_lastRep--
							}
							_lastRep--
						}
						_lastRep--
					}
					_lastRep--
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) values(data []QueryData) []values {
	return []values{
		s._Col1(data),
		s._Col2(data),
		s._Col3(data),
		s._Col4(data),
		s._Col5_mapKey(data),
		s._Col5_mapValue(data),
		s._Col6_mapKey(data),
		s._Col6_mapValue(data),
		s._Col7_mapKey(data),
		s._Col7_mapValue(data),
		s._Col8_mapKey(data),
		s._Col8_mapValue(data),
		s._Col9_mapKey(data),
		s._Col9_mapValue(data),
		s._Col10_mapKey(data),
		s._Col10_mapValue(data),
		s._Col11_mapKey(data),
		s._Col11_mapValue(data),
		s._Col12_mapKey(data),
		s._Col12_mapValue(data),
		s._Col13_mapKey(data),
		s._Col13_mapValue_mapKey(data),
		s._Col13_mapValue_mapValue(data),
		s._Col14_mapKey(data),
		s._Col14_mapValue_mapKey(data),
		s._Col14_mapValue_mapValue(data),
		s._Col15_mapKey(data),
		s._Col15_mapValue_mapKey(data),
		s._Col15_mapValue_mapValue(data),
		s._Col16_mapKey(data),
		s._Col16_mapValue_mapKey(data),
		s._Col16_mapValue_mapValue_mapKey(data),
		s._Col16_mapValue_mapValue_mapValue_mapKey(data),
		s._Col16_mapValue_mapValue_mapValue_mapValue(data),
		s._Col17_mapKey(data),
		s._Col17_mapValue_mapKey(data),
		s._Col17_mapValue_mapValue_mapKey(data),
		s._Col17_mapValue_mapValue_mapValue_mapKey(data),
		s._Col17_mapValue_mapValue_mapValue_mapValue(data),
		s._Col18_mapKey(data),
		s._Col18_mapValue_mapKey(data),
		s._Col18_mapValue_mapValue_mapKey(data),
		s._Col18_mapValue_mapValue_mapValue_mapKey(data),
		s._Col18_mapValue_mapValue_mapValue_mapValue(data),
		s._Col19_mapKey(data),
		s._Col19_mapValue_mapKey(data),
		s._Col19_mapValue_mapValue_mapKey(data),
		s._Col19_mapValue_mapValue_mapValue_mapKey(data),
		s._Col19_mapValue_mapValue_mapValue_mapValue(data),
		s._Col20_mapKey(data),
		s._Col20_mapValue_mapKey(data),
		s._Col20_mapValue_mapValue_mapKey(data),
		s._Col20_mapValue_mapValue_mapValue_mapKey(data),
		s._Col20_mapValue_mapValue_mapValue_mapValue(data),
		s._Col21_mapKey(data),
		s._Col21_mapValue_mapKey(data),
		s._Col21_mapValue_mapValue_mapKey(data),
		s._Col21_mapValue_mapValue_mapValue_mapKey(data),
		s._Col21_mapValue_mapValue_mapValue_mapValue(data),
	}
}

func (s *QueryDataWriter) Write(out io.Writer, values []QueryData) {
	var writer = file.NewParquetWriter(out, s.schema.Root(), file.WithWriterProps(
		parquet.NewWriterProperties(
			parquet.WithVersion(parquet.V1_0),
			parquet.WithRootRepetition(parquet.Repetition(0)),
		),
	))
	defer writer.Close()

	var rgw = writer.AppendRowGroup()
	defer rgw.Close()

	for _, val := range s.values(values) {
		cw, err := rgw.NextColumn()
		if err != nil {
			panic(err)
		}

		switch w := cw.(type) {
		case *file.Int64ColumnChunkWriter:
			_, err = w.WriteBatch(val.values.([]int64), val.defLev, val.repLev)
			if err != nil {
				panic(err)
			}
		case *file.Int32ColumnChunkWriter:
			_, err = w.WriteBatch(val.values.([]int32), val.defLev, val.repLev)
			if err != nil {
				panic(err)
			}
		case *file.ByteArrayColumnChunkWriter:
			_, err = w.WriteBatch(val.values.([]parquet.ByteArray), val.defLev, val.repLev)
			if err != nil {
				panic(err)
			}
		case *file.Float64ColumnChunkWriter:
			_, err = w.WriteBatch(val.values.([]float64), val.defLev, val.repLev)
			if err != nil {
				panic(err)
			}
		case *file.BooleanColumnChunkWriter:
			_, err = w.WriteBatch(val.values.([]bool), val.defLev, val.repLev)
			if err != nil {
				panic(err)
			}
		default:
			panic(fmt.Sprintf("unimplemented: %T", w))
		}

		cw.Close()
	}
}
