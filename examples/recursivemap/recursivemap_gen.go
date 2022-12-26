package recursivemap

import (
	"fmt"
	"io"
	"reflect"
	"unsafe"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/apache/arrow/go/v11/parquet/schema"
)

type values struct {
	values interface{}
	defLev []int16
	repLev []int16
}

type RecursiveMapWriter struct {
	schema     *schema.Schema
	properties []WriterProperty
	keys       [][][]interface{}
}

type WriterProperty = parquet.WriterProperty

func WithDictionaryFor(path string, dict bool) WriterProperty {
	return parquet.WithDictionaryFor(path, dict)
}

func WithDataPageSize(pgsize int64) WriterProperty {
	return parquet.WithDataPageSize(pgsize)
}

func NewRecursiveMapWriter(opts ...WriterProperty) *RecursiveMapWriter {
	_schema, err := schema.NewSchemaFromStruct(RecursiveMap{})
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

	var properties = []WriterProperty{
		parquet.WithVersion(parquet.V1_0),
		parquet.WithRootRepetition(parquet.Repetition(0)),
		parquet.WithDictionaryDefault(false),
	}

	return &RecursiveMapWriter{schema: schema.NewSchema(root), properties: append(properties, opts...)}
}

func unsafeByteArray(s string) parquet.ByteArray {
	if len(s) == 0 {
		return nil
	}

	return (*[0x7fff0000]byte)(unsafe.Pointer(
		(*reflect.StringHeader)(unsafe.Pointer(&s)).Data),
	)[:len(s):len(s)]
}

func (s *RecursiveMapWriter) _Col1(data []RecursiveMap) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].Col1)
	}

	return values{values: _values}
}

func (s *RecursiveMapWriter) _Col2(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col2
		if len(val0) == 0 {
			num++
			continue
		}

		num += len(val0)
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col2

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {

			_values[nv] = unsafeByteArray(val1)

			_def[n], _rep[n], _lastRep = 1, _lastRep, 1

			nv++
			n++
		}
		_lastRep--
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col3(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col3
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			num += len(val1)
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col3

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {

				_values[nv] = unsafeByteArray(val2)

				_def[n], _rep[n], _lastRep = 2, _lastRep, 2

				nv++
				n++
			}
			_lastRep--
		}
		_lastRep--
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col4(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col4
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				num += len(val2)
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col4

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {

					_values[nv] = unsafeByteArray(val3)

					_def[n], _rep[n], _lastRep = 3, _lastRep, 3

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

func (s *RecursiveMapWriter) _Col5_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col5
		if len(val0) == 0 {
			num++
			continue
		}

		num += len(val0)
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col5

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for val1 := range val0 {
			if 0 >= len(s.keys[i]) {
				s.keys[i] = append(s.keys[i], make([][]interface{}, 1-len(s.keys[i]))...)
			}

			s.keys[i][0] = append(s.keys[i][0], val1)

			_values[nv] = unsafeByteArray(val1)

			_def[n], _rep[n], _lastRep = 1, _lastRep, 1

			nv++
			n++
		}
		_lastRep--
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col5_mapValue(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col5
		if len(val0) == 0 {
			num++
			continue
		}

		num += len(val0)
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col5

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			val1, loc0 = val0[loc0[0].(string)], loc0[1:]

			_values[nv] = unsafeByteArray(val1)

			_def[n], _rep[n], _lastRep = 1, _lastRep, 1

			nv++
			n++
		}
		_lastRep--
		s.keys[i] = s.keys[i][len(s.keys[i]):]
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col6_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col6
		if len(val0) == 0 {
			num++
			continue
		}

		num += len(val0)
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col6

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for val1 := range val0 {
			if 0 >= len(s.keys[i]) {
				s.keys[i] = append(s.keys[i], make([][]interface{}, 1-len(s.keys[i]))...)
			}

			s.keys[i][0] = append(s.keys[i][0], val1)

			_values[nv] = unsafeByteArray(val1)

			_def[n], _rep[n], _lastRep = 1, _lastRep, 1

			nv++
			n++
		}
		_lastRep--
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col6_mapValue(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col6
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			num += len(val1)
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col6

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			val1, loc0 = val0[loc0[0].(string)], loc0[1:]
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {

				_values[nv] = unsafeByteArray(val2)

				_def[n], _rep[n], _lastRep = 2, _lastRep, 2

				nv++
				n++
			}
			_lastRep--
		}
		_lastRep--
		s.keys[i] = s.keys[i][len(s.keys[i]):]
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col7_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col7
		if len(val0) == 0 {
			num++
			continue
		}

		num += len(val0)
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col7

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for val1 := range val0 {
			if 0 >= len(s.keys[i]) {
				s.keys[i] = append(s.keys[i], make([][]interface{}, 1-len(s.keys[i]))...)
			}

			s.keys[i][0] = append(s.keys[i][0], val1)

			_values[nv] = unsafeByteArray(val1)

			_def[n], _rep[n], _lastRep = 1, _lastRep, 1

			nv++
			n++
		}
		_lastRep--
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col7_mapValue(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col7
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				num += len(val2)
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col7

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			val1, loc0 = val0[loc0[0].(string)], loc0[1:]
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {

					_values[nv] = unsafeByteArray(val3)

					_def[n], _rep[n], _lastRep = 3, _lastRep, 3

					nv++
					n++
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--
		s.keys[i] = s.keys[i][len(s.keys[i]):]
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col8_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col8
		if len(val0) == 0 {
			num++
			continue
		}

		num += len(val0)
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col8

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for val1 := range val0 {
			if 0 >= len(s.keys[i]) {
				s.keys[i] = append(s.keys[i], make([][]interface{}, 1-len(s.keys[i]))...)
			}

			s.keys[i][0] = append(s.keys[i][0], val1)

			_values[nv] = unsafeByteArray(val1)

			_def[n], _rep[n], _lastRep = 1, _lastRep, 1

			nv++
			n++
		}
		_lastRep--
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col8_mapValue(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col8
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					num += len(val3)
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col8

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			val1, loc0 = val0[loc0[0].(string)], loc0[1:]
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for _, val4 := range val3 {

						_values[nv] = unsafeByteArray(val4)

						_def[n], _rep[n], _lastRep = 4, _lastRep, 4

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
		s.keys[i] = s.keys[i][len(s.keys[i]):]
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col9_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col9
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			num += len(val1)
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col9

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for val2 := range val1 {
				if 1 >= len(s.keys[i]) {
					s.keys[i] = append(s.keys[i], make([][]interface{}, 2-len(s.keys[i]))...)
				}

				s.keys[i][1] = append(s.keys[i][1], val2)

				_values[nv] = unsafeByteArray(val2)

				_def[n], _rep[n], _lastRep = 2, _lastRep, 2

				nv++
				n++
			}
			_lastRep--
		}
		_lastRep--
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col9_mapValue(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col9
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			num += len(val1)
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col9

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				val2, loc1 = val1[loc1[0].(string)], loc1[1:]

				_values[nv] = unsafeByteArray(val2)

				_def[n], _rep[n], _lastRep = 2, _lastRep, 2

				nv++
				n++
			}
			_lastRep--
		}
		_lastRep--
		s.keys[i] = s.keys[i][len(s.keys[i]):]
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col10_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col10
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			num += len(val1)
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col10

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for val2 := range val1 {
				if 1 >= len(s.keys[i]) {
					s.keys[i] = append(s.keys[i], make([][]interface{}, 2-len(s.keys[i]))...)
				}

				s.keys[i][1] = append(s.keys[i][1], val2)

				_values[nv] = unsafeByteArray(val2)

				_def[n], _rep[n], _lastRep = 2, _lastRep, 2

				nv++
				n++
			}
			_lastRep--
		}
		_lastRep--
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col10_mapValue(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col10
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				num += len(val2)
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col10

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				val2, loc1 = val1[loc1[0].(string)], loc1[1:]
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {

					_values[nv] = unsafeByteArray(val3)

					_def[n], _rep[n], _lastRep = 3, _lastRep, 3

					nv++
					n++
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--
		s.keys[i] = s.keys[i][len(s.keys[i]):]
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col11_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col11
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				num += len(val2)
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col11

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for val3 := range val2 {
					if 2 >= len(s.keys[i]) {
						s.keys[i] = append(s.keys[i], make([][]interface{}, 3-len(s.keys[i]))...)
					}

					s.keys[i][2] = append(s.keys[i][2], val3)

					_values[nv] = unsafeByteArray(val3)

					_def[n], _rep[n], _lastRep = 3, _lastRep, 3

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

func (s *RecursiveMapWriter) _Col11_mapValue(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col11
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							num++
							continue
						}

						num += len(val4)
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col11

		var loc2 []interface{}
		if len(s.keys[i]) > 2 {
			loc2 = s.keys[i][2]
		}

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					val3, loc2 = val2[loc2[0].(string)], loc2[1:]
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							_def[n], _rep[n], _lastRep = 4, _lastRep, 5
							n++
						}

						for _, val5 := range val4 {

							_values[nv] = unsafeByteArray(val5)

							_def[n], _rep[n], _lastRep = 5, _lastRep, 5

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
		s.keys[i] = s.keys[i][len(s.keys[i]):]
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col12_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col12
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					num += len(val3)
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col12

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for val4 := range val3 {
						if 3 >= len(s.keys[i]) {
							s.keys[i] = append(s.keys[i], make([][]interface{}, 4-len(s.keys[i]))...)
						}

						s.keys[i][3] = append(s.keys[i][3], val4)

						_values[nv] = unsafeByteArray(val4)

						_def[n], _rep[n], _lastRep = 4, _lastRep, 4

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

func (s *RecursiveMapWriter) _Col12_mapValue(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col12
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							num++
							continue
						}

						for _, val5 := range val4 {
							if len(val5) == 0 {
								num++
								continue
							}

							for _, val6 := range val5 {
								if len(val6) == 0 {
									num++
									continue
								}

								num += len(val6)
							}
						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col12

		var loc3 []interface{}
		if len(s.keys[i]) > 3 {
			loc3 = s.keys[i][3]
		}

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for _, val4 := range val3 {
						val4, loc3 = val3[loc3[0].(string)], loc3[1:]
						if len(val4) == 0 {
							_def[n], _rep[n], _lastRep = 4, _lastRep, 5
							n++
						}

						for _, val5 := range val4 {
							if len(val5) == 0 {
								_def[n], _rep[n], _lastRep = 5, _lastRep, 6
								n++
							}

							for _, val6 := range val5 {
								if len(val6) == 0 {
									_def[n], _rep[n], _lastRep = 6, _lastRep, 7
									n++
								}

								for _, val7 := range val6 {

									_values[nv] = unsafeByteArray(val7)

									_def[n], _rep[n], _lastRep = 7, _lastRep, 7

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
		s.keys[i] = s.keys[i][len(s.keys[i]):]
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col13_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col13
		if len(val0) == 0 {
			num++
			continue
		}

		num += len(val0)
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col13

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for val1 := range val0 {
			if 0 >= len(s.keys[i]) {
				s.keys[i] = append(s.keys[i], make([][]interface{}, 1-len(s.keys[i]))...)
			}

			s.keys[i][0] = append(s.keys[i][0], val1)

			_values[nv] = unsafeByteArray(val1)

			_def[n], _rep[n], _lastRep = 1, _lastRep, 1

			nv++
			n++
		}
		_lastRep--
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col13_mapValue_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col13
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			num += len(val1)
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col13

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			val1, loc0 = val0[loc0[0].(string)], loc0[1:]
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for val2 := range val1 {
				if 1 >= len(s.keys[i]) {
					s.keys[i] = append(s.keys[i], make([][]interface{}, 2-len(s.keys[i]))...)
				}

				s.keys[i][1] = append(s.keys[i][1], val2)

				_values[nv] = unsafeByteArray(val2)

				_def[n], _rep[n], _lastRep = 2, _lastRep, 2

				nv++
				n++
			}
			_lastRep--
		}
		_lastRep--
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col13_mapValue_mapValue(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col13
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			num += len(val1)
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

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
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			val1, loc0 = val0[loc0[0].(string)], loc0[1:]
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				val2, loc1 = val1[loc1[0].(string)], loc1[1:]

				_values[nv] = unsafeByteArray(val2)

				_def[n], _rep[n], _lastRep = 2, _lastRep, 2

				nv++
				n++
			}
			_lastRep--
		}
		_lastRep--
		s.keys[i] = s.keys[i][len(s.keys[i]):]
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col14_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col14
		if len(val0) == 0 {
			num++
			continue
		}

		num += len(val0)
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col14

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for val1 := range val0 {
			if 0 >= len(s.keys[i]) {
				s.keys[i] = append(s.keys[i], make([][]interface{}, 1-len(s.keys[i]))...)
			}

			s.keys[i][0] = append(s.keys[i][0], val1)

			_values[nv] = unsafeByteArray(val1)

			_def[n], _rep[n], _lastRep = 1, _lastRep, 1

			nv++
			n++
		}
		_lastRep--
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col14_mapValue_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col14
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			num += len(val1)
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col14

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			val1, loc0 = val0[loc0[0].(string)], loc0[1:]
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for val2 := range val1 {
				if 1 >= len(s.keys[i]) {
					s.keys[i] = append(s.keys[i], make([][]interface{}, 2-len(s.keys[i]))...)
				}

				s.keys[i][1] = append(s.keys[i][1], val2)

				_values[nv] = unsafeByteArray(val2)

				_def[n], _rep[n], _lastRep = 2, _lastRep, 2

				nv++
				n++
			}
			_lastRep--
		}
		_lastRep--
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col14_mapValue_mapValue(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col14
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				num += len(val2)
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

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
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			val1, loc0 = val0[loc0[0].(string)], loc0[1:]
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				val2, loc1 = val1[loc1[0].(string)], loc1[1:]
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {

					_values[nv] = unsafeByteArray(val3)

					_def[n], _rep[n], _lastRep = 3, _lastRep, 3

					nv++
					n++
				}
				_lastRep--
			}
			_lastRep--
		}
		_lastRep--
		s.keys[i] = s.keys[i][len(s.keys[i]):]
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col15_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col15
		if len(val0) == 0 {
			num++
			continue
		}

		num += len(val0)
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col15

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for val1 := range val0 {
			if 0 >= len(s.keys[i]) {
				s.keys[i] = append(s.keys[i], make([][]interface{}, 1-len(s.keys[i]))...)
			}

			s.keys[i][0] = append(s.keys[i][0], val1)

			_values[nv] = unsafeByteArray(val1)

			_def[n], _rep[n], _lastRep = 1, _lastRep, 1

			nv++
			n++
		}
		_lastRep--
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col15_mapValue_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col15
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			num += len(val1)
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col15

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			val1, loc0 = val0[loc0[0].(string)], loc0[1:]
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for val2 := range val1 {
				if 1 >= len(s.keys[i]) {
					s.keys[i] = append(s.keys[i], make([][]interface{}, 2-len(s.keys[i]))...)
				}

				s.keys[i][1] = append(s.keys[i][1], val2)

				_values[nv] = unsafeByteArray(val2)

				_def[n], _rep[n], _lastRep = 2, _lastRep, 2

				nv++
				n++
			}
			_lastRep--
		}
		_lastRep--
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col15_mapValue_mapValue(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col15
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					num += len(val3)
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

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
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			val1, loc0 = val0[loc0[0].(string)], loc0[1:]
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				val2, loc1 = val1[loc1[0].(string)], loc1[1:]
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for _, val4 := range val3 {

						_values[nv] = unsafeByteArray(val4)

						_def[n], _rep[n], _lastRep = 4, _lastRep, 4

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
		s.keys[i] = s.keys[i][len(s.keys[i]):]
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col16_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col16
		if len(val0) == 0 {
			num++
			continue
		}

		num += len(val0)
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col16

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for val1 := range val0 {
			if 0 >= len(s.keys[i]) {
				s.keys[i] = append(s.keys[i], make([][]interface{}, 1-len(s.keys[i]))...)
			}

			s.keys[i][0] = append(s.keys[i][0], val1)

			_values[nv] = unsafeByteArray(val1)

			_def[n], _rep[n], _lastRep = 1, _lastRep, 1

			nv++
			n++
		}
		_lastRep--
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col16_mapValue_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col16
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			num += len(val1)
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col16

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			val1, loc0 = val0[loc0[0].(string)], loc0[1:]
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for val2 := range val1 {
				if 1 >= len(s.keys[i]) {
					s.keys[i] = append(s.keys[i], make([][]interface{}, 2-len(s.keys[i]))...)
				}

				s.keys[i][1] = append(s.keys[i][1], val2)

				_values[nv] = unsafeByteArray(val2)

				_def[n], _rep[n], _lastRep = 2, _lastRep, 2

				nv++
				n++
			}
			_lastRep--
		}
		_lastRep--
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col16_mapValue_mapValue_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col16
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					num += len(val3)
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

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

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			val1, loc0 = val0[loc0[0].(string)], loc0[1:]
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				val2, loc1 = val1[loc1[0].(string)], loc1[1:]
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for val4 := range val3 {
						if 3 >= len(s.keys[i]) {
							s.keys[i] = append(s.keys[i], make([][]interface{}, 4-len(s.keys[i]))...)
						}

						s.keys[i][3] = append(s.keys[i][3], val4)

						_values[nv] = unsafeByteArray(val4)

						_def[n], _rep[n], _lastRep = 4, _lastRep, 4

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

func (s *RecursiveMapWriter) _Col16_mapValue_mapValue_mapValue_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col16
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							num++
							continue
						}

						num += len(val4)
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

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

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			val1, loc0 = val0[loc0[0].(string)], loc0[1:]
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				val2, loc1 = val1[loc1[0].(string)], loc1[1:]
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for _, val4 := range val3 {
						val4, loc3 = val3[loc3[0].(string)], loc3[1:]
						if len(val4) == 0 {
							_def[n], _rep[n], _lastRep = 4, _lastRep, 5
							n++
						}

						for val5 := range val4 {
							if 4 >= len(s.keys[i]) {
								s.keys[i] = append(s.keys[i], make([][]interface{}, 5-len(s.keys[i]))...)
							}

							s.keys[i][4] = append(s.keys[i][4], val5)

							_values[nv] = unsafeByteArray(val5)

							_def[n], _rep[n], _lastRep = 5, _lastRep, 5

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

func (s *RecursiveMapWriter) _Col16_mapValue_mapValue_mapValue_mapValue(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col16
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							num++
							continue
						}

						num += len(val4)
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

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
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			val1, loc0 = val0[loc0[0].(string)], loc0[1:]
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				val2, loc1 = val1[loc1[0].(string)], loc1[1:]
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for _, val4 := range val3 {
						val4, loc3 = val3[loc3[0].(string)], loc3[1:]
						if len(val4) == 0 {
							_def[n], _rep[n], _lastRep = 4, _lastRep, 5
							n++
						}

						for _, val5 := range val4 {
							val5, loc4 = val4[loc4[0].(string)], loc4[1:]

							_values[nv] = unsafeByteArray(val5)

							_def[n], _rep[n], _lastRep = 5, _lastRep, 5

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
		s.keys[i] = s.keys[i][len(s.keys[i]):]
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col17_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col17
		if len(val0) == 0 {
			num++
			continue
		}

		num += len(val0)
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col17

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for val1 := range val0 {
			if 0 >= len(s.keys[i]) {
				s.keys[i] = append(s.keys[i], make([][]interface{}, 1-len(s.keys[i]))...)
			}

			s.keys[i][0] = append(s.keys[i][0], val1)

			_values[nv] = unsafeByteArray(val1)

			_def[n], _rep[n], _lastRep = 1, _lastRep, 1

			nv++
			n++
		}
		_lastRep--
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col17_mapValue_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col17
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			num += len(val1)
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col17

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			val1, loc0 = val0[loc0[0].(string)], loc0[1:]
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for val2 := range val1 {
				if 1 >= len(s.keys[i]) {
					s.keys[i] = append(s.keys[i], make([][]interface{}, 2-len(s.keys[i]))...)
				}

				s.keys[i][1] = append(s.keys[i][1], val2)

				_values[nv] = unsafeByteArray(val2)

				_def[n], _rep[n], _lastRep = 2, _lastRep, 2

				nv++
				n++
			}
			_lastRep--
		}
		_lastRep--
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col17_mapValue_mapValue_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col17
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					num += len(val3)
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

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

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			val1, loc0 = val0[loc0[0].(string)], loc0[1:]
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				val2, loc1 = val1[loc1[0].(string)], loc1[1:]
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for val4 := range val3 {
						if 3 >= len(s.keys[i]) {
							s.keys[i] = append(s.keys[i], make([][]interface{}, 4-len(s.keys[i]))...)
						}

						s.keys[i][3] = append(s.keys[i][3], val4)

						_values[nv] = unsafeByteArray(val4)

						_def[n], _rep[n], _lastRep = 4, _lastRep, 4

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

func (s *RecursiveMapWriter) _Col17_mapValue_mapValue_mapValue_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col17
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							num++
							continue
						}

						num += len(val4)
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

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

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			val1, loc0 = val0[loc0[0].(string)], loc0[1:]
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				val2, loc1 = val1[loc1[0].(string)], loc1[1:]
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for _, val4 := range val3 {
						val4, loc3 = val3[loc3[0].(string)], loc3[1:]
						if len(val4) == 0 {
							_def[n], _rep[n], _lastRep = 4, _lastRep, 5
							n++
						}

						for val5 := range val4 {
							if 4 >= len(s.keys[i]) {
								s.keys[i] = append(s.keys[i], make([][]interface{}, 5-len(s.keys[i]))...)
							}

							s.keys[i][4] = append(s.keys[i][4], val5)

							_values[nv] = unsafeByteArray(val5)

							_def[n], _rep[n], _lastRep = 5, _lastRep, 5

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

func (s *RecursiveMapWriter) _Col17_mapValue_mapValue_mapValue_mapValue(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col17
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							num++
							continue
						}

						for _, val5 := range val4 {
							if len(val5) == 0 {
								num++
								continue
							}

							num += len(val5)
						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

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
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			val1, loc0 = val0[loc0[0].(string)], loc0[1:]
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				val2, loc1 = val1[loc1[0].(string)], loc1[1:]
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for _, val4 := range val3 {
						val4, loc3 = val3[loc3[0].(string)], loc3[1:]
						if len(val4) == 0 {
							_def[n], _rep[n], _lastRep = 4, _lastRep, 5
							n++
						}

						for _, val5 := range val4 {
							val5, loc4 = val4[loc4[0].(string)], loc4[1:]
							if len(val5) == 0 {
								_def[n], _rep[n], _lastRep = 5, _lastRep, 6
								n++
							}

							for _, val6 := range val5 {

								_values[nv] = unsafeByteArray(val6)

								_def[n], _rep[n], _lastRep = 6, _lastRep, 6

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
		s.keys[i] = s.keys[i][len(s.keys[i]):]
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col18_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col18
		if len(val0) == 0 {
			num++
			continue
		}

		num += len(val0)
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col18

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for val1 := range val0 {
			if 0 >= len(s.keys[i]) {
				s.keys[i] = append(s.keys[i], make([][]interface{}, 1-len(s.keys[i]))...)
			}

			s.keys[i][0] = append(s.keys[i][0], val1)

			_values[nv] = unsafeByteArray(val1)

			_def[n], _rep[n], _lastRep = 1, _lastRep, 1

			nv++
			n++
		}
		_lastRep--
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col18_mapValue_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col18
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			num += len(val1)
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col18

		var loc0 []interface{}
		if len(s.keys[i]) > 0 {
			loc0 = s.keys[i][0]
		}

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			val1, loc0 = val0[loc0[0].(string)], loc0[1:]
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for val2 := range val1 {
				if 1 >= len(s.keys[i]) {
					s.keys[i] = append(s.keys[i], make([][]interface{}, 2-len(s.keys[i]))...)
				}

				s.keys[i][1] = append(s.keys[i][1], val2)

				_values[nv] = unsafeByteArray(val2)

				_def[n], _rep[n], _lastRep = 2, _lastRep, 2

				nv++
				n++
			}
			_lastRep--
		}
		_lastRep--
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col18_mapValue_mapValue_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col18
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							num++
							continue
						}

						num += len(val4)
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

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

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			val1, loc0 = val0[loc0[0].(string)], loc0[1:]
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				val2, loc1 = val1[loc1[0].(string)], loc1[1:]
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							_def[n], _rep[n], _lastRep = 4, _lastRep, 5
							n++
						}

						for val5 := range val4 {
							if 4 >= len(s.keys[i]) {
								s.keys[i] = append(s.keys[i], make([][]interface{}, 5-len(s.keys[i]))...)
							}

							s.keys[i][4] = append(s.keys[i][4], val5)

							_values[nv] = unsafeByteArray(val5)

							_def[n], _rep[n], _lastRep = 5, _lastRep, 5

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

func (s *RecursiveMapWriter) _Col18_mapValue_mapValue_mapValue_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col18
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							num++
							continue
						}

						for _, val5 := range val4 {
							if len(val5) == 0 {
								num++
								continue
							}

							num += len(val5)
						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

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

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			val1, loc0 = val0[loc0[0].(string)], loc0[1:]
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				val2, loc1 = val1[loc1[0].(string)], loc1[1:]
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							_def[n], _rep[n], _lastRep = 4, _lastRep, 5
							n++
						}

						for _, val5 := range val4 {
							val5, loc4 = val4[loc4[0].(string)], loc4[1:]
							if len(val5) == 0 {
								_def[n], _rep[n], _lastRep = 5, _lastRep, 6
								n++
							}

							for val6 := range val5 {
								if 5 >= len(s.keys[i]) {
									s.keys[i] = append(s.keys[i], make([][]interface{}, 6-len(s.keys[i]))...)
								}

								s.keys[i][5] = append(s.keys[i][5], val6)

								_values[nv] = unsafeByteArray(val6)

								_def[n], _rep[n], _lastRep = 6, _lastRep, 6

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

func (s *RecursiveMapWriter) _Col18_mapValue_mapValue_mapValue_mapValue(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col18
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							num++
							continue
						}

						for _, val5 := range val4 {
							if len(val5) == 0 {
								num++
								continue
							}

							for _, val6 := range val5 {
								if len(val6) == 0 {
									num++
									continue
								}

								for _, val7 := range val6 {
									if len(val7) == 0 {
										num++
										continue
									}

									num += len(val7)
								}
							}
						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

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
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			val1, loc0 = val0[loc0[0].(string)], loc0[1:]
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				val2, loc1 = val1[loc1[0].(string)], loc1[1:]
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							_def[n], _rep[n], _lastRep = 4, _lastRep, 5
							n++
						}

						for _, val5 := range val4 {
							val5, loc4 = val4[loc4[0].(string)], loc4[1:]
							if len(val5) == 0 {
								_def[n], _rep[n], _lastRep = 5, _lastRep, 6
								n++
							}

							for _, val6 := range val5 {
								val6, loc5 = val5[loc5[0].(string)], loc5[1:]
								if len(val6) == 0 {
									_def[n], _rep[n], _lastRep = 6, _lastRep, 7
									n++
								}

								for _, val7 := range val6 {
									if len(val7) == 0 {
										_def[n], _rep[n], _lastRep = 7, _lastRep, 8
										n++
									}

									for _, val8 := range val7 {

										_values[nv] = unsafeByteArray(val8)

										_def[n], _rep[n], _lastRep = 8, _lastRep, 8

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
		s.keys[i] = s.keys[i][len(s.keys[i]):]
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col19_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col19
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			num += len(val1)
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col19

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for val2 := range val1 {
				if 1 >= len(s.keys[i]) {
					s.keys[i] = append(s.keys[i], make([][]interface{}, 2-len(s.keys[i]))...)
				}

				s.keys[i][1] = append(s.keys[i][1], val2)

				_values[nv] = unsafeByteArray(val2)

				_def[n], _rep[n], _lastRep = 2, _lastRep, 2

				nv++
				n++
			}
			_lastRep--
		}
		_lastRep--
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col19_mapValue_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col19
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				num += len(val2)
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col19

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				val2, loc1 = val1[loc1[0].(string)], loc1[1:]
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for val3 := range val2 {
					if 2 >= len(s.keys[i]) {
						s.keys[i] = append(s.keys[i], make([][]interface{}, 3-len(s.keys[i]))...)
					}

					s.keys[i][2] = append(s.keys[i][2], val3)

					_values[nv] = unsafeByteArray(val3)

					_def[n], _rep[n], _lastRep = 3, _lastRep, 3

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

func (s *RecursiveMapWriter) _Col19_mapValue_mapValue_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col19
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							num++
							continue
						}

						num += len(val4)
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

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

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				val2, loc1 = val1[loc1[0].(string)], loc1[1:]
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					val3, loc2 = val2[loc2[0].(string)], loc2[1:]
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							_def[n], _rep[n], _lastRep = 4, _lastRep, 5
							n++
						}

						for val5 := range val4 {
							if 4 >= len(s.keys[i]) {
								s.keys[i] = append(s.keys[i], make([][]interface{}, 5-len(s.keys[i]))...)
							}

							s.keys[i][4] = append(s.keys[i][4], val5)

							_values[nv] = unsafeByteArray(val5)

							_def[n], _rep[n], _lastRep = 5, _lastRep, 5

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

func (s *RecursiveMapWriter) _Col19_mapValue_mapValue_mapValue_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col19
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							num++
							continue
						}

						for _, val5 := range val4 {
							if len(val5) == 0 {
								num++
								continue
							}

							num += len(val5)
						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

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

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				val2, loc1 = val1[loc1[0].(string)], loc1[1:]
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					val3, loc2 = val2[loc2[0].(string)], loc2[1:]
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							_def[n], _rep[n], _lastRep = 4, _lastRep, 5
							n++
						}

						for _, val5 := range val4 {
							val5, loc4 = val4[loc4[0].(string)], loc4[1:]
							if len(val5) == 0 {
								_def[n], _rep[n], _lastRep = 5, _lastRep, 6
								n++
							}

							for val6 := range val5 {
								if 5 >= len(s.keys[i]) {
									s.keys[i] = append(s.keys[i], make([][]interface{}, 6-len(s.keys[i]))...)
								}

								s.keys[i][5] = append(s.keys[i][5], val6)

								_values[nv] = unsafeByteArray(val6)

								_def[n], _rep[n], _lastRep = 6, _lastRep, 6

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

func (s *RecursiveMapWriter) _Col19_mapValue_mapValue_mapValue_mapValue(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col19
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							num++
							continue
						}

						for _, val5 := range val4 {
							if len(val5) == 0 {
								num++
								continue
							}

							num += len(val5)
						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

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
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				val2, loc1 = val1[loc1[0].(string)], loc1[1:]
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					val3, loc2 = val2[loc2[0].(string)], loc2[1:]
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							_def[n], _rep[n], _lastRep = 4, _lastRep, 5
							n++
						}

						for _, val5 := range val4 {
							val5, loc4 = val4[loc4[0].(string)], loc4[1:]
							if len(val5) == 0 {
								_def[n], _rep[n], _lastRep = 5, _lastRep, 6
								n++
							}

							for _, val6 := range val5 {
								val6, loc5 = val5[loc5[0].(string)], loc5[1:]

								_values[nv] = unsafeByteArray(val6)

								_def[n], _rep[n], _lastRep = 6, _lastRep, 6

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
		s.keys[i] = s.keys[i][len(s.keys[i]):]
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col20_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col20
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			num += len(val1)
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col20

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for val2 := range val1 {
				if 1 >= len(s.keys[i]) {
					s.keys[i] = append(s.keys[i], make([][]interface{}, 2-len(s.keys[i]))...)
				}

				s.keys[i][1] = append(s.keys[i][1], val2)

				_values[nv] = unsafeByteArray(val2)

				_def[n], _rep[n], _lastRep = 2, _lastRep, 2

				nv++
				n++
			}
			_lastRep--
		}
		_lastRep--
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col20_mapValue_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col20
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				num += len(val2)
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col20

		var loc1 []interface{}
		if len(s.keys[i]) > 1 {
			loc1 = s.keys[i][1]
		}

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				val2, loc1 = val1[loc1[0].(string)], loc1[1:]
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for val3 := range val2 {
					if 2 >= len(s.keys[i]) {
						s.keys[i] = append(s.keys[i], make([][]interface{}, 3-len(s.keys[i]))...)
					}

					s.keys[i][2] = append(s.keys[i][2], val3)

					_values[nv] = unsafeByteArray(val3)

					_def[n], _rep[n], _lastRep = 3, _lastRep, 3

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

func (s *RecursiveMapWriter) _Col20_mapValue_mapValue_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col20
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							num++
							continue
						}

						num += len(val4)
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

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

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				val2, loc1 = val1[loc1[0].(string)], loc1[1:]
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					val3, loc2 = val2[loc2[0].(string)], loc2[1:]
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							_def[n], _rep[n], _lastRep = 4, _lastRep, 5
							n++
						}

						for val5 := range val4 {
							if 4 >= len(s.keys[i]) {
								s.keys[i] = append(s.keys[i], make([][]interface{}, 5-len(s.keys[i]))...)
							}

							s.keys[i][4] = append(s.keys[i][4], val5)

							_values[nv] = unsafeByteArray(val5)

							_def[n], _rep[n], _lastRep = 5, _lastRep, 5

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

func (s *RecursiveMapWriter) _Col20_mapValue_mapValue_mapValue_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col20
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							num++
							continue
						}

						for _, val5 := range val4 {
							if len(val5) == 0 {
								num++
								continue
							}

							num += len(val5)
						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

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

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				val2, loc1 = val1[loc1[0].(string)], loc1[1:]
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					val3, loc2 = val2[loc2[0].(string)], loc2[1:]
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							_def[n], _rep[n], _lastRep = 4, _lastRep, 5
							n++
						}

						for _, val5 := range val4 {
							val5, loc4 = val4[loc4[0].(string)], loc4[1:]
							if len(val5) == 0 {
								_def[n], _rep[n], _lastRep = 5, _lastRep, 6
								n++
							}

							for val6 := range val5 {
								if 5 >= len(s.keys[i]) {
									s.keys[i] = append(s.keys[i], make([][]interface{}, 6-len(s.keys[i]))...)
								}

								s.keys[i][5] = append(s.keys[i][5], val6)

								_values[nv] = unsafeByteArray(val6)

								_def[n], _rep[n], _lastRep = 6, _lastRep, 6

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

func (s *RecursiveMapWriter) _Col20_mapValue_mapValue_mapValue_mapValue(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col20
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							num++
							continue
						}

						for _, val5 := range val4 {
							if len(val5) == 0 {
								num++
								continue
							}

							for _, val6 := range val5 {
								if len(val6) == 0 {
									num++
									continue
								}

								num += len(val6)
							}
						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

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
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				val2, loc1 = val1[loc1[0].(string)], loc1[1:]
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					val3, loc2 = val2[loc2[0].(string)], loc2[1:]
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							_def[n], _rep[n], _lastRep = 4, _lastRep, 5
							n++
						}

						for _, val5 := range val4 {
							val5, loc4 = val4[loc4[0].(string)], loc4[1:]
							if len(val5) == 0 {
								_def[n], _rep[n], _lastRep = 5, _lastRep, 6
								n++
							}

							for _, val6 := range val5 {
								val6, loc5 = val5[loc5[0].(string)], loc5[1:]
								if len(val6) == 0 {
									_def[n], _rep[n], _lastRep = 6, _lastRep, 7
									n++
								}

								for _, val7 := range val6 {

									_values[nv] = unsafeByteArray(val7)

									_def[n], _rep[n], _lastRep = 7, _lastRep, 7

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
		s.keys[i] = s.keys[i][len(s.keys[i]):]
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) _Col21_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col21
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				num += len(val2)
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col21

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for val3 := range val2 {
					if 2 >= len(s.keys[i]) {
						s.keys[i] = append(s.keys[i], make([][]interface{}, 3-len(s.keys[i]))...)
					}

					s.keys[i][2] = append(s.keys[i][2], val3)

					_values[nv] = unsafeByteArray(val3)

					_def[n], _rep[n], _lastRep = 3, _lastRep, 3

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

func (s *RecursiveMapWriter) _Col21_mapValue_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col21
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					num += len(val3)
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Col21

		var loc2 []interface{}
		if len(s.keys[i]) > 2 {
			loc2 = s.keys[i][2]
		}

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					val3, loc2 = val2[loc2[0].(string)], loc2[1:]
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for val4 := range val3 {
						if 3 >= len(s.keys[i]) {
							s.keys[i] = append(s.keys[i], make([][]interface{}, 4-len(s.keys[i]))...)
						}

						s.keys[i][3] = append(s.keys[i][3], val4)

						_values[nv] = unsafeByteArray(val4)

						_def[n], _rep[n], _lastRep = 4, _lastRep, 4

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

func (s *RecursiveMapWriter) _Col21_mapValue_mapValue_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col21
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							num++
							continue
						}

						for _, val5 := range val4 {
							if len(val5) == 0 {
								num++
								continue
							}

							for _, val6 := range val5 {
								if len(val6) == 0 {
									num++
									continue
								}

								num += len(val6)
							}
						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

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

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					val3, loc2 = val2[loc2[0].(string)], loc2[1:]
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for _, val4 := range val3 {
						val4, loc3 = val3[loc3[0].(string)], loc3[1:]
						if len(val4) == 0 {
							_def[n], _rep[n], _lastRep = 4, _lastRep, 5
							n++
						}

						for _, val5 := range val4 {
							if len(val5) == 0 {
								_def[n], _rep[n], _lastRep = 5, _lastRep, 6
								n++
							}

							for _, val6 := range val5 {
								if len(val6) == 0 {
									_def[n], _rep[n], _lastRep = 6, _lastRep, 7
									n++
								}

								for val7 := range val6 {
									if 6 >= len(s.keys[i]) {
										s.keys[i] = append(s.keys[i], make([][]interface{}, 7-len(s.keys[i]))...)
									}

									s.keys[i][6] = append(s.keys[i][6], val7)

									_values[nv] = unsafeByteArray(val7)

									_def[n], _rep[n], _lastRep = 7, _lastRep, 7

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

func (s *RecursiveMapWriter) _Col21_mapValue_mapValue_mapValue_mapKey(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col21
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							num++
							continue
						}

						for _, val5 := range val4 {
							if len(val5) == 0 {
								num++
								continue
							}

							for _, val6 := range val5 {
								if len(val6) == 0 {
									num++
									continue
								}

								for _, val7 := range val6 {
									if len(val7) == 0 {
										num++
										continue
									}

									num += len(val7)
								}
							}
						}
					}
				}
			}
		}
	}

	var (
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

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

		if len(val0) == 0 {
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					val3, loc2 = val2[loc2[0].(string)], loc2[1:]
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for _, val4 := range val3 {
						val4, loc3 = val3[loc3[0].(string)], loc3[1:]
						if len(val4) == 0 {
							_def[n], _rep[n], _lastRep = 4, _lastRep, 5
							n++
						}

						for _, val5 := range val4 {
							if len(val5) == 0 {
								_def[n], _rep[n], _lastRep = 5, _lastRep, 6
								n++
							}

							for _, val6 := range val5 {
								if len(val6) == 0 {
									_def[n], _rep[n], _lastRep = 6, _lastRep, 7
									n++
								}

								for _, val7 := range val6 {
									val7, loc6 = val6[loc6[0].(string)], loc6[1:]
									if len(val7) == 0 {
										_def[n], _rep[n], _lastRep = 7, _lastRep, 8
										n++
									}

									for val8 := range val7 {
										if 7 >= len(s.keys[i]) {
											s.keys[i] = append(s.keys[i], make([][]interface{}, 8-len(s.keys[i]))...)
										}

										s.keys[i][7] = append(s.keys[i][7], val8)

										_values[nv] = unsafeByteArray(val8)

										_def[n], _rep[n], _lastRep = 8, _lastRep, 8

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

func (s *RecursiveMapWriter) _Col21_mapValue_mapValue_mapValue_mapValue(data []RecursiveMap) values {
	var num int
	for i := range data {
		var val0 = data[i].Col21
		if len(val0) == 0 {
			num++
			continue
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				num++
				continue
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					num++
					continue
				}

				for _, val3 := range val2 {
					if len(val3) == 0 {
						num++
						continue
					}

					for _, val4 := range val3 {
						if len(val4) == 0 {
							num++
							continue
						}

						for _, val5 := range val4 {
							if len(val5) == 0 {
								num++
								continue
							}

							for _, val6 := range val5 {
								if len(val6) == 0 {
									num++
									continue
								}

								for _, val7 := range val6 {
									if len(val7) == 0 {
										num++
										continue
									}

									for _, val8 := range val7 {
										if len(val8) == 0 {
											num++
											continue
										}

										for _, val9 := range val8 {
											if len(val9) == 0 {
												num++
												continue
											}

											num += len(val9)
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
		_values = make([]parquet.ByteArray, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

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
			_def[n], _rep[n], _lastRep = 0, _lastRep, 1
			n++
		}

		for _, val1 := range val0 {
			if len(val1) == 0 {
				_def[n], _rep[n], _lastRep = 1, _lastRep, 2
				n++
			}

			for _, val2 := range val1 {
				if len(val2) == 0 {
					_def[n], _rep[n], _lastRep = 2, _lastRep, 3
					n++
				}

				for _, val3 := range val2 {
					val3, loc2 = val2[loc2[0].(string)], loc2[1:]
					if len(val3) == 0 {
						_def[n], _rep[n], _lastRep = 3, _lastRep, 4
						n++
					}

					for _, val4 := range val3 {
						val4, loc3 = val3[loc3[0].(string)], loc3[1:]
						if len(val4) == 0 {
							_def[n], _rep[n], _lastRep = 4, _lastRep, 5
							n++
						}

						for _, val5 := range val4 {
							if len(val5) == 0 {
								_def[n], _rep[n], _lastRep = 5, _lastRep, 6
								n++
							}

							for _, val6 := range val5 {
								if len(val6) == 0 {
									_def[n], _rep[n], _lastRep = 6, _lastRep, 7
									n++
								}

								for _, val7 := range val6 {
									val7, loc6 = val6[loc6[0].(string)], loc6[1:]
									if len(val7) == 0 {
										_def[n], _rep[n], _lastRep = 7, _lastRep, 8
										n++
									}

									for _, val8 := range val7 {
										val8, loc7 = val7[loc7[0].(string)], loc7[1:]
										if len(val8) == 0 {
											_def[n], _rep[n], _lastRep = 8, _lastRep, 9
											n++
										}

										for _, val9 := range val8 {
											if len(val9) == 0 {
												_def[n], _rep[n], _lastRep = 9, _lastRep, 10
												n++
											}

											for _, val10 := range val9 {

												_values[nv] = unsafeByteArray(val10)

												_def[n], _rep[n], _lastRep = 10, _lastRep, 10

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
		s.keys[i] = s.keys[i][len(s.keys[i]):]
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *RecursiveMapWriter) values(data []RecursiveMap) []values {
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

func (s *RecursiveMapWriter) Write(out io.Writer, values []RecursiveMap) {
	var writer = file.NewParquetWriter(out, s.schema.Root(), file.WithWriterProps(
		parquet.NewWriterProperties(s.properties...),
	))
	defer writer.Close()

	var rgw = writer.AppendRowGroup()
	defer rgw.Close()

	s.keys = make([][][]interface{}, len(values))

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
