package querydata

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

type QueryDataWriter struct {
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

func NewQueryDataWriter(opts ...WriterProperty) *QueryDataWriter {
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

	var properties = []WriterProperty{
		parquet.WithVersion(parquet.V1_0),
		parquet.WithRootRepetition(parquet.Repetition(0)),
		parquet.WithDictionaryDefault(false),
	}

	return &QueryDataWriter{schema: schema.NewSchema(root), properties: append(properties, opts...)}
}

func unsafeByteArray(s string) parquet.ByteArray {
	if len(s) == 0 {
		return nil
	}

	return (*[0x7fff0000]byte)(unsafe.Pointer(
		(*reflect.StringHeader)(unsafe.Pointer(&s)).Data),
	)[:len(s):len(s)]
}

func (s *QueryDataWriter) _ID(data []QueryData) values {
	var _values = make([]int64, len(data))

	for i := range data {
		_values[i] = data[i].ID
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _OrgName(data []QueryData) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].OrgName)
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _TraceID(data []QueryData) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].TraceID)
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _ServiceID(data []QueryData) values {
	var _values = make([]int64, len(data))

	for i := range data {
		_values[i] = data[i].ServiceID
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _ServiceName(data []QueryData) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].ServiceName)
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _ServiceLabel(data []QueryData) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].ServiceLabel)
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _ObservedAt(data []QueryData) values {
	var _values = make([]int64, len(data))

	for i := range data {
		_values[i] = data[i].ObservedAt
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _ClientAddr(data []QueryData) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].ClientAddr)
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _ClientVersion(data []QueryData) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].ClientVersion)
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _UserID(data []QueryData) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].UserID)
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _Profile(data []QueryData) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].Profile)
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _Roles(data []QueryData) values {
	var num int
	for i := range data {
		var val0 = data[i].Roles
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
		var val0 = data[i].Roles

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

func (s *QueryDataWriter) _OperationType(data []QueryData) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].OperationType)
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _OperationName(data []QueryData) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].OperationName)
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _QueryHash(data []QueryData) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].QueryHash)
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _QueryInput(data []QueryData) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].QueryInput)
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _QueryOutput(data []QueryData) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].QueryOutput)
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _Depth(data []QueryData) values {
	var _values = make([]int32, len(data))

	for i := range data {
		_values[i] = data[i].Depth
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _Height(data []QueryData) values {
	var _values = make([]int32, len(data))

	for i := range data {
		_values[i] = data[i].Height
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _Directives(data []QueryData) values {
	var _values = make([]int32, len(data))

	for i := range data {
		_values[i] = data[i].Directives
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _RequestSize(data []QueryData) values {
	var _values = make([]int32, len(data))

	for i := range data {
		_values[i] = data[i].RequestSize
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _ResponseSize(data []QueryData) values {
	var _values = make([]int32, len(data))

	for i := range data {
		_values[i] = data[i].ResponseSize
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _CountTotal(data []QueryData) values {
	var _values = make([]int32, len(data))

	for i := range data {
		_values[i] = data[i].CountTotal
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _CreditLeft(data []QueryData) values {
	var _values = make([]int32, len(data))

	for i := range data {
		_values[i] = data[i].CreditLeft
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _CreditLeftPerMinute(data []QueryData) values {
	var _values = make([]int32, len(data))

	for i := range data {
		_values[i] = data[i].CreditLeftPerMinute
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _CreditLeftPerHour(data []QueryData) values {
	var _values = make([]int32, len(data))

	for i := range data {
		_values[i] = data[i].CreditLeftPerHour
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _CallsLeftPerMinute(data []QueryData) values {
	var _values = make([]int32, len(data))

	for i := range data {
		_values[i] = data[i].CallsLeftPerMinute
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _CallsLeftPerHour(data []QueryData) values {
	var _values = make([]int32, len(data))

	for i := range data {
		_values[i] = data[i].CallsLeftPerHour
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _Duration(data []QueryData) values {
	var _values = make([]float64, len(data))

	for i := range data {
		_values[i] = data[i].Duration
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _SidecarProcessTime(data []QueryData) values {
	var _values = make([]float64, len(data))

	for i := range data {
		_values[i] = data[i].SidecarProcessTime
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _ServerProcessTime(data []QueryData) values {
	var _values = make([]float64, len(data))

	for i := range data {
		_values[i] = data[i].ServerProcessTime
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _Status(data []QueryData) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].Status)
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _InternalReason(data []QueryData) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].InternalReason)
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _TypeCounts_mapKey(data []QueryData) values {
	var num int
	for i := range data {
		var val0 = data[i].TypeCounts
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
		var val0 = data[i].TypeCounts

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

func (s *QueryDataWriter) _TypeCounts_mapValue(data []QueryData) values {
	var num int
	for i := range data {
		var val0 = data[i].TypeCounts
		if len(val0) == 0 {
			num++
			continue
		}

		num += len(val0)
	}

	var (
		_values = make([]int32, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].TypeCounts

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

			_values[nv] = val1

			_def[n], _rep[n], _lastRep = 1, _lastRep, 1

			nv++
			n++
		}
		_lastRep--
		s.keys[i] = s.keys[i][len(s.keys[i]):]
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _FieldCounts_mapKey(data []QueryData) values {
	var num int
	for i := range data {
		var val0 = data[i].FieldCounts
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
		var val0 = data[i].FieldCounts

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

func (s *QueryDataWriter) _FieldCounts_mapValue(data []QueryData) values {
	var num int
	for i := range data {
		var val0 = data[i].FieldCounts
		if len(val0) == 0 {
			num++
			continue
		}

		num += len(val0)
	}

	var (
		_values = make([]int32, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].FieldCounts

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

			_values[nv] = val1

			_def[n], _rep[n], _lastRep = 1, _lastRep, 1

			nv++
			n++
		}
		_lastRep--
		s.keys[i] = s.keys[i][len(s.keys[i]):]
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _PathCounts_mapKey(data []QueryData) values {
	var num int
	for i := range data {
		var val0 = data[i].PathCounts
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
		var val0 = data[i].PathCounts

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

func (s *QueryDataWriter) _PathCounts_mapValue(data []QueryData) values {
	var num int
	for i := range data {
		var val0 = data[i].PathCounts
		if len(val0) == 0 {
			num++
			continue
		}

		num += len(val0)
	}

	var (
		_values = make([]int32, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].PathCounts

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

			_values[nv] = val1

			_def[n], _rep[n], _lastRep = 1, _lastRep, 1

			nv++
			n++
		}
		_lastRep--
		s.keys[i] = s.keys[i][len(s.keys[i]):]
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _FieldTime_mapKey(data []QueryData) values {
	var num int
	for i := range data {
		var val0 = data[i].FieldTime
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
		var val0 = data[i].FieldTime

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

func (s *QueryDataWriter) _FieldTime_mapValue(data []QueryData) values {
	var num int
	for i := range data {
		var val0 = data[i].FieldTime
		if len(val0) == 0 {
			num++
			continue
		}

		num += len(val0)
	}

	var (
		_values = make([]int64, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].FieldTime

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

			_values[nv] = val1

			_def[n], _rep[n], _lastRep = 1, _lastRep, 1

			nv++
			n++
		}
		_lastRep--
		s.keys[i] = s.keys[i][len(s.keys[i]):]
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _PathTime_mapKey(data []QueryData) values {
	var num int
	for i := range data {
		var val0 = data[i].PathTime
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
		var val0 = data[i].PathTime

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

func (s *QueryDataWriter) _PathTime_mapValue(data []QueryData) values {
	var num int
	for i := range data {
		var val0 = data[i].PathTime
		if len(val0) == 0 {
			num++
			continue
		}

		num += len(val0)
	}

	var (
		_values = make([]int64, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].PathTime

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

			_values[nv] = val1

			_def[n], _rep[n], _lastRep = 1, _lastRep, 1

			nv++
			n++
		}
		_lastRep--
		s.keys[i] = s.keys[i][len(s.keys[i]):]
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _HasErrors(data []QueryData) values {
	var _values = make([]bool, len(data))

	for i := range data {
		_values[i] = data[i].HasErrors
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _Errors_mapKey(data []QueryData) values {
	var num int
	for i := range data {
		var val0 = data[i].Errors
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
		var val0 = data[i].Errors

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

func (s *QueryDataWriter) _Errors_mapValue(data []QueryData) values {
	var num int
	for i := range data {
		var val0 = data[i].Errors
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
		var val0 = data[i].Errors

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

func (s *QueryDataWriter) _ErrorReasonsMap_mapKey(data []QueryData) values {
	var num int
	for i := range data {
		var val0 = data[i].ErrorReasonsMap
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
		var val0 = data[i].ErrorReasonsMap

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

func (s *QueryDataWriter) _ErrorReasonsMap_mapValue(data []QueryData) values {
	var num int
	for i := range data {
		var val0 = data[i].ErrorReasonsMap
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
		_values = make([]int32, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].ErrorReasonsMap

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

				_values[nv] = val2

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

func (s *QueryDataWriter) _InternalError(data []QueryData) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].InternalError)
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _InternalStack(data []QueryData) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].InternalStack)
	}

	return values{values: _values}
}

func (s *QueryDataWriter) _Stopwatch_mapKey(data []QueryData) values {
	var num int
	for i := range data {
		var val0 = data[i].Stopwatch
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
		var val0 = data[i].Stopwatch

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

func (s *QueryDataWriter) _Stopwatch_mapValue(data []QueryData) values {
	var num int
	for i := range data {
		var val0 = data[i].Stopwatch
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
		_values = make([]int64, num)
		_def    = make([]int16, num)
		_rep    = make([]int16, num)
	)

	var n, nv int

	for i := range data {
		var _lastRep int16
		var val0 = data[i].Stopwatch

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

				_values[nv] = val2

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

func (s *QueryDataWriter) values(data []QueryData) []values {
	return []values{
		s._ID(data),
		s._OrgName(data),
		s._TraceID(data),
		s._ServiceID(data),
		s._ServiceName(data),
		s._ServiceLabel(data),
		s._ObservedAt(data),
		s._ClientAddr(data),
		s._ClientVersion(data),
		s._UserID(data),
		s._Profile(data),
		s._Roles(data),
		s._OperationType(data),
		s._OperationName(data),
		s._QueryHash(data),
		s._QueryInput(data),
		s._QueryOutput(data),
		s._Depth(data),
		s._Height(data),
		s._Directives(data),
		s._RequestSize(data),
		s._ResponseSize(data),
		s._CountTotal(data),
		s._CreditLeft(data),
		s._CreditLeftPerMinute(data),
		s._CreditLeftPerHour(data),
		s._CallsLeftPerMinute(data),
		s._CallsLeftPerHour(data),
		s._Duration(data),
		s._SidecarProcessTime(data),
		s._ServerProcessTime(data),
		s._Status(data),
		s._InternalReason(data),
		s._TypeCounts_mapKey(data),
		s._TypeCounts_mapValue(data),
		s._FieldCounts_mapKey(data),
		s._FieldCounts_mapValue(data),
		s._PathCounts_mapKey(data),
		s._PathCounts_mapValue(data),
		s._FieldTime_mapKey(data),
		s._FieldTime_mapValue(data),
		s._PathTime_mapKey(data),
		s._PathTime_mapValue(data),
		s._HasErrors(data),
		s._Errors_mapKey(data),
		s._Errors_mapValue(data),
		s._ErrorReasonsMap_mapKey(data),
		s._ErrorReasonsMap_mapValue(data),
		s._InternalError(data),
		s._InternalStack(data),
		s._Stopwatch_mapKey(data),
		s._Stopwatch_mapValue(data),
	}
}

func (s *QueryDataWriter) Write(out io.Writer, values []QueryData) {
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
