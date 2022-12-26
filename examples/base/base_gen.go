package base

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

type BaseWriter struct {
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

func NewBaseWriter(opts ...WriterProperty) *BaseWriter {
	_schema, err := schema.NewSchemaFromStruct(Base{})
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

	return &BaseWriter{schema: schema.NewSchema(root), properties: append(properties, opts...)}
}

func unsafeByteArray(s string) parquet.ByteArray {
	if len(s) == 0 {
		return nil
	}

	return (*[0x7fff0000]byte)(unsafe.Pointer(
		(*reflect.StringHeader)(unsafe.Pointer(&s)).Data),
	)[:len(s):len(s)]
}

func (s *BaseWriter) _ID(data []Base) values {
	var _values = make([]int64, len(data))

	for i := range data {
		_values[i] = data[i].ID
	}

	return values{values: _values}
}

func (s *BaseWriter) _OrgName(data []Base) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].OrgName)
	}

	return values{values: _values}
}

func (s *BaseWriter) _TraceID(data []Base) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].TraceID)
	}

	return values{values: _values}
}

func (s *BaseWriter) _ServiceID(data []Base) values {
	var _values = make([]int64, len(data))

	for i := range data {
		_values[i] = data[i].ServiceID
	}

	return values{values: _values}
}

func (s *BaseWriter) _ServiceName(data []Base) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].ServiceName)
	}

	return values{values: _values}
}

func (s *BaseWriter) _ServiceLabel(data []Base) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].ServiceLabel)
	}

	return values{values: _values}
}

func (s *BaseWriter) _ObservedAt(data []Base) values {
	var _values = make([]int64, len(data))

	for i := range data {
		_values[i] = data[i].ObservedAt
	}

	return values{values: _values}
}

func (s *BaseWriter) _ClientAddr(data []Base) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].ClientAddr)
	}

	return values{values: _values}
}

func (s *BaseWriter) _ClientVersion(data []Base) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].ClientVersion)
	}

	return values{values: _values}
}

func (s *BaseWriter) _UserID(data []Base) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].UserID)
	}

	return values{values: _values}
}

func (s *BaseWriter) _Profile(data []Base) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].Profile)
	}

	return values{values: _values}
}

func (s *BaseWriter) _Roles(data []Base) values {
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

func (s *BaseWriter) _OperationType(data []Base) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].OperationType)
	}

	return values{values: _values}
}

func (s *BaseWriter) _OperationName(data []Base) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].OperationName)
	}

	return values{values: _values}
}

func (s *BaseWriter) _QueryHash(data []Base) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].QueryHash)
	}

	return values{values: _values}
}

func (s *BaseWriter) _QueryInput(data []Base) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].QueryInput)
	}

	return values{values: _values}
}

func (s *BaseWriter) _QueryOutput(data []Base) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].QueryOutput)
	}

	return values{values: _values}
}

func (s *BaseWriter) _Depth(data []Base) values {
	var _values = make([]int32, len(data))

	for i := range data {
		_values[i] = data[i].Depth
	}

	return values{values: _values}
}

func (s *BaseWriter) _Height(data []Base) values {
	var _values = make([]int32, len(data))

	for i := range data {
		_values[i] = data[i].Height
	}

	return values{values: _values}
}

func (s *BaseWriter) _Directives(data []Base) values {
	var _values = make([]int32, len(data))

	for i := range data {
		_values[i] = data[i].Directives
	}

	return values{values: _values}
}

func (s *BaseWriter) _RequestSize(data []Base) values {
	var _values = make([]int32, len(data))

	for i := range data {
		_values[i] = data[i].RequestSize
	}

	return values{values: _values}
}

func (s *BaseWriter) _ResponseSize(data []Base) values {
	var _values = make([]int32, len(data))

	for i := range data {
		_values[i] = data[i].ResponseSize
	}

	return values{values: _values}
}

func (s *BaseWriter) _CountTotal(data []Base) values {
	var _values = make([]int32, len(data))

	for i := range data {
		_values[i] = data[i].CountTotal
	}

	return values{values: _values}
}

func (s *BaseWriter) _CreditLeft(data []Base) values {
	var _values = make([]int32, len(data))

	for i := range data {
		_values[i] = data[i].CreditLeft
	}

	return values{values: _values}
}

func (s *BaseWriter) _CreditLeftPerMinute(data []Base) values {
	var _values = make([]int32, len(data))

	for i := range data {
		_values[i] = data[i].CreditLeftPerMinute
	}

	return values{values: _values}
}

func (s *BaseWriter) _CreditLeftPerHour(data []Base) values {
	var _values = make([]int32, len(data))

	for i := range data {
		_values[i] = data[i].CreditLeftPerHour
	}

	return values{values: _values}
}

func (s *BaseWriter) _CallsLeftPerMinute(data []Base) values {
	var _values = make([]int32, len(data))

	for i := range data {
		_values[i] = data[i].CallsLeftPerMinute
	}

	return values{values: _values}
}

func (s *BaseWriter) _CallsLeftPerHour(data []Base) values {
	var _values = make([]int32, len(data))

	for i := range data {
		_values[i] = data[i].CallsLeftPerHour
	}

	return values{values: _values}
}

func (s *BaseWriter) _Duration(data []Base) values {
	var _values = make([]float64, len(data))

	for i := range data {
		_values[i] = data[i].Duration
	}

	return values{values: _values}
}

func (s *BaseWriter) _SidecarProcessTime(data []Base) values {
	var _values = make([]float64, len(data))

	for i := range data {
		_values[i] = data[i].SidecarProcessTime
	}

	return values{values: _values}
}

func (s *BaseWriter) _ServerProcessTime(data []Base) values {
	var _values = make([]float64, len(data))

	for i := range data {
		_values[i] = data[i].ServerProcessTime
	}

	return values{values: _values}
}

func (s *BaseWriter) _Status(data []Base) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].Status)
	}

	return values{values: _values}
}

func (s *BaseWriter) _InternalReason(data []Base) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].InternalReason)
	}

	return values{values: _values}
}

func (s *BaseWriter) _HasErrors(data []Base) values {
	var _values = make([]bool, len(data))

	for i := range data {
		_values[i] = data[i].HasErrors
	}

	return values{values: _values}
}

func (s *BaseWriter) _InternalError(data []Base) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].InternalError)
	}

	return values{values: _values}
}

func (s *BaseWriter) _InternalStack(data []Base) values {
	var _values = make([]parquet.ByteArray, len(data))

	for i := range data {
		_values[i] = unsafeByteArray(data[i].InternalStack)
	}

	return values{values: _values}
}

func (s *BaseWriter) values(data []Base) []values {
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
		s._HasErrors(data),
		s._InternalError(data),
		s._InternalStack(data),
	}
}

func (s *BaseWriter) Write(out io.Writer, values []Base) {
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
