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
}

func NewQueryDataWriter() *QueryDataWriter {
	_schema, err := schema.NewSchemaFromStruct(QueryData{})
	if err != nil {
		panic(err)
	}

	return &QueryDataWriter{schema: _schema}
}


func (s *QueryDataWriter) _ID(data []QueryData) values {
	var _values = make([]int64, len(data))
	for i := range data {
		_values[i] = int64(data[i].ID)
	}

	return values{values: _values}
}


func (s *QueryDataWriter) _CallsLeftPerMinute(data []QueryData) values {
	var _values = make([]int32, len(data))
	for i := range data {
		_values[i] = int32(data[i].CallsLeftPerMinute)
	}

	return values{values: _values}
}


func (s *QueryDataWriter) _CallsLeftPerHour(data []QueryData) values {
	var _values = make([]int32, len(data))
	for i := range data {
		_values[i] = int32(data[i].CallsLeftPerHour)
	}

	return values{values: _values}
}


func (s *QueryDataWriter) _Duration(data []QueryData) values {
	var _values = make([]float64, len(data))
	for i := range data {
		_values[i] = float64(data[i].Duration)
	}

	return values{values: _values}
}


func (s *QueryDataWriter) _SidecarProcessTime(data []QueryData) values {
	var _values = make([]float64, len(data))
	for i := range data {
		_values[i] = float64(data[i].SidecarProcessTime)
	}

	return values{values: _values}
}


func (s *QueryDataWriter) _ServerProcessTime(data []QueryData) values {
	var _values = make([]float64, len(data))
	for i := range data {
		_values[i] = float64(data[i].ServerProcessTime)
	}

	return values{values: _values}
}


func (s *QueryDataWriter) _Status(data []QueryData) values {
	var _values = make([]parquet.ByteArray, len(data))
	for i := range data {
		_values[i] = parquet.ByteArray(data[i].Status)
	}

	return values{values: _values}
}



func (s *QueryDataWriter) _TypeCounts_key(data []QueryData) values {
	var n int
	for i := range data {
		for range data[i].TypeCounts {
			n++
		}

        if len(data[i].TypeCounts) == 0 {
            n++
        }
	}

	var _values = make([]parquet.ByteArray, n)
	var _def = make([]int16, n)
	var _rep = make([]int16, n)

	n = 0
	var nv int

	for i := range data {
		var first = true
		for key := range data[i].TypeCounts {
			_values[nv] = parquet.ByteArray(key)
            nv++
			_def[n] = 1
			_rep[n] = 1

			if first {
				first = false
				_rep[n] = 0
			}

			n++
		}

        if len(data[i].TypeCounts) == 0 {
            n++
        }
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}




func (s *QueryDataWriter) _TypeCounts_value(data []QueryData) values {
	var n int
	for i := range data {
		for range data[i].TypeCounts {
			n++
		}

        if len(data[i].TypeCounts) == 0 {
            n++
        }
	}

	var _values = make([]int32, n)
	var _def = make([]int16, n)
	var _rep = make([]int16, n)

	n = 0
	var nv int

	for i := range data {
		var first = true
		for _, val := range data[i].TypeCounts {
			_values[nv] = int32(val)
			nv++
			_def[n] = 1
			_rep[n] = 1

			if first {
				first = false
				_rep[n] = 0
			}

			n++
		}

        if len(data[i].TypeCounts) == 0 {
            n++
        }
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}




func (s *QueryDataWriter) _FieldCounts_key(data []QueryData) values {
	var n int
	for i := range data {
		for range data[i].FieldCounts {
			n++
		}

        if len(data[i].FieldCounts) == 0 {
            n++
        }
	}

	var _values = make([]parquet.ByteArray, n)
	var _def = make([]int16, n)
	var _rep = make([]int16, n)

	n = 0
	var nv int

	for i := range data {
		var first = true
		for key := range data[i].FieldCounts {
			_values[nv] = parquet.ByteArray(key)
            nv++
			_def[n] = 1
			_rep[n] = 1

			if first {
				first = false
				_rep[n] = 0
			}

			n++
		}

        if len(data[i].FieldCounts) == 0 {
            n++
        }
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}




func (s *QueryDataWriter) _FieldCounts_value(data []QueryData) values {
	var n int
	for i := range data {
		for range data[i].FieldCounts {
			n++
		}

        if len(data[i].FieldCounts) == 0 {
            n++
        }
	}

	var _values = make([]int32, n)
	var _def = make([]int16, n)
	var _rep = make([]int16, n)

	n = 0
	var nv int

	for i := range data {
		var first = true
		for _, val := range data[i].FieldCounts {
			_values[nv] = int32(val)
			nv++
			_def[n] = 1
			_rep[n] = 1

			if first {
				first = false
				_rep[n] = 0
			}

			n++
		}

        if len(data[i].FieldCounts) == 0 {
            n++
        }
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}




func (s *QueryDataWriter) _PathCounts_key(data []QueryData) values {
	var n int
	for i := range data {
		for range data[i].PathCounts {
			n++
		}

        if len(data[i].PathCounts) == 0 {
            n++
        }
	}

	var _values = make([]parquet.ByteArray, n)
	var _def = make([]int16, n)
	var _rep = make([]int16, n)

	n = 0
	var nv int

	for i := range data {
		var first = true
		for key := range data[i].PathCounts {
			_values[nv] = parquet.ByteArray(key)
            nv++
			_def[n] = 1
			_rep[n] = 1

			if first {
				first = false
				_rep[n] = 0
			}

			n++
		}

        if len(data[i].PathCounts) == 0 {
            n++
        }
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}




func (s *QueryDataWriter) _PathCounts_value(data []QueryData) values {
	var n int
	for i := range data {
		for range data[i].PathCounts {
			n++
		}

        if len(data[i].PathCounts) == 0 {
            n++
        }
	}

	var _values = make([]int32, n)
	var _def = make([]int16, n)
	var _rep = make([]int16, n)

	n = 0
	var nv int

	for i := range data {
		var first = true
		for _, val := range data[i].PathCounts {
			_values[nv] = int32(val)
			nv++
			_def[n] = 1
			_rep[n] = 1

			if first {
				first = false
				_rep[n] = 0
			}

			n++
		}

        if len(data[i].PathCounts) == 0 {
            n++
        }
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}




func (s *QueryDataWriter) _FieldTime_key(data []QueryData) values {
	var n int
	for i := range data {
		for range data[i].FieldTime {
			n++
		}

        if len(data[i].FieldTime) == 0 {
            n++
        }
	}

	var _values = make([]parquet.ByteArray, n)
	var _def = make([]int16, n)
	var _rep = make([]int16, n)

	n = 0
	var nv int

	for i := range data {
		var first = true
		for key := range data[i].FieldTime {
			_values[nv] = parquet.ByteArray(key)
            nv++
			_def[n] = 1
			_rep[n] = 1

			if first {
				first = false
				_rep[n] = 0
			}

			n++
		}

        if len(data[i].FieldTime) == 0 {
            n++
        }
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}




func (s *QueryDataWriter) _FieldTime_value(data []QueryData) values {
	var n int
	for i := range data {
		for range data[i].FieldTime {
			n++
		}

        if len(data[i].FieldTime) == 0 {
            n++
        }
	}

	var _values = make([]int64, n)
	var _def = make([]int16, n)
	var _rep = make([]int16, n)

	n = 0
	var nv int

	for i := range data {
		var first = true
		for _, val := range data[i].FieldTime {
			_values[nv] = int64(val)
			nv++
			_def[n] = 1
			_rep[n] = 1

			if first {
				first = false
				_rep[n] = 0
			}

			n++
		}

        if len(data[i].FieldTime) == 0 {
            n++
        }
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}




func (s *QueryDataWriter) _PathTime_key(data []QueryData) values {
	var n int
	for i := range data {
		for range data[i].PathTime {
			n++
		}

        if len(data[i].PathTime) == 0 {
            n++
        }
	}

	var _values = make([]parquet.ByteArray, n)
	var _def = make([]int16, n)
	var _rep = make([]int16, n)

	n = 0
	var nv int

	for i := range data {
		var first = true
		for key := range data[i].PathTime {
			_values[nv] = parquet.ByteArray(key)
            nv++
			_def[n] = 1
			_rep[n] = 1

			if first {
				first = false
				_rep[n] = 0
			}

			n++
		}

        if len(data[i].PathTime) == 0 {
            n++
        }
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}




func (s *QueryDataWriter) _PathTime_value(data []QueryData) values {
	var n int
	for i := range data {
		for range data[i].PathTime {
			n++
		}

        if len(data[i].PathTime) == 0 {
            n++
        }
	}

	var _values = make([]int64, n)
	var _def = make([]int16, n)
	var _rep = make([]int16, n)

	n = 0
	var nv int

	for i := range data {
		var first = true
		for _, val := range data[i].PathTime {
			_values[nv] = int64(val)
			nv++
			_def[n] = 1
			_rep[n] = 1

			if first {
				first = false
				_rep[n] = 0
			}

			n++
		}

        if len(data[i].PathTime) == 0 {
            n++
        }
	}

	return values{values: _values, defLev: _def, repLev: _rep}
}


func (s *QueryDataWriter) values(data []QueryData) []values {
    return []values{
	    s._ID(data),
	    s._CallsLeftPerMinute(data),
	    s._CallsLeftPerHour(data),
	    s._Duration(data),
	    s._SidecarProcessTime(data),
	    s._ServerProcessTime(data),
	    s._Status(data),
	    s._TypeCounts_key(data),
	    s._TypeCounts_value(data),
	    s._FieldCounts_key(data),
	    s._FieldCounts_value(data),
	    s._PathCounts_key(data),
	    s._PathCounts_value(data),
	    s._FieldTime_key(data),
	    s._FieldTime_value(data),
	    s._PathTime_key(data),
	    s._PathTime_value(data),
    }
}

func (s *QueryDataWriter) Write(out io.Writer, values []QueryData) {
	var writer = file.NewParquetWriter(out, s.schema.Root(), file.WithWriterProps(
		parquet.NewWriterProperties(parquet.WithVersion(parquet.V1_0)),
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
