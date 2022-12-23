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
	keys   [][]interface{}
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

func (s *QueryDataWriter) _ID(data []QueryData) values {
	var n = len(data)

	var (
		_values = make([]int64, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		_values[i] = int64(data[i].ID)

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _TraceID(data []QueryData) values {
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

		_values[i] = parquet.ByteArray(data[i].TraceID)

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _Roles(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Roles
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
		var val0 = data[i].Roles

		if len(val0) == 0 {
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

func (s *QueryDataWriter) _HasErrors(data []QueryData) values {
	var n = len(data)

	var (
		_values = make([]bool, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		_values[i] = bool(data[i].HasErrors)

	}

	return values{values: _values, defLev: _def, repLev: _rep}
}

func (s *QueryDataWriter) _TypeCounts_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].TypeCounts
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

		var _lastRep int16
		var val0 = data[i].TypeCounts

		if len(val0) == 0 {
			n++
		}

		var c0 int
		for val1 := range val0 {
			if 0 >= len(s.keys) {
				s.keys = append(s.keys, make([][]interface{}, 1-len(s.keys))...)
			}

			s.keys[0] = append(s.keys[0], val1)

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

func (s *QueryDataWriter) _TypeCounts_mapValue(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].TypeCounts
		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			_ = val1
			n++

		}
	}

	var (
		_values = make([]int32, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].TypeCounts

		var loc0 = s.keys[0]

		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			val1 = val0[loc0[0].(string)]
			loc0 = loc0[1:]

			val1 = val0[s.keys[0][0].(string)]
			s.keys[0] = s.keys[0][1:]

			_values[nv] = int32(val1)
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

func (s *QueryDataWriter) _ErrorReasonsMap_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].ErrorReasonsMap
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

		var _lastRep int16
		var val0 = data[i].ErrorReasonsMap

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

					var c3 int
					for val4 := range val3 {
						if 3 >= len(s.keys) {
							s.keys = append(s.keys, make([][]interface{}, 4-len(s.keys))...)
						}

						s.keys[3] = append(s.keys[3], val4)

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

func (s *QueryDataWriter) _ErrorReasonsMap_mapValue_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].ErrorReasonsMap
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

		var _lastRep int16
		var val0 = data[i].ErrorReasonsMap

		var loc3 = s.keys[3]

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

						val4 = val3[loc3[0].(string)]
						loc3 = loc3[1:]

						if len(val4) == 0 {
							n++
						}

						for _, val5 := range val4 {

							if len(val5) == 0 {
								n++
							}

							var c5 int
							for val6 := range val5 {
								if 5 >= len(s.keys) {
									s.keys = append(s.keys, make([][]interface{}, 6-len(s.keys))...)
								}

								s.keys[5] = append(s.keys[5], val6)

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

func (s *QueryDataWriter) _ErrorReasonsMap_mapValue_mapValue(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].ErrorReasonsMap
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
		_values = make([]int32, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].ErrorReasonsMap

		var loc3 = s.keys[3]

		var loc5 = s.keys[5]

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

						val4 = val3[loc3[0].(string)]
						loc3 = loc3[1:]

						val4 = val3[s.keys[3][0].(string)]
						s.keys[3] = s.keys[3][1:]

						if len(val4) == 0 {
							n++
						}

						for _, val5 := range val4 {

							if len(val5) == 0 {
								n++
							}

							for _, val6 := range val5 {

								val6 = val5[loc5[0].(string)]
								loc5 = loc5[1:]

								val6 = val5[s.keys[5][0].(string)]
								s.keys[5] = s.keys[5][1:]

								if len(val6) == 0 {
									n++
								}

								for _, val7 := range val6 {

									if len(val7) == 0 {
										n++
									}

									for _, val8 := range val7 {

										_values[nv] = int32(val8)
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

func (s *QueryDataWriter) _Stopwatch_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Stopwatch
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

		var _lastRep int16
		var val0 = data[i].Stopwatch

		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			var c1 int
			for val2 := range val1 {
				if 1 >= len(s.keys) {
					s.keys = append(s.keys, make([][]interface{}, 2-len(s.keys))...)
				}

				s.keys[1] = append(s.keys[1], val2)

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

func (s *QueryDataWriter) _Stopwatch_mapValue(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Stopwatch
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
		_values = make([]int64, n)
		_def    = make([]int16, n)
		_rep    = make([]int16, n)
	)

	n = 0
	var nv int
	_ = nv

	for i := range data {

		var _lastRep int16
		var val0 = data[i].Stopwatch

		var loc1 = s.keys[1]

		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				val2 = val1[s.keys[1][0].(string)]
				s.keys[1] = s.keys[1][1:]

				_values[nv] = int64(val2)
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

func (s *QueryDataWriter) _Errors_mapKey(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Errors
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

		var _lastRep int16
		var val0 = data[i].Errors

		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			var c1 int
			for val2 := range val1 {
				if 1 >= len(s.keys) {
					s.keys = append(s.keys, make([][]interface{}, 2-len(s.keys))...)
				}

				s.keys[1] = append(s.keys[1], val2)

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

func (s *QueryDataWriter) _Errors_mapValue(data []QueryData) values {
	var n = len(data)

	n = 0
	for i := range data {
		var val0 = data[i].Errors
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
		var val0 = data[i].Errors

		var loc1 = s.keys[1]

		if len(val0) == 0 {
			n++
		}

		for _, val1 := range val0 {

			if len(val1) == 0 {
				n++
			}

			for _, val2 := range val1 {

				val2 = val1[loc1[0].(string)]
				loc1 = loc1[1:]

				val2 = val1[s.keys[1][0].(string)]
				s.keys[1] = s.keys[1][1:]

				if len(val2) == 0 {
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

func (s *QueryDataWriter) values(data []QueryData) []values {
	return []values{
		s._ID(data),
		s._TraceID(data),
		s._Roles(data),
		s._HasErrors(data),
		s._TypeCounts_mapKey(data),
		s._TypeCounts_mapValue(data),
		s._ErrorReasonsMap_mapKey(data),
		s._ErrorReasonsMap_mapValue_mapKey(data),
		s._ErrorReasonsMap_mapValue_mapValue(data),
		s._Stopwatch_mapKey(data),
		s._Stopwatch_mapValue(data),
		s._Errors_mapKey(data),
		s._Errors_mapValue(data),
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
