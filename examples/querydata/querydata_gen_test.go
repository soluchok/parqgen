package querydata

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/xitongsys/parquet-go-source/buffer"

	"github.com/stretchr/testify/require"

	"github.com/xitongsys/parquet-go/reader"
)

func TestQueryDataWriter_Write(t *testing.T) {
	var (
		qdw = NewQueryDataWriter()
		out = buffer.NewBufferFile()
	)

	var input = []QueryData{
		{
			ID:        1,
			TraceID:   "trace_1",
			Roles:     []string{"admin", "user", "manager"},
			HasErrors: true,
			TypeCounts: map[string]int32{
				"field1": 1,
				"field2": 2,
				"field3": 3,
			},
			ErrorReasonsMap: [][][]map[string][]map[string][][]int32{
				{
					{
						{
							"tessdfsdft1": []map[string][][]int32{
								{
									"reasdfson1": [][]int32{{1, 2, 3}, {1, 2, 33}},
								},
								{
									"reasdfsdfson2": [][]int32{{3434, 2, 3}, {13, 234, 3}},
								},
							},
							"tessdft12": []map[string][][]int32{
								{
									"324": [][]int32{{1, 32, 3}, {31, 234, 3}},
								},
								{
									"resdfason3": [][]int32{{11, 2, 3}, {1, 234, 3}},
								},
							},
						},
						{
							"fhgdfgh": []map[string][][]int32{
								{
									"sdf": [][]int32{{1, 2, 344}, {1, 233, 3}},
								},
								{
									"sdfhhggh": [][]int32{{3434, 2, 3}, {1, 33234, 3}},
								},
							},
							"fghfh": []map[string][][]int32{
								{
									"324": [][]int32{{133, 32, 3}, {1, 23334, 3}},
								},
								{
									"fhdf": [][]int32{{11, 234, 3}, {1, 343434, 3}},
								},
							},
						},
					},
					{
						{
							"test1": []map[string][][]int32{
								{
									"readdson1": [][]int32{{1, 2, 3}, {31, 2, 3}},
								},
								{
									"readdson2": [][]int32{{3434, 2, 3}, {31, 234, 3}},
								},
							},
							"testd12": []map[string][][]int32{
								{
									"32d4": [][]int32{{1, 32, 3}, {1, 233434, 3}},
								},
								{
									"readdson3": [][]int32{{11, 2, 3}, {1, 234, 3}},
								},
							},
						},
						{
							"fhgdfgh": []map[string][][]int32{
								{
									"sdf": [][]int32{{1, 2, 344}, {1, 233, 3}},
								},
								{
									"sdfhhggh": [][]int32{{3434, 2, 3}, {1, 33234, 3}},
								},
							},
							"fghfh": []map[string][][]int32{
								{
									"324": [][]int32{{133, 32, 3}, {1, 23334, 3}},
								},
								{
									"fhdf": [][]int32{{11, 234, 3}, {1, 343434, 3}},
								},
							},
						},
					},
					{
						{
							"test1": []map[string][][]int32{
								{
									"reason1": [][]int32{{1, 2, 3}, {1, 2, 3}},
								},
								{
									"reason2": [][]int32{{3434, 2, 3}, {1, 234, 3}},
								},
							},
							"test1df2": []map[string][][]int32{
								{
									"324": [][]int32{{1, 32, 3}, {1, 234, 3}},
								},
								{
									"reason3": [][]int32{{11, 2, 3}, {1, 234, 3}},
								},
							},
						},
						{
							"fhgdfgh": []map[string][][]int32{
								{
									"sdf": [][]int32{{1, 2, 344}, {1, 233, 3}},
								},
								{
									"sdfhhggh": [][]int32{{3434, 2, 3}, {1, 33234, 3}},
								},
							},
							"fghfdfh": []map[string][][]int32{
								{
									"324": [][]int32{{133, 32, 3}, {1, 23334, 3}},
								},
								{
									"fhdf": [][]int32{{11, 234, 3}, {1, 343434, 3}},
								},
							},
						},
					},
				},
			},
			Stopwatch: []map[string]int64{
				{
					"stopwatch1": 1,
					"stopwatch2": 2,
					"stopwatch3": 3,
				},
				{
					"stopwatch4": 4,
					"stopwatch5": 5,
					"stopwatch6": 6,
				},
			},
			Errors: []map[string][]string{
				{
					"errors1": []string{"1", "1"},
					"errors2": []string{"2", "2"},
					"errors3": []string{"3", "3"},
				},
				{
					"errors4": []string{"4", "4"},
					"errors5": []string{"5", "5"},
					"errors6": []string{"6", "6"},
				},
			},
		},
		//{
		//ID:              1,
		//TraceID:         "sf",
		//Roles: []string{},
		//HasErrors:       false,
		//TypeCounts:      map[string]int32{"1": 2},
		//ErrorReasonsMap: map[string][]int32{},
		//Stopwatch:       []map[string]int64{},
		//Errors:          []map[string][]string{},
		//},
		//{
		//	ID:              3,
		//	TraceID:         "",
		//	Roles:           []string{},
		//	HasErrors:       false,
		//	TypeCounts:      map[string]int32{},
		//	ErrorReasonsMap: map[string][]int32{},
		//	Stopwatch:       []map[string]int64{},
		//	Errors:          []map[string][]string{},
		//},
		//{
		//	ID:              4,
		//	TraceID:         "",
		//	Roles:           []string{},
		//	HasErrors:       false,
		//	TypeCounts:      map[string]int32{},
		//	ErrorReasonsMap: map[string][]int32{},
		//	Stopwatch:       []map[string]int64{},
		//	Errors:          []map[string][]string{},
		//},
	}

	qdw.Write(out, input)

	os.WriteFile("querydata.parquet", out.Bytes(), 0600)

	pr, err := reader.NewParquetReader(out, nil, 1)
	require.NoError(t, err)

	output := make([]QueryData, int(pr.GetNumRows()))
	require.NoError(t, pr.Read(&output))

	expected, err := json.Marshal(input)
	require.NoError(t, err)

	actual, err := json.Marshal(output)
	require.NoError(t, err)

	require.JSONEq(t, string(expected), string(actual))
}
