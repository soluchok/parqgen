package querydata

import (
	"bytes"
	"os"
	"testing"
)

func TestQueryDataWriter_Write(t *testing.T) {
	var (
		qdw = NewQueryDataWriter()
		out = bytes.Buffer{}
	)

	qdw.Write(&out, []QueryData{
		{
			Roles: []string{"2"},
			ID:    1,
			//Roles: []string{"1", "2", "3"},
			//TypeCounts: map[string]int32{
			//	"1": 1,
			//},
		},
		{
			//ID: 2,
			//Roles: []string{"4", "5"},
			TypeCounts: map[string]int32{
				"2": 2,
			},
			Roles: []string{"1"},
		},
		{
			//	ID: 3,
			//	//Roles: []string{"6"},
			//TypeCounts: map[string]int32{
			//	"111": 111,
			//	"222": 222,
			//},
		},
		{
			//	ID: 3,
			//	//Roles: []string{"6"},
			TypeCounts: map[string]int32{
				"2": 4,
			},
			Roles: []string{"1", "1"},
		},
	})

	os.WriteFile("querydata.parquet", out.Bytes(), 0600)
}
