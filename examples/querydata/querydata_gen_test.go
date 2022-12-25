package querydata

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
)

func TestQueryDataWriter_Write(t *testing.T) {
	var (
		qdw = NewQueryDataWriter()
		out = buffer.NewBufferFile()
	)

	var input = []QueryData{
		{
			Col1:  "",
			Col2:  []string{},
			Col3:  [][]string{},
			Col4:  [][][]string{},
			Col5:  map[string]string{},
			Col6:  map[string][]string{},
			Col7:  map[string][][]string{},
			Col8:  map[string][][][]string{},
			Col9:  []map[string]string{},
			Col10: []map[string][]string{},
			Col11: [][]map[string][][]string{},
			Col12: [][][]map[string][][][]string{},
			Col13: map[string]map[string]string{},
			Col14: map[string]map[string][]string{},
			Col15: map[string]map[string][][]string{},
			Col16: map[string]map[string][]map[string]map[string]string{},
			Col17: map[string]map[string][]map[string]map[string][]string{},
			Col18: map[string]map[string][][]map[string]map[string][][]string{},
			Col19: []map[string]map[string][]map[string]map[string]string{},
			Col20: []map[string]map[string][]map[string]map[string][]string{},
			Col21: [][]map[string]map[string][][]map[string]map[string][][]string{},
		},
		{
			Col1: "data_1",
			Col2: []string{
				"data_2",
			},
			Col3: [][]string{
				{"data_3"},
			},
			Col4: [][][]string{
				{
					{"data_4"},
				},
			},
			Col5: map[string]string{
				"map_data_5": "map_data_6",
			},
			Col6: map[string][]string{
				"map_data_7": {
					"map_data_8",
				},
			},
			Col7: map[string][][]string{
				"map_data_9": {
					{
						"map_data_10",
					},
				},
			},
			Col8: map[string][][][]string{
				"map_data_11": {
					{
						{
							"map_data_12",
						},
					},
				},
			},
			Col9: []map[string]string{
				{
					"map_data_13": "map_data_14",
				},
			},
			Col10: []map[string][]string{
				{
					"map_data_15": {
						"map_data_16",
					},
				},
			},
			Col11: [][]map[string][][]string{
				{
					{
						"map_data_17": {
							{
								"map_data_18",
							},
						},
					},
				},
			},
			Col12: [][][]map[string][][][]string{
				{
					{
						{
							"map_data_17": {
								{
									{
										"map_data_18",
									},
								},
							},
						},
					},
				},
			},
			Col13: map[string]map[string]string{
				"col13.0": {},
				"col13.1": {
					"col13.2": "col13.3",
				},
			},
			Col14: map[string]map[string][]string{
				"col14.0": {},
				"col14.1": {
					"col14.2": {"col14.3", "col14.4"},
				},
			},
			Col15: map[string]map[string][][]string{
				"Col15.0": {},
				"Col15.1": {
					"Col15.2": {
						{}, {"Col15.3", "Col15.4"}, {}, {"Col15.5", "Col15.6"},
					},
				},
			},
			Col16: map[string]map[string][]map[string]map[string]string{
				"Col16.0": {},
				"Col16.1": {
					"Col16.1.1":   {},
					"Col16.1.1.1": {{"Col16.1.1.1.1": {}}},
					"Col16.2": {
						{},
						{
							"Col16.3": {
								"Col16.4": "Col16.5",
							},
						},
						{
							"Col16.6": {
								"Col16.7": "Col16.8",
								"Col16.9": "Col16.10",
							},
						},
					},
				},
			},
			Col17: map[string]map[string][]map[string]map[string][]string{
				"Col17.0":        {},
				"Col17.0.1":      {"Col17.0.1.1": {}},
				"Col17.0.1.2":    {"Col17.0.1.1.2": {{"Col17.0.1.1.2.3": {}}}},
				"Col17.0.1.2.12": {"Col17.0.1.1.2.14": {{"Col17.0.1.1.2.3.12": {"Col17.0.1.1.2.3.121": {}}}}},
				"Col17.1": {
					"Col17.2": {
						{},
						{
							"Col17.3": {
								"Col17.4": {"Col17.5", "Col17.5.1"},
							},
						},
						{
							"Col17.6": {
								"Col17.7": {"Col17.8"},
								"Col17.9": {"Col17.10"},
							},
						},
					},
				},
			},
			Col18: map[string]map[string][][]map[string]map[string][][]string{
				"Col18.0":       {},
				"Col18.0.1":     {"Col18.0.1.1": {}},
				"Col18.0.1.1":   {"Col18.0.1.1.2": {{{"Col18.0.1.1.3": {}}}}},
				"Col18.0.1.1.1": {"Col18.0.1.1.2.1": {{{"Col18.0.1.1.3.1": {"Col18.0.1.1.3.2": {}}}}}},
				"Col18.1": {
					"Col18.2": {
						{},
						{
							{},
							{
								"Col18.3": {
									"Col18.4": {{"Col18.5", "Col18.5.1"}},
								},
							},
							{
								"Col18.6": {
									"Col18.7": {{}, {"Col18.8"}},
									"Col18.9": {{"Col18.10"}, {}},
								},
							},
						},
					},
				},
			},
			Col19: []map[string]map[string][]map[string]map[string]string{
				{},
				{
					"Col19.0":     {},
					"Col19.0.1":   {"Col19.0.1.1": {}},
					"Col19.0.1.2": {"Col19.0.1.1.2": {{"Col19.0.1.1.2.1": {}}}},
					"Col19.1": {
						"Col19.2": {
							{},
							{
								"Col19.3": {
									"Col19.4": "Col19.5",
								},
							},
							{
								"Col19.6": {
									"Col19.7": "Col19.8",
									"Col19.9": "Col19.10",
								},
							},
						},
					},
				},
			},
			Col20: []map[string]map[string][]map[string]map[string][]string{
				{},
				{
					"Col20.0":       {},
					"Col20.0.1":     {"Col20.0.2": {}},
					"Col20.0.1.2.3": {"Col20.0.2.3.4": {{"Col20.0.2.3.4.8": {}}}},
					"Col20.0.1.2.4": {"Col20.0.2.3.5": {{"Col20.0.2.3.4.6": {"Col20.0.2.3.4.1": {}}}}},
					"Col20.1": {
						"Col20.2": {
							{},
							{
								"Col20.3": {
									"Col20.4": {"Col20.5", "Col20.5.1"},
								},
							},
							{
								"Col20.6": {
									"Col20.7": {"Col20.8"},
									"Col20.9": {"Col20.10", "Col20.11", "Col20.12"},
								},
							},
						},
					},
				},
			},
			Col21: [][]map[string]map[string][][]map[string]map[string][][]string{
				{},
				{
					{},
					{
						"Col21.0":     {},
						"Col21.0.1":   {"Col21.0.1.1": {}},
						"Col21.0.1.1": {"Col21.0.1.1.1": {{}}},
						"Col21.0.1.1.1": {"Col21.0.1.1.1": {
							{
								{
									"Col21.0.1.1.1.1": map[string][][]string{
										"Col21.0.1.1.1.1.1": {},
									},
								},
							},
						},
						},
						"Col21.1": {
							"Col21.2": {
								{
									{},
									{
										"Col21.3": {
											"Col21.4": {{}, {"Col21.5", "Col21.5.1"}},
										},
									},
									{
										"Col21.6": {
											"Col21.7": {{"Col21.8"}},
											"Col21.9": {{"Col21.10", "Col21.11", "Col21.12"}},
										},
										"Col21.7": {},
									},
									{
										"Col21.6.1": {
											"Col21.7.1": {{"Col21.8.1"}},
											"Col21.9.1": {{"Col21.10.1", "Col21.11.1", "Col21.12.1"}},
										},
										"Col21.7.1": {},
									},
								},
							},
						},
					},
				},
				{{}},
				{
					{},
					{
						"Col21.1.0": {},
						"Col21.1.1": {
							"Col21.1.2": {
								{
									{},
									{
										"Col21.1.3": {
											"Col21.1.4": {{}, {"Col21.1.5", "Col21.1.5.1"}},
										},
									},
									{
										"Col21.1.6": {
											"Col21.1.7": {{"Col21.1.8"}},
											"Col21.1.9": {{"Col21.1.10", "Col21.1.11", "Col21.1.12"}},
										},
										"Col21.1.7": {},
									},
									{
										"Col21.1.6.1": {
											"Col21.1.7.1": {{"Col21.1.8.1"}},
											"Col21.1.9.1": {{"Col21.1.10.1", "Col21.1.11.1", "Col21.1.12.1"}},
										},
										"Col21.1.7.1": {},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			Col1:  "info_3",
			Col2:  []string{},
			Col3:  [][]string{},
			Col4:  [][][]string{},
			Col5:  map[string]string{},
			Col6:  map[string][]string{},
			Col7:  map[string][][]string{},
			Col8:  map[string][][][]string{},
			Col9:  []map[string]string{},
			Col10: []map[string][]string{},
			Col11: [][]map[string][][]string{},
			Col12: [][][]map[string][][][]string{},
			Col13: map[string]map[string]string{},
			Col14: map[string]map[string][]string{},
			Col15: map[string]map[string][][]string{},
			Col16: map[string]map[string][]map[string]map[string]string{},
			Col17: map[string]map[string][]map[string]map[string][]string{},
			Col18: map[string]map[string][][]map[string]map[string][][]string{},
			Col19: []map[string]map[string][]map[string]map[string]string{},
			Col20: []map[string]map[string][]map[string]map[string][]string{},
			Col21: [][]map[string]map[string][][]map[string]map[string][][]string{},
		},
		{
			Col1: "payload_1",
			Col2: []string{
				"payload_2",
				"payload_2.1",
				"payload_2.2",
			},
			Col3: [][]string{
				{"payload_3", "payload_3.1", "payload_3.2"},
				{},
				{"payload_3.3", "payload_3.4", "payload_3.5"},
				{},
				{},
				{"payload_3.6", "payload_3.7", "payload_3.8"},
			},
			Col4: [][][]string{
				{
					{"payload_4", "payload_4.1", "payload_4.2", "payload_4.3"},
					{},
					{"payload_4.4", "payload_4.5", "payload_4.6", "payload_4.7"},
					{},
					{},
					{"payload_4.8", "payload_4.9", "payload_4.10", "payload_4.11"},
				},
				{},
				{{}},
				{
					{"payload_4.12", "payload_4.13", "payload_4.14", "payload_4.15"},
					{},
					{"payload_4.16", "payload_4.17", "payload_4.18", "payload_4.19"},
					{},
					{},
					{"payload_4.20", "payload_4.21", "payload_4.22", "payload_4.23"},
				},
				{},
				{
					{"payload_4.24", "payload_4.25", "payload_4.26", "payload_4.27"},
				},
				{{}},
			},
			Col5: map[string]string{
				"map_payload_5":   "map_payload_6",
				"map_payload_5.1": "map_payload_6.1",
			},
			Col6: map[string][]string{
				"map_payload_7": {
					"map_payload_8",
				},
				"map_payload_7.1": {},
				"map_payload_7.2": {
					"map_payload_8.1",
					"map_payload_8.2",
					"map_payload_8.3",
				},
				"map_payload_7.3": {},
			},
			Col7: map[string][][]string{
				"map_payload_9": {
					{
						"map_payload_10",
					},
				},
				"map_payload_9.1": {},
				"map_payload_9.2": {
					{},
					{"map_payload_10.1", "map_payload_10.2"},
					{},
					{"map_payload_10.3", "map_payload_10.4"},
				},
				"map_payload_9.3": {{}},
				"map_payload_9.5": {{"map_payload_10.5", "map_payload_10.6"}, {}},
			},
			Col8: map[string][][][]string{
				"map_payload_11": {
					{
						{
							"map_payload_12",
						},
					},
				},
				"map_payload_11.1": {},
				"map_payload_11.2": {{}},
				"map_payload_11.3": {{{}}},
				"map_payload_11.4": {
					{
						{
							"map_payload_12.1",
						},
						{
							"map_payload_12.2",
						},
						{
							"map_payload_12.3",
						},
					},
				},
				"map_payload_11.5": {
					{
						{
							"map_payload_12.4",
						},
					},
					{
						{
							"map_payload_12.5",
						},
						{
							"map_payload_12.6",
						},
					},
					{
						{
							"map_payload_12.7", "map_payload_12.8",
						},
						{
							"map_payload_12.9",
						},
					},
				},
			},
			Col9: []map[string]string{
				{},
				{
					"map_payload_13": "map_payload_14",
				},
				{},
				{
					"map_payload_13.1": "map_payload_14.1",
					"map_payload_13.2": "map_payload_14.2",
				},
				{},
			},
			Col10: []map[string][]string{
				{
					"map_payload_15": {
						"map_payload_16",
					},
				},
				{},
				{
					"map_payload_15.1": {},
				},
				{
					"map_payload_15.2": {
						"map_payload_16.1",
					},
				},
			},
			Col11: [][]map[string][][]string{
				{
					{
						"map_payload_17": {
							{
								"map_payload_18",
							},
						},
					},
				},
				{},
				{{}},
				{
					{
						"map_payload_17.1": {},
					},
				},
				{
					{
						"map_payload_17.2": {
							{
								"map_payload_18.1",
								"map_payload_18.2",
							},
							{},
							{
								"map_payload_18.3",
								"map_payload_18.4",
							},
						},
						"map_payload_17.3": {
							{
								"map_payload_18.5",
								"map_payload_18.6",
							},
							{},
							{
								"map_payload_18.7",
								"map_payload_18.8",
							},
						},
					},
				},
			},
			Col12: [][][]map[string][][][]string{
				{},
				{
					{},
					{
						{
							"map_payload_17": {
								{
									{
										"map_payload_18",
									},
								},
							},
						},
					},
					{},
					{{}},
					{
						{
							"map_payload_17.1": {
								{
									{
										"map_payload_18",
									},
								},
							},
							"map_payload_17.2": {},
							"map_payload_17.3": {{}},
							"map_payload_17.4": {{{}}},
							"map_payload_17.5": {
								{
									{
										"map_payload_18.1",
									},
									{
										"map_payload_18.2",
									},
								},
								{},
								{
									{},
									{
										"map_payload_18.3",
									},
								},
							},
						},
					},
				},
			},
			Col13: map[string]map[string]string{},
			Col14: map[string]map[string][]string{},
			Col15: map[string]map[string][][]string{},
			Col16: map[string]map[string][]map[string]map[string]string{},
			Col17: map[string]map[string][]map[string]map[string][]string{},
			Col18: map[string]map[string][][]map[string]map[string][][]string{},
			Col19: []map[string]map[string][]map[string]map[string]string{},
			Col20: []map[string]map[string][]map[string]map[string][]string{},
			Col21: [][]map[string]map[string][][]map[string]map[string][][]string{},
		},
	}

	qdw.Write(out, input)

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
