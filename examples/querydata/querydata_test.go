package querydata

import (
	"bytes"
	_ "embed"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/writer"
)

var (
	//go:embed query_data_schema.json
	jsonSchema string
)

var benchmarkTable = []struct {
	input []QueryData
}{
	{input: newQueryData(100)},
	{input: newQueryData(1_000)},
	{input: newQueryData(10_000)},
	{input: newQueryData(100_000)},
}

func BenchmarkQueryData_xitongsys(b *testing.B) {
	var out bytes.Buffer

	sh, err := schema.NewSchemaHandlerFromJSON(jsonSchema)
	if err != nil {
		b.Fatal(err)
	}

	for _, test := range benchmarkTable {
		b.Run(fmt.Sprintf("input_size_%d", len(test.input)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				p, err := writer.NewParquetWriterFromWriter(&out, sh, 4)
				if err != nil {
					b.Fatal(err)
				}

				p.SchemaHandler = sh
				p.Footer.Schema = p.Footer.Schema[:0]
				p.Footer.Schema = append(p.Footer.Schema, p.SchemaHandler.SchemaElements...)

				for _, item := range test.input {
					if err = p.Write(item); err != nil {
						b.Fatal(err)
					}
				}

				if err = p.WriteStop(); err != nil {
					b.Fatal(err)
				}

				out.Reset()
			}
		})
	}
}

func BenchmarkQueryData_inigo(b *testing.B) {
	var out bytes.Buffer

	var qdw = NewQueryDataWriter(
		WithDictionaryFor("service_name", true),
		WithDictionaryFor("service_label", true),
		WithDictionaryFor("client_version", true),
		WithDictionaryFor("user_id", true),
		WithDictionaryFor("profile", true),
		WithDictionaryFor("operation_type", true),
		WithDictionaryFor("operation_name", true),
		WithDictionaryFor("status", true),
		WithDictionaryFor("internal_reason", true),
	)

	for _, test := range benchmarkTable {
		b.Run(fmt.Sprintf("input_size_%d", len(test.input)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {

				qdw.Write(&out, test.input)

				out.Reset()
			}
		})
	}
}

func newQueryData(num int) []QueryData {
	var data = make([]QueryData, num)

	rand.Seed(time.Now().Unix())

	var (
		serviceName    = []string{"service_name_1", "service_name_2", "service_name_3", "service_name_4"}
		serviceLabel   = []string{"service_label_1", "service_label_2", "service_label_3", "service_label_4"}
		clientVersion  = []string{"client_version_1", "client_version_2", "client_version_3", "client_version_4"}
		profile        = []string{"profile_1", "profile_2", "profile_3", "profile_4"}
		operationType  = []string{"operation_type_1", "operation_type_2", "operation_type_3"}
		operationName  = []string{"operation_name_1", "operation_name_2", "operation_name_3"}
		status         = []string{"status_1", "status_2", "status_3"}
		internalReason = []string{"internal_reason_1", "internal_reason_2", "internal_reason_3"}
	)

	for i := 0; i < num; i++ {

		data[i] = QueryData{
			ID:            int64(i),
			TraceID:       fmt.Sprintf("trace_id_%d", i),
			OrgName:       fmt.Sprintf("org_name_%d", i),
			ServiceID:     int64(i),
			ServiceName:   serviceName[rand.Intn(len(serviceName))],
			ServiceLabel:  serviceLabel[rand.Intn(len(serviceLabel))],
			ObservedAt:    time.Now().Unix(),
			ClientAddr:    fmt.Sprintf("client_version_%d", i),
			ClientVersion: clientVersion[rand.Intn(len(clientVersion))],
			UserID:        fmt.Sprintf("user_id_%d", i),
			Profile:       profile[rand.Intn(len(profile))],
			Roles: []string{
				fmt.Sprintf("user%d", i),
				fmt.Sprintf("admin%d", i),
				fmt.Sprintf("manager%d", i),
			},
			OperationType:       operationType[rand.Intn(len(operationType))],
			OperationName:       operationName[rand.Intn(len(operationName))],
			QueryHash:           fmt.Sprintf("query_hash_%d", i),
			QueryInput:          fmt.Sprintf("query_input_%d", i),
			QueryOutput:         fmt.Sprintf("query_output_%d", i),
			Depth:               int32(i),
			Height:              int32(i),
			Directives:          int32(i),
			RequestSize:         int32(i),
			ResponseSize:        int32(i),
			CountTotal:          int32(i),
			CreditLeft:          int32(i),
			CreditLeftPerMinute: int32(i),
			CreditLeftPerHour:   int32(i),
			CallsLeftPerMinute:  int32(i),
			CallsLeftPerHour:    int32(i),
			Duration:            float64(i),
			SidecarProcessTime:  float64(i),
			ServerProcessTime:   float64(i),
			Status:              status[rand.Intn(len(status))],
			InternalReason:      internalReason[rand.Intn(len(internalReason))],
			TypeCounts: map[string]int32{
				fmt.Sprintf("version_%d", i): int32(i),
				fmt.Sprintf("login_%d", i):   int32(i),
				fmt.Sprintf("service_%d", i): int32(i),
				fmt.Sprintf("user_%d", i):    int32(i),
				fmt.Sprintf("count_%d", i):   int32(i),
				fmt.Sprintf("data_%d", i):    int32(i),
			},
			FieldCounts: map[string]int32{
				fmt.Sprintf("version_field_%d", i): int32(i),
				fmt.Sprintf("login_field_%d", i):   int32(i),
				fmt.Sprintf("service_field_%d", i): int32(i),
				fmt.Sprintf("user_field_%d", i):    int32(i),
				fmt.Sprintf("count_field_%d", i):   int32(i),
				fmt.Sprintf("data_field_%d", i):    int32(i),
			},
			PathCounts: map[string]int32{
				fmt.Sprintf("version_path_%d", i): int32(i),
				fmt.Sprintf("login_path_%d", i):   int32(i),
				fmt.Sprintf("service_path_%d", i): int32(i),
				fmt.Sprintf("user_path_%d", i):    int32(i),
				fmt.Sprintf("count_path_%d", i):   int32(i),
				fmt.Sprintf("data_path_%d", i):    int32(i),
			},
			FieldTime: map[string]int64{
				fmt.Sprintf("version_field_time_%d", i): int64(i),
				fmt.Sprintf("login_field_time_%d", i):   int64(i),
				fmt.Sprintf("service_field_time_%d", i): int64(i),
				fmt.Sprintf("user_field_time_%d", i):    int64(i),
				fmt.Sprintf("count_field_time_%d", i):   int64(i),
				fmt.Sprintf("data_field_time_%d", i):    int64(i),
			},
			PathTime: map[string]int64{
				fmt.Sprintf("version_path_time_%d", i): int64(i),
				fmt.Sprintf("login_path_time_%d", i):   int64(i),
				fmt.Sprintf("service_path_time_%d", i): int64(i),
				fmt.Sprintf("user_path_time_%d", i):    int64(i),
				fmt.Sprintf("count_path_time_%d", i):   int64(i),
				fmt.Sprintf("data_path_time_%d", i):    int64(i),
			},
			HasErrors: true,
			Errors: []map[string][]string{
				{},
				{
					fmt.Sprintf("version_errors_%d", i): []string{"1", strconv.Itoa(i)},
					fmt.Sprintf("login_errors_%d", i):   []string{"2", strconv.Itoa(i)},
					fmt.Sprintf("service_errors_%d", i): []string{"3", strconv.Itoa(i)},
					fmt.Sprintf("user_errors_%d", i):    []string{"4", strconv.Itoa(i)},
					fmt.Sprintf("count_errors_%d", i):   []string{"5", strconv.Itoa(i)},
					fmt.Sprintf("data_errors_%d", i):    []string{"6", strconv.Itoa(i)},
				},
				{},
				{
					fmt.Sprintf("version_errors_%d", i): []string{"7", strconv.Itoa(i)},
					fmt.Sprintf("login_errors_%d", i):   []string{"8", strconv.Itoa(i)},
					fmt.Sprintf("service_errors_%d", i): []string{"9", strconv.Itoa(i)},
					fmt.Sprintf("user_errors_%d", i):    []string{"10", strconv.Itoa(i)},
					fmt.Sprintf("count_errors_%d", i):   []string{"11", strconv.Itoa(i)},
					fmt.Sprintf("data_errors_%d", i):    []string{"12", strconv.Itoa(i)},
				},
				{},
			},
			ErrorReasonsMap: map[string][]int32{
				fmt.Sprintf("version_error_reasons_map_%d", i): {int32(i)},
				fmt.Sprintf("login_error_reasons_map_%d", i):   {int32(i)},
				fmt.Sprintf("service_error_reasons_map_%d", i): {int32(i)},
				fmt.Sprintf("user_error_reasons_map_%d", i):    {int32(i)},
				fmt.Sprintf("count_error_reasons_map_%d", i):   {int32(i)},
				fmt.Sprintf("data_error_reasons_map_%d", i):    {int32(i)},
			},
			InternalError: fmt.Sprintf("query_output_%d", i),
			InternalStack: fmt.Sprintf("query_output_%d", i),
			Stopwatch: []map[string]int64{
				{
					fmt.Sprintf("version_stopwatch_%d", i): int64(i),
					fmt.Sprintf("login_stopwatch_%d", i):   int64(i),
					fmt.Sprintf("service_stopwatch_%d", i): int64(i),
					fmt.Sprintf("user_stopwatch_%d", i):    int64(i),
					fmt.Sprintf("count_stopwatch_%d", i):   int64(i),
					fmt.Sprintf("data_stopwatch_%d", i):    int64(i),
				},
			},
		}
	}

	return data
}
