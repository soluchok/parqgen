package base

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

var benchmarkTable = []struct {
	input []Base
}{
	{input: newQueryData(100)},
	{input: newQueryData(1_000)},
	{input: newQueryData(10_000)},
	{input: newQueryData(100_000)},
}

func BenchmarkQueryData_parquet(b *testing.B) {
	var out bytes.Buffer

	var qdw, err = NewParquetWriter(&out)
	if err != nil {
		panic(err)
	}

	for _, test := range benchmarkTable {
		b.Run(fmt.Sprintf("input_size_%d", len(test.input)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {

				for j := range test.input {
					qdw.Add(test.input[j])
				}

				if err = qdw.Write(); err != nil {
					panic(err)
				}

				out.Reset()
			}
		})
	}
}

func BenchmarkQueryData_inigo(b *testing.B) {
	var out bytes.Buffer

	var qdw = NewBaseWriter(WithDataPageSize(10 * 1024))

	for _, test := range benchmarkTable {
		b.Run(fmt.Sprintf("input_size_%d", len(test.input)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {

				qdw.Write(&out, test.input)

				out.Reset()
			}
		})
	}
}

func newQueryData(num int) []Base {
	var data = make([]Base, num)

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

		data[i] = Base{
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
			HasErrors:           true,
			InternalError:       fmt.Sprintf("query_output_%d", i),
			InternalStack:       fmt.Sprintf("query_output_%d", i),
		}
	}

	return data
}
