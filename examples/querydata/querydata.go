package querydata

//go:generate go run ../../main.go -source querydata.go -destination querydata_gen.go -type QueryData -package querydata

type QueryData struct {
	ID                  int64                 `parquet:"name=id"`
	OrgName             string                `parquet:"name=org_name, converted=UTF8"`
	TraceID             string                `parquet:"name=trace_id, converted=UTF8, repetition=required"`
	ServiceID           int64                 `parquet:"name=service_id"`
	ServiceName         string                `parquet:"name=service_name, converted=UTF8"`
	ServiceLabel        string                `parquet:"name=service_label, converted=UTF8"`
	ObservedAt          int64                 `parquet:"name=observed_at, converted=TIMESTAMP_MILLIS"`
	ClientAddr          string                `parquet:"name=client_addr, converted=UTF8"`
	ClientVersion       string                `parquet:"name=client_version, converted=UTF8"`
	UserID              string                `parquet:"name=user_id"`
	Profile             string                `parquet:"name=profile, converted=UTF8"`
	Roles               []string              `parquet:"name=roles"`
	OperationType       string                `parquet:"name=operation_type, converted=UTF8, repetition=required"`
	OperationName       string                `parquet:"name=operation_name, converted=UTF8, repetition=required"`
	QueryHash           string                `parquet:"name=query_hash, converted=UTF8"`
	QueryInput          string                `parquet:"name=query_input, converted=UTF8"`
	QueryOutput         string                `parquet:"name=query_output, converted=UTF8"`
	Depth               int32                 `parquet:"name=depth"`
	Height              int32                 `parquet:"name=height"`
	Directives          int32                 `parquet:"name=directives"`
	RequestSize         int32                 `parquet:"name=request_size"`
	ResponseSize        int32                 `parquet:"name=response_size"`
	CountTotal          int32                 `parquet:"name=count_total"`
	CreditLeft          int32                 `parquet:"name=credit_left"`
	CreditLeftPerMinute int32                 `parquet:"name=credit_left_per_minute"`
	CreditLeftPerHour   int32                 `parquet:"name=credit_left_per_hour"`
	CallsLeftPerMinute  int32                 `parquet:"name=calls_left_per_minute"`
	CallsLeftPerHour    int32                 `parquet:"name=calls_left_per_hour"`
	Duration            float64               `parquet:"name=duration, repetition=required"`
	SidecarProcessTime  float64               `parquet:"name=sidecar_process_time, repetition=required"`
	ServerProcessTime   float64               `parquet:"name=server_process_time, repetition=required"`
	Status              string                `parquet:"name=status, converted=UTF8, repetition=required"`
	InternalReason      string                `parquet:"name=internal_reason, converted=UTF8"`
	TypeCounts          map[string]int32      `parquet:"name=type_counts, keyconverted=UTF8, valuerepetition=required"`
	FieldCounts         map[string]int32      `parquet:"name=field_counts, keyconverted=UTF8, valuerepetition=required"`
	PathCounts          map[string]int32      `parquet:"name=path_counts, keyconverted=UTF8, valuerepetition=required"`
	FieldTime           map[string]int64      `parquet:"name=field_time, keyconverted=UTF8, valuerepetition=required"`
	PathTime            map[string]int64      `parquet:"name=path_time, keyconverted=UTF8, valuerepetition=required"`
	HasErrors           bool                  `parquet:"name=has_errors"`
	Errors              []map[string][]string `parquet:"name=errors, keyconverted=UTF8"`
	ErrorReasonsMap     map[string][]int32    `parquet:"name=error_reasons_map, keyconverted=UTF8"`
	InternalError       string                `parquet:"name=internal_error, converted=UTF8"`
	InternalStack       string                `parquet:"name=internal_stack, converted=UTF8"`
	Stopwatch           []map[string]int64    `parquet:"name=stopwatch, keyconverted=UTF8"`
}
