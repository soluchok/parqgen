package querydata

type QueryData struct {
	ID int64 `parquet:"name=id"`
	//TraceID             string
	//OrgName             string
	//ServiceID           int64
	//ServiceName         string
	//ServiceLabel        string
	//ObservedAt          int64
	//ClientAddr          string
	//ClientVersion       string
	//UserID              string
	//Profile             string
	//Roles               []string
	//OperationType       string
	//OperationName       string
	//QueryHash           string
	//QueryInput          string
	//QueryOutput         string
	//Depth               int32
	//Height              int32
	//Directives          int32
	//RequestSize         int32
	//ResponseSize        int32
	//CountTotal          int32
	//CreditLeft          int32
	//CreditLeftPerMinute int32
	//CreditLeftPerHour   int32
	CallsLeftPerMinute int32
	CallsLeftPerHour   int32
	Duration           float64
	SidecarProcessTime float64
	ServerProcessTime  float64
	Status             string
	//InternalReason      string
	TypeCounts  map[string]int32 `parquet:"keyconverted=UTF8"`
	FieldCounts map[string]int32 `parquet:"keyconverted=UTF8"`
	PathCounts  map[string]int32 `parquet:"keyconverted=UTF8"`
	FieldTime   map[string]int64 `parquet:"keyconverted=UTF8"`
	PathTime    map[string]int64 `parquet:"keyconverted=UTF8"`
	//HasErrors           bool
	//InternalError       string
	//InternalStack       string
	// NOT SUPPORTED YET
	//Errors              []map[string][]string
	//ErrorReasonsMap     map[string][]int32
	//Stopwatch []map[string]int64
}
