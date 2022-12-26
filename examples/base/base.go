package base

//go:generate go run ../../main.go -source base.go -destination base_gen.go -type Base -package base
//go:generate go run github.com/inigolabs/parquet/cmd/parquetgen -input base.go -type Base -package base

type Base struct {
	ID                  int64
	OrgName             string
	TraceID             string
	ServiceID           int64
	ServiceName         string
	ServiceLabel        string
	ObservedAt          int64
	ClientAddr          string
	ClientVersion       string
	UserID              string
	Profile             string
	Roles               []string
	OperationType       string
	OperationName       string
	QueryHash           string
	QueryInput          string
	QueryOutput         string
	Depth               int32
	Height              int32
	Directives          int32
	RequestSize         int32
	ResponseSize        int32
	CountTotal          int32
	CreditLeft          int32
	CreditLeftPerMinute int32
	CreditLeftPerHour   int32
	CallsLeftPerMinute  int32
	CallsLeftPerHour    int32
	Duration            float64
	SidecarProcessTime  float64
	ServerProcessTime   float64
	Status              string
	InternalReason      string
	HasErrors           bool
	InternalError       string
	InternalStack       string
}
