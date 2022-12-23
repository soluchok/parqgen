package querydata

type QueryData struct {
	ID              int64
	TraceID         string
	Roles           []string
	HasErrors       bool
	TypeCounts      map[string]int32
	ErrorReasonsMap [][][]map[string][]map[string][][]int32
	Stopwatch       []map[string]int64
	Errors          []map[string][]string
}
