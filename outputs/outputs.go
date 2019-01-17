package outputs

import "time"

// ServerInfo holds server information gathered from first 2 log lines
type ServerInfo struct {
	Binary             string
	VersionShort       string
	Version            string
	VersionDescription string
	TCPPort            int
	UnixSocket         string
	CumBytes           int
	QueryCount         int
	UniqueQueries      int
	StartTime          time.Time
	EndTime            time.Time
}

// QueryStatsSlice holds a bunch of QueryStats
type QueryStatsSlice []*QueryStats

// QueryStats holds query statistics
type QueryStats struct {
	Hash            [32]byte
	Schema          string
	Count           int
	FingerPrint     string
	CumQueryTime    float64
	CumBytesSent    int
	CumLockTime     float64
	CumRowsSent     int
	CumRowsExamined int
	CumRowsAffected int
	CumKilled       int
	CumErrored      int
	Concurrency     float64
	QueryTime       []float64
	BytesSent       []float64
	LockTime        []float64
	RowsSent        []float64
	RowsExamined    []float64
	RowsAffected    []float64
}

// Outputs is a map containing name to function mapping
var Outputs = map[string]func(ServerInfo, QueryStatsSlice){}

// Add lets each output to add themselves in Outputs
func Add(name string, target func(ServerInfo, QueryStatsSlice)) {
	Outputs[name] = target
}
