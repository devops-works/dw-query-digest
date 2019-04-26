package outputs

import (
	"io"
	"time"
)

// ServerInfo holds server information gathered from first 2 log lines
type ServerInfo struct {
	Binary                   string    `json:"binary"`
	VersionShort             string    `json:"versionShort"`
	Version                  string    `json:"version"`
	VersionDescription       string    `json:"versionDescription"`
	TCPPort                  int       `json:"tcpPort"`
	UnixSocket               string    `json:"unixSocket"`
	CumBytes                 int       `json:"cumBytes"`
	CumLines                 int       `json:"cumLines"`
	QueryCount               int       `json:"queryCount"`
	UniqueQueries            int       `json:"uniqueQueries"`
	Start                    time.Time `json:"Start"`
	End                      time.Time `json:"End"`
	AnalysisStart            time.Time `json:"analysisStart"`
	AnalysisEnd              time.Time `json:"analysisEnd"`
	AnalysedLinesPerSecond   float64   `json:"analysedLinesPerSecond"`
	AnalysedQueriesPerSecond float64   `json:"analysedQueriesPerSecond"`
	AnalysedBytesPerSecond   float64   `json:"analysedBytesPerSecond"`
	AnalysisDuration         float64   `json:"analysisDuration"`
	// May be merge querystats here with:
	// Queries []QueryStats ?
}

// QueryStatsSlice holds a bunch of QueryStats
type QueryStatsSlice []*QueryStats

// QueryStats holds query statistics
type QueryStats struct {
	Hash            [32]byte  `json:"hash"`
	Schema          string    `json:"schema"`
	Count           int       `json:"count"`
	FingerPrint     string    `json:"fingerprint"`
	CumQueryTime    float64   `json:"cumQueryTime"`
	CumBytesSent    int       `json:"cumBytesSent"`
	CumLockTime     float64   `json:"cumLockTime"`
	CumRowsSent     int       `json:"cumRowsSent"`
	CumRowsExamined int       `json:"cumRowsExamined"`
	CumRowsAffected int       `json:"cumRowsAffected"`
	CumKilled       int       `json:"cumKilled"`
	CumErrored      int       `json:"cumErrored"`
	Concurrency     float64   `json:"concurrency"`
	QueryTime       []float64 `json:"queryTime"`
	BytesSent       []float64 `json:"bytesSent"`
	LockTime        []float64 `json:"lockTime"`
	RowsSent        []float64 `json:"rowsSent"`
	RowsExamined    []float64 `json:"rowsExamined"`
	RowsAffected    []float64 `json:"rowsAffected"`
}

// CacheInfo contains cache information
type CacheInfo struct {
	Server  ServerInfo      `json:"meta"`
	Queries QueryStatsSlice `json:"stats"`
}

// Outputs is a map containing name to function mapping
var Outputs = map[string]func(ServerInfo, QueryStatsSlice, io.Writer){}

// Add lets each output to add themselves in Outputs
func Add(name string, target func(ServerInfo, QueryStatsSlice, io.Writer)) {
	Outputs[name] = target
}
