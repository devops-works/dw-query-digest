package terminal

import (
	"fmt"
	"io"
	"sort"
	"time"

	"gonum.org/v1/gonum/stat"

	outputs "github.com/devops-works/dw-query-digest/outputs"
)

// Display show report in the terminal
func Display(servermeta outputs.ServerInfo, s outputs.QueryStatsSlice, w io.Writer) {
	fmt.Fprintf(w, "\n# Server Info\n\n")
	fmt.Fprintf(w, "  Binary             : %s\n", servermeta.Binary)
	fmt.Fprintf(w, "  VersionShort       : %s\n", servermeta.VersionShort)
	fmt.Fprintf(w, "  Version            : %s\n", servermeta.Version)
	fmt.Fprintf(w, "  VersionDescription : %s\n", servermeta.VersionDescription)
	fmt.Fprintf(w, "  TCPPort            : %d\n", servermeta.TCPPort)
	fmt.Fprintf(w, "  UnixSocket         : %s\n", servermeta.UnixSocket)

	fmt.Fprintf(w, "\n# Internal Analyzer Statistics\n\n")
	// fmt.Fprintf(w, "  Start     : %s\n", servermeta.AnalysisStart)
	fmt.Fprintf(w, "  Duration  : %14.3fs\n", servermeta.AnalysisDuration)
	fmt.Fprintf(w, "  Log lines : %14.3fM (%d)\n", float64(servermeta.CumLines)/1000000.0, servermeta.CumLines)
	fmt.Fprintf(w, "  Lines/s   : %14.3f\n", servermeta.AnalysedLinesPerSecond)
	fmt.Fprintf(w, "  Bytes/s   : %14.3f\n", servermeta.AnalysedBytesPerSecond)
	fmt.Fprintf(w, "  Queries/s : %14.3f\n", servermeta.AnalysedQueriesPerSecond)

	fmt.Fprintf(w, "\n# Global Statistics\n\n")
	fmt.Fprintf(w, "  Total queries      : %.3fM (%d)\n", float64(servermeta.QueryCount)/1000000.0, servermeta.QueryCount)
	fmt.Fprintf(w, "  Total bytes        : %.3fM (%d)\n", float64(servermeta.CumBytes)/1000000.0, servermeta.CumBytes)
	fmt.Fprintf(w, "  Total fingerprints : %d\n", servermeta.UniqueQueries)
	fmt.Fprintf(w, "  Capture start      : %s\n", servermeta.Start)
	fmt.Fprintf(w, "  Capture end        : %s\n", servermeta.End)
	fmt.Fprintf(w, "  Duration           : %s (%d s)\n", servermeta.End.Sub(servermeta.Start), servermeta.End.Sub(servermeta.Start)/time.Second)
	fmt.Fprintf(w, "  QPS                : %.0f\n", float64(time.Second)*(float64(servermeta.QueryCount)/float64(servermeta.End.Sub(servermeta.Start))))

	fmt.Fprintf(w, "\n# Queries\n")

	ffactor := 100.0 * float64(time.Second) / float64(servermeta.End.Sub(servermeta.Start))
	for idx, val := range s {
		val.Concurrency = val.CumQueryTime * ffactor
		sort.Float64s(val.QueryTime)
		fmt.Fprintf(w, "\n# Query #%d: %x\n\n", idx+1, val.Hash[0:5])
		fmt.Fprintf(w, "  Fingerprint     : %s\n", val.FingerPrint)
		fmt.Fprintf(w, "  Schema          : %s\n", val.Schema)
		fmt.Fprintf(w, "  Calls           : %d\n", val.Count)
		fmt.Fprintf(w, "  CumErrored      : %d\n", val.CumErrored)
		fmt.Fprintf(w, "  CumKilled       : %d\n", val.CumKilled)
		fmt.Fprintf(w, "  CumQueryTime    : %s\n", fsecsToDuration(val.CumQueryTime))
		fmt.Fprintf(w, "  CumLockTime     : %s\n", fsecsToDuration(val.CumLockTime))
		fmt.Fprintf(w, "  CumRowsSent     : %d\n", val.CumRowsSent)
		fmt.Fprintf(w, "  CumRowsExamined : %d\n", val.CumRowsExamined)
		fmt.Fprintf(w, "  CumRowsAffected : %d\n", val.CumRowsAffected)
		fmt.Fprintf(w, "  CumBytesSent    : %d\n", val.CumBytesSent)
		fmt.Fprintf(w, "  Concurrency     : %2.2f%%\n", val.Concurrency)
		fmt.Fprintf(w, "  min / max time  : %s / %s\n", fsecsToDuration(val.QueryTime[0]), fsecsToDuration(val.QueryTime[len(val.QueryTime)-1]))
		fmt.Fprintf(w, "  mean time       : %s\n", fsecsToDuration(stat.Mean(val.QueryTime, nil)))
		fmt.Fprintf(w, "  p50 time        : %s\n", fsecsToDuration(stat.Quantile(0.5, 1, val.QueryTime, nil)))
		fmt.Fprintf(w, "  p95 time        : %s\n", fsecsToDuration(stat.Quantile(0.95, 1, val.QueryTime, nil)))
		fmt.Fprintf(w, "  stddev time     : %s\n", fsecsToDuration(stat.StdDev(val.QueryTime, nil)))
		// fmt.Fprintf(w, "\tmax time        : %.2f\n", stat.Max(0.95, 1, val.QueryTime, nil))

	}

}

// fsecsToDuration converts float seconds to time.Duration
// Since we have float64 seconds durations
// We first convert to µs (* 1e6) then to duration
func fsecsToDuration(d float64) time.Duration {
	return time.Duration(d*1e6) * time.Microsecond
}

func init() {
	outputs.Add("terminal", Display)
}
