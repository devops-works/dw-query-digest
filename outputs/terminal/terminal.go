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
	fmt.Printf("\n# Server Info\n\n")
	fmt.Printf("  Binary             : %s\n", servermeta.Binary)
	fmt.Printf("  VersionShort       : %s\n", servermeta.VersionShort)
	fmt.Printf("  Version            : %s\n", servermeta.Version)
	fmt.Printf("  VersionDescription : %s\n", servermeta.VersionDescription)
	fmt.Printf("  TCPPort            : %d\n", servermeta.TCPPort)
	fmt.Printf("  UnixSocket         : %s\n", servermeta.UnixSocket)

	fmt.Printf("\n# Global Statistics\n\n")
	fmt.Printf("  Total queries      : %.3fM (%d)\n", float64(servermeta.QueryCount)/1000000.0, servermeta.QueryCount)
	fmt.Printf("  Total bytes        : %.3fM (%d)\n", float64(servermeta.CumBytes)/1000000.0, servermeta.CumBytes)
	fmt.Printf("  Total fingerprints : %d\n", servermeta.UniqueQueries)
	fmt.Printf("  Capture start      : %s\n", servermeta.StartTime)
	fmt.Printf("  Capture end        : %s\n", servermeta.EndTime)
	fmt.Printf("  Duration           : %s (%d s)\n", servermeta.EndTime.Sub(servermeta.StartTime), servermeta.EndTime.Sub(servermeta.StartTime)/time.Second)
	fmt.Printf("  QPS                : %.0f\n", float64(time.Second)*(float64(servermeta.QueryCount)/float64(servermeta.EndTime.Sub(servermeta.StartTime))))

	fmt.Printf("\n# Queries\n")

	ffactor := 100.0 * float64(time.Second) / float64(servermeta.EndTime.Sub(servermeta.StartTime))
	for idx, val := range s {
		val.Concurrency = val.CumQueryTime * ffactor
		sort.Float64s(val.QueryTime)
		fmt.Printf("\n# Query #%d: %x\n\n", idx+1, val.Hash[0:5])
		fmt.Printf("  Fingerprint     : %s\n", val.FingerPrint)
		fmt.Printf("  Schema          : %s\n", val.Schema)
		fmt.Printf("  Calls           : %d\n", val.Count)
		fmt.Printf("  CumErrored      : %d\n", val.CumErrored)
		fmt.Printf("  CumKilled       : %d\n", val.CumKilled)
		fmt.Printf("  CumQueryTime    : %s\n", fsecsToDuration(val.CumQueryTime))
		fmt.Printf("  CumLockTime     : %s\n", fsecsToDuration(val.CumLockTime))
		fmt.Printf("  CumRowsSent     : %d\n", val.CumRowsSent)
		fmt.Printf("  CumRowsExamined : %d\n", val.CumRowsExamined)
		fmt.Printf("  CumRowsAffected : %d\n", val.CumRowsAffected)
		fmt.Printf("  CumBytesSent    : %d\n", val.CumBytesSent)
		fmt.Printf("  Concurrency     : %2.2f%%\n", val.Concurrency)
		fmt.Printf("  min / max time  : %s / %s\n", fsecsToDuration(val.QueryTime[0]), fsecsToDuration(val.QueryTime[len(val.QueryTime)-1]))
		fmt.Printf("  mean time       : %s\n", fsecsToDuration(stat.Mean(val.QueryTime, nil)))
		fmt.Printf("  p50 time        : %s\n", fsecsToDuration(stat.Quantile(0.5, 1, val.QueryTime, nil)))
		fmt.Printf("  p95 time        : %s\n", fsecsToDuration(stat.Quantile(0.95, 1, val.QueryTime, nil)))
		fmt.Printf("  stddev time     : %s\n", fsecsToDuration(stat.StdDev(val.QueryTime, nil)))
		// fmt.Printf("\tmax time        : %.2f\n", stat.Max(0.95, 1, val.QueryTime, nil))

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
