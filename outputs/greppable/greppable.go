package greppable

import (
	"fmt"
	"io"
	"sort"
	"time"

	"gonum.org/v1/gonum/stat"

	outputs "gitlab.com/devopsworks/tools/dw-query-digest/outputs"
)

// Display show report in the terminal
func Display(servermeta outputs.ServerInfo, s outputs.QueryStatsSlice, w io.Writer) {
	fmt.Fprintf(w, "# Binary:%s;", servermeta.Binary)
	fmt.Fprintf(w, "VersionShort:%s;", servermeta.VersionShort)
	fmt.Fprintf(w, "Version:%s;", servermeta.Version)
	fmt.Fprintf(w, "VersionDescription:%s;", servermeta.VersionDescription)
	fmt.Fprintf(w, "TCPPort:%d;", servermeta.TCPPort)
	fmt.Fprintf(w, "UnixSocket:%s;", servermeta.UnixSocket)
	fmt.Fprintf(w, "Total queries:%.3fM (%d);", float64(servermeta.QueryCount)/1000000.0, servermeta.QueryCount)
	fmt.Fprintf(w, "Total bytes:%.3fM (%d);", float64(servermeta.CumBytes)/1000000.0, servermeta.CumBytes)
	fmt.Fprintf(w, "Total fingerprints:%d;", servermeta.UniqueQueries)
	fmt.Fprintf(w, "Capture start:%s;", servermeta.Start)
	fmt.Fprintf(w, "Capture end:%s;", servermeta.End)
	fmt.Fprintf(w, "Duration:%s (%d s);", servermeta.End.Sub(servermeta.Start), servermeta.End.Sub(servermeta.Start)/time.Second)
	fmt.Fprintf(w, "QPS:%.0f\n", float64(time.Second)*(float64(servermeta.QueryCount)/float64(servermeta.End.Sub(servermeta.Start))))

	fmt.Fprintf(w, "# 1_Pos;2_QueryID;3_Fingerprint;4_Schema;5_Calls;")
	fmt.Fprintf(w, "6_CumErrored;7_CumKilled;8_CumQueryTime(s);9_CumLockTime(s);10_CumRowsSent;")
	fmt.Fprintf(w, "11_CumRowsExamined;12_CumRowsAffected;13_CumBytesSent;14_Concurency(%%);15_Min(s);16_Max(s);")
	fmt.Fprintf(w, "17_Mean(s);18_P50(s);19_P95(s);20_StdDev(s)\n")

	ffactor := 100.0 * float64(time.Second) / float64(servermeta.End.Sub(servermeta.Start))
	for idx, val := range s {
		val.Concurrency = val.CumQueryTime * ffactor
		sort.Float64s(val.QueryTime)

		// We need %s%s since val.FingerPrint comes with a ';' at the end
		fmt.Fprintf(w, "%d;%x;%s%s;%d;", idx+1, val.Hash[0:5], val.FingerPrint, val.Schema, val.Count)
		fmt.Fprintf(w, "%d;%d;%f;%f;%d;", val.CumErrored, val.CumKilled, val.CumQueryTime, val.CumLockTime, val.CumRowsSent)
		fmt.Fprintf(w, "%d;%d;%d;%2.2f%%;%f;%f;", val.CumRowsExamined, val.CumRowsAffected, val.CumBytesSent, val.Concurrency, val.QueryTime[0], val.QueryTime[len(val.QueryTime)-1])
		fmt.Fprintf(w, "%f;%f;", stat.Mean(val.QueryTime, nil), stat.Quantile(0.5, 1, val.QueryTime, nil))
		fmt.Fprintf(w, "%f;%f\n", stat.Quantile(0.95, 1, val.QueryTime, nil), stat.StdDev(val.QueryTime, nil))
	}

}

// fsecsToDuration converts float seconds to time.Duration
// Since we have float64 seconds durations
// We first convert to µs (* 1e6) then to duration
// func fsecsToDuration(d float64) time.Duration {
// 	return time.Duration(d*1e6) * time.Microsecond
// }

func init() {
	outputs.Add("greppable", Display)
}
