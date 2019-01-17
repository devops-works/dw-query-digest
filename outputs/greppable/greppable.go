package greppable

import (
	"fmt"
	"sort"
	"time"

	"gonum.org/v1/gonum/stat"

	outputs "github.com/devops-works/dw-query-digest/outputs"
)

// Display show report in the terminal
func Display(servermeta outputs.ServerInfo, s outputs.QueryStatsSlice) {
	fmt.Printf("# Binary:%s;", servermeta.Binary)
	fmt.Printf("VersionShort:%s;", servermeta.VersionShort)
	fmt.Printf("Version:%s;", servermeta.Version)
	fmt.Printf("VersionDescription:%s;", servermeta.VersionDescription)
	fmt.Printf("TCPPort:%d;", servermeta.TCPPort)
	fmt.Printf("UnixSocket:%s;", servermeta.UnixSocket)
	fmt.Printf("Total queries:%.3fM (%d);", float64(servermeta.QueryCount)/1000000.0, servermeta.QueryCount)
	fmt.Printf("Total bytes:%.3fM (%d);", float64(servermeta.CumBytes)/1000000.0, servermeta.CumBytes)
	fmt.Printf("Total fingerprints:%d;", servermeta.UniqueQueries)
	fmt.Printf("Capture start:%s;", servermeta.StartTime)
	fmt.Printf("Capture end:%s;", servermeta.EndTime)
	fmt.Printf("Duration:%s (%d s);", servermeta.EndTime.Sub(servermeta.StartTime), servermeta.EndTime.Sub(servermeta.StartTime)/time.Second)
	fmt.Printf("QPS:%.0f\n", float64(time.Second)*(float64(servermeta.QueryCount)/float64(servermeta.EndTime.Sub(servermeta.StartTime))))

	fmt.Printf("# 1_Pos;2_QueryID;3_Fingerprint;4_Schema;5_Calls;")
	fmt.Printf("6_CumErrored;7_CumKilled;8_CumQueryTime(s);9_CumLockTime(s);10_CumRowsSent;")
	fmt.Printf("11_CumRowsExamined;12_CumRowsAffected;13_CumBytesSent;14_Concurency(%%);15_Min(s);16_Max(s);")
	fmt.Printf("17_Mean(s);18_P50(s);19_P95(s);20_StdDev(s)\n")

	ffactor := 100.0 * float64(time.Second) / float64(servermeta.EndTime.Sub(servermeta.StartTime))
	for idx, val := range s {
		val.Concurrency = val.CumQueryTime * ffactor
		sort.Float64s(val.QueryTime)

		// We need %s%s since val.FingerPrint comes with a ';' at the end
		fmt.Printf("%d;%x;%s%s;%d;", idx+1, val.Hash[0:5], val.FingerPrint, val.Schema, val.Count)
		fmt.Printf("%d;%d;%f;%f;%d;", val.CumErrored, val.CumKilled, val.CumQueryTime, val.CumLockTime, val.CumRowsSent)
		fmt.Printf("%d;%d;%d;%2.2f%%;%f;%f;", val.CumRowsExamined, val.CumRowsAffected, val.CumBytesSent, val.Concurrency, val.QueryTime[0], val.QueryTime[len(val.QueryTime)-1])
		fmt.Printf("%f;%f;", stat.Mean(val.QueryTime, nil), stat.Quantile(0.5, 1, val.QueryTime, nil))
		fmt.Printf("%f;%f\n", stat.Quantile(0.95, 1, val.QueryTime, nil), stat.StdDev(val.QueryTime, nil))
	}

}

// fsecsToDuration converts float seconds to time.Duration
// Since we have float64 seconds durations
// We first convert to µs (* 1e6) then to duration
func fsecsToDuration(d float64) time.Duration {
	return time.Duration(d*1e6) * time.Microsecond
}

func init() {
	outputs.Add("greppable", Display)
}
