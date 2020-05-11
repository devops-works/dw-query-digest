package json

import (
	"encoding/json"
	"io"
	"time"

	log "github.com/sirupsen/logrus"

	outputs "gitlab.com/devopsworks/tools/dw-query-digest/outputs"
)

// Display show report in the terminal
func Display(servermeta outputs.ServerInfo, s outputs.QueryStatsSlice, w io.Writer) {
	c := outputs.CacheInfo{
		Server:  servermeta,
		Queries: s,
	}

	json, err := json.MarshalIndent(c, "", "\t")
	if err != nil {
		log.Fatal(err)
	}

	w.Write(json)
}

// fsecsToDuration converts float seconds to time.Duration
// Since we have float64 seconds durations
// We first convert to µs (* 1e6) then to duration
func fsecsToDuration(d float64) time.Duration {
	return time.Duration(d*1e6) * time.Microsecond
}

func init() {
	outputs.Add("json", Display)
}
