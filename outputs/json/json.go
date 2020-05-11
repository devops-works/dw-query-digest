package json

import (
	"encoding/json"
	"io"

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
		log.Errorf("unable to marshal JSON: %v", err)
	}

	_, err = w.Write(json)
	if err != nil {
		log.Errorf("unable to write JSON: %v", err)
	}
}

// fsecsToDuration converts float seconds to time.Duration
// Since we have float64 seconds durations
// We first convert to µs (* 1e6) then to duration
// func fsecsToDuration(d float64) time.Duration {
// 	return time.Duration(d*1e6) * time.Microsecond
// }

func init() {
	outputs.Add("json", Display)
}
