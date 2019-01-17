package terminal

import (
	outputs "github.com/devops-works/dw-query-digest/outputs"
)

// Display show report in the terminal
func Display(servermeta outputs.ServerInfo, s outputs.QueryStatsSlice) {
}

func init() {
	outputs.Add("null", Display)
}
