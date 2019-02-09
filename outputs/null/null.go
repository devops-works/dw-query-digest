package null

import (
	"io"

	outputs "github.com/devops-works/dw-query-digest/outputs"
)

// Display show report in the terminal
func Display(servermeta outputs.ServerInfo, s outputs.QueryStatsSlice, w io.Writer) {
}

func init() {
	outputs.Add("null", Display)
}
