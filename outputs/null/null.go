package null

import (
	"io"

	outputs "github.com/devops-works/dw-query-digest/outputs"
)

// Display show report in the terminal
func Display(outputs.ServerInfo, outputs.QueryStatsSlice, io.Writer) {
	// This is empty since we do not have to do anything
}

func init() {
	outputs.Add("null", Display)
}
