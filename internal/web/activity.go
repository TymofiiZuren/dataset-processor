package web

import "dataset-processor/internal/durable"

type activityItem struct {
	Revision int64  `json:"revision"`
	Action   string `json:"action"`
	Stage    string `json:"stage"`
	At       string `json:"at"`
}
type activityView struct {
	Events    []activityItem `json:"events"`
	Truncated bool           `json:"truncated"`
}

func activitySummary(a durable.Activity) activityView {
	v := activityView{Events: []activityItem{}, Truncated: a.Truncated}
	for _, e := range a.Events {
		v.Events = append(v.Events, activityItem{Revision: e.Revision, Action: activityAction(e.Code), Stage: stage(e.State), At: e.At.UTC().Format("2006-01-02 15:04:05 MST")})
	}
	return v
}
func activityAction(code string) string {
	switch code {
	case "input_pinned":
		return "Input version verified"
	case "storage_reserved":
		return "Output storage reserved"
	case "stage_claimed":
		return "Coordinator claimed a processing stage"
	case "stage_renewed":
		return "Coordinator lease renewed"
	case "preparation_selected":
		return "Record preparation completed"
	case "chunk_claimed":
		return "Worker claimed a chunk"
	case "chunk_committed":
		return "Chunk output committed"
	case "result_published":
		return "Integrity checks passed; result published"
	case "cancelled":
		return "Cancellation confirmed"
	default:
		return "Processing state updated"
	}
}
