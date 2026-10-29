package main

import "dataset-processor/internal/durable"

func recoverySummary(j durable.Job, chunks []durable.Chunk) map[string]any {
	c := []map[string]any{}
	for _, part := range chunks {
		c = append(c, map[string]any{"index": part.Index, "state": part.State, "attempts": part.Attempts, "until": part.Until})
	}
	return map[string]any{"id": j.ID, "state": j.State, "parts": j.Parts, "committed_chunks": j.Committed, "records": j.Total, "accepted": j.Accepted, "rejected": j.Rejected, "input_version": j.Input.Version, "input_sha256": j.Input.SHA256, "chunks": c}
}
