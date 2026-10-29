package web

import "net/http"

type historyItem struct {
	ID, Stage, Updated string
	Accepted, Rejected int64
	Committed, Parts   int
}

func (s *Server) home(w http.ResponseWriter, r *http.Request, csrf string) {
	v := view{SignedIn: true, CSRF: csrf}
	if s.runtime != nil {
		overview, err := s.runtime.Store.Overview(r.Context(), s.runtime.Owner)
		if err != nil {
			v.OverviewError = true
			w.Header().Set("Retry-After", "5")
		} else {
			v.Overview = &overview
		}
		jobs, e := s.runtime.Store.RecentJobs(r.Context(), s.runtime.Owner, 20)
		if e != nil {
			v.HistoryError = true
			w.Header().Set("Retry-After", "5")
		} else {
			for _, j := range jobs {
				v.History = append(v.History, historyItem{ID: j.ID, Stage: stage(j.State), Updated: j.Updated.UTC().Format("2006-01-02 15:04:05 MST"), Accepted: j.Accepted, Rejected: j.Rejected, Committed: j.Committed, Parts: j.Parts})
			}
		}
	}
	status := http.StatusOK
	if v.HistoryError || v.OverviewError {
		status = http.StatusServiceUnavailable
	}
	s.render(w, status, v)
}
