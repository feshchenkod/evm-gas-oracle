package oracle

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

func Handler(store *Store, stale time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		origin := r.Header.Get("Origin")
		if origin == "" {
			origin = "*"
		}
		w.Header().Set("Vary", "Origin")
		w.Header().Set("Access-Control-Allow-Origin", origin)
		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,PATCH,DELETE,HEAD,OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", r.Header.Get("Access-Control-Request-Headers"))
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.URL.Path == "/livez" {
			write(w, r, 200, "application/json; charset=utf-8", []byte(`{"status":"ok"}`))
			return
		}
		snapshot := store.Snapshot(time.Now(), stale)
		if r.URL.Path == "/readyz" {
			status := 200
			if !snapshot.Ready {
				status = 503
			}
			body, _ := json.Marshal(struct {
				Status  string `json:"status"`
				Samples int    `json:"number_of_blocks"`
				Latest  uint64 `json:"latest_block_number"`
			}{snapshot.Reason, snapshot.Samples, snapshot.Latest})
			write(w, r, status, "application/json; charset=utf-8", body)
			return
		}
		// Existing clients can use any path/method, including /health. Preserve that API.
		if snapshot.Samples == 0 {
			// Compatibility errors clear middleware headers before returning text.
			w.Header().Del("Vary")
			w.Header().Del("Access-Control-Allow-Origin")
			write(w, r, 500, "text/plain; charset=utf-8", []byte(coldMessage))
			return
		}
		// Compatibility endpoints retain the last snapshot during RPC outages. /readyz
		// independently rejects errors/stale data, so operators can fail over safely.
		write(w, r, 200, "application/json; charset=utf-8", snapshot.Body)
	})
}
func write(w http.ResponseWriter, r *http.Request, status int, contentType string, body []byte) {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
}
