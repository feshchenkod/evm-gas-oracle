// Local-only fixture for the default-port Docker smoke test.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"
)

func main() {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID     uint64
			Method string
			Params []json.RawMessage
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&request); err != nil {
			http.Error(w, "bad request", 400)
			return
		}
		n := uint64(2)
		if request.Method == "eth_getBlockByHash" {
			if len(request.Params) < 1 {
				http.Error(w, "missing params", 400)
				return
			}
			var hash string
			if err := json.Unmarshal(request.Params[0], &hash); err != nil || len(hash) != 66 {
				http.Error(w, "bad hash", 400)
				return
			}
			id, err := strconv.ParseUint(hash[2:], 16, 64)
			if err != nil || id < 1 || id > 3 {
				http.Error(w, "bad block", 400)
				return
			}
			n = id - 1
		}
		b := map[string]any{"number": fmt.Sprintf("0x%x", n), "hash": fmt.Sprintf("0x%064x", n+1), "parentHash": fmt.Sprintf("0x%064x", n), "miner": "0x1111111111111111111111111111111111111111", "transactions": []map[string]string{{"from": "0x2222222222222222222222222222222222222222", "gasPrice": "0x0"}, {"from": "0x2222222222222222222222222222222222222222", "gasPrice": "0x2faf080"}}}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": b})
	})
	server := http.Server{Addr: ":18545", Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second}
	log.Fatal(server.ListenAndServe())
}
