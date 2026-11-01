package oracle

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRPCRequestsAndFailures(t *testing.T) {
	good := block(7, 8, hash(7), "0xa")
	for _, scenario := range []string{"ok-number", "ok-hash", "http-error", "rpc-error", "null", "bad-json", "wrong-id", "wrong-version", "compressed", "too-large", "redirect", "wrong-hash", "wrong-height", "hash-only-transactions", "timeout"} {
		t.Run(scenario, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					ID     uint64            `json:"id"`
					Method string            `json:"method"`
					Params []json.RawMessage `json:"params"`
				}
				if r.Method != "POST" || r.Header.Get("Accept-Encoding") != "identity" {
					t.Error("unexpected RPC request")
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					return
				}
				if len(request.Params) != 2 || string(request.Params[1]) != "true" {
					t.Error("expected full transactions")
				}
				switch scenario {
				case "http-error":
					w.WriteHeader(503)
					return
				case "redirect":
					w.Header().Set("Location", "http://127.0.0.1:1/private")
					w.WriteHeader(302)
					return
				case "rpc-error":
					fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,"error":{"code":-32000,"message":"private token"}}`, request.ID)
					return
				case "null":
					fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,"result":null}`, request.ID)
					return
				case "bad-json":
					fmt.Fprint(w, "invalid")
					return
				case "too-large":
					fmt.Fprint(w, strings.Repeat(" ", 2048))
					return
				case "compressed":
					w.Header().Set("Content-Encoding", "br")
					fmt.Fprint(w, "not decoded")
					return
				case "hash-only-transactions":
					fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,"result":{"transactions":["0x123"]}}`, request.ID)
					return
				case "timeout":
					select {
					case <-r.Context().Done():
					case <-time.After(time.Second):
					}
					return
				}
				id := request.ID
				version := "2.0"
				b := good
				if scenario == "wrong-id" {
					id++
				}
				if scenario == "wrong-version" {
					version = "1.0"
				}
				if scenario == "wrong-hash" {
					b.Hash = hash(999)
				}
				if scenario == "wrong-height" {
					b.Number = "0x8"
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": version, "id": id, "result": b})
			}))
			defer server.Close()
			config := Config{RPCURL: server.URL + "/secret-token", RPCTimeout: 50 * time.Millisecond, MaxResponseBytes: 1024}
			client := NewRPCClient(config)
			defer client.Close()
			var err error
			if scenario == "ok-hash" || scenario == "wrong-hash" {
				_, err = client.ByHash(context.Background(), good.Hash)
			} else {
				_, err = client.ByNumber(context.Background(), "0x7")
			}
			if strings.HasPrefix(scenario, "ok-") {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("expected failure")
			} else if strings.Contains(err.Error(), "secret-token") || strings.Contains(err.Error(), "private token") {
				t.Fatal("error leaked RPC secrets")
			}
		})
	}
}
func TestPollingHTTPIntegration(t *testing.T) {
	blocks := chain(0, 3, 1000, hash(0), "0x0")
	for i := range blocks {
		blocks[i].Transactions = append(blocks[i].Transactions, Transaction{address(2), "0x2faf080"})
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID     uint64
			Method string
			Params []json.RawMessage
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		b := blocks[len(blocks)-1]
		if request.Method == "eth_getBlockByHash" {
			var target string
			_ = json.Unmarshal(request.Params[0], &target)
			for _, candidate := range blocks {
				if candidate.Hash == target {
					b = candidate
				}
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": b})
	}))
	defer upstream.Close()
	client := NewRPCClient(Config{RPCURL: upstream.URL, RPCTimeout: time.Second, MaxResponseBytes: 1 << 20})
	defer client.Close()
	store := NewStore()
	p := Poller{Source: client, Store: store, Policy: "ignore"}
	if err := p.Sync(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(Handler(store, time.Minute))
	defer server.Close()
	res, err := http.Get(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var body map[string]any
	if err = json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != 200 || body["number_of_blocks"] != float64(4) || body["percentile_50"] != "0.05 nanoeth" {
		t.Fatal(body)
	}
}
