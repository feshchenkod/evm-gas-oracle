package oracle

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRPCPacingAndSingleFlight(t *testing.T) {
	var active, maxActive atomic.Int64
	var mu sync.Mutex
	var starts []time.Time
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for previous := maxActive.Load(); n > previous; previous = maxActive.Load() {
			if maxActive.CompareAndSwap(previous, n) {
				break
			}
		}
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		var req struct{ ID uint64 }
		_ = json.NewDecoder(r.Body).Decode(&req)
		time.Sleep(10 * time.Millisecond)
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": block(1, 2, hash(1), "0xa")})
	}))
	defer server.Close()
	client := NewRPCClient(Config{RPCURL: server.URL, RPCTimeout: time.Second, MaxResponseBytes: 1 << 20, MaxRequestsPerSecond: 20})
	defer client.Close()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := client.ByNumber(context.Background(), "latest"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if maxActive.Load() != 1 {
		t.Fatal("concurrent RPC requests", maxActive.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	for i := 1; i < len(starts); i++ {
		if starts[i].Sub(starts[i-1]) < 40*time.Millisecond {
			t.Fatal("RPC requests were not paced", starts)
		}
	}
}
func TestRPCCancelWhileQueuedOrPaced(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var count atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		close(entered)
		<-release
		var req struct{ ID uint64 }
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": block(1, 2, hash(1), "0xa")})
	}))
	defer server.Close()
	client := NewRPCClient(Config{RPCURL: server.URL, RPCTimeout: time.Second, MaxResponseBytes: 1 << 20, MaxRequestsPerSecond: 1})
	defer client.Close()
	done := make(chan error, 1)
	go func() { _, err := client.ByNumber(context.Background(), "latest"); done <- err }()
	<-entered
	checkCanceled := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		start := time.Now()
		if _, err := client.ByNumber(ctx, "latest"); err == nil {
			t.Fatal("cancellation ignored")
		}
		if time.Since(start) > 500*time.Millisecond {
			t.Fatal("cancel waited for RPC gate")
		}
	}
	checkCanceled()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	checkCanceled()
	if count.Load() != 1 {
		t.Fatal("canceled requests reached node", count.Load())
	}
}
func TestKnownParentIsNotRefetched(t *testing.T) {
	initial := chain(0, 1, 100, hash(0), "0xa")
	source := sourceFor(initial)
	store := NewStore()
	poller := Poller{Source: source, Store: store, Policy: "skip-block"}
	if err := poller.Sync(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	calls := source.calls
	source.head = block(2, 102, initial[1].Hash, "0xb")
	if err := poller.Sync(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if source.calls != calls {
		t.Fatal("already-known parent requested again")
	}
}

// A stalled upstream must not hold the snapshot lock or any HTTP worker budget.
type stalledSource struct {
	head    Block
	entered chan struct{}
	release chan struct{}
}

func (s *stalledSource) ByNumber(ctx context.Context, _ string) (Block, error) {
	close(s.entered)
	select {
	case <-ctx.Done():
		return Block{}, ctx.Err()
	case <-s.release:
		return s.head, nil
	}
}
func (s *stalledSource) ByHash(context.Context, string) (Block, error) {
	return Block{}, fmt.Errorf("known parent should not be fetched")
}
func TestHTTPReadersContinueWhileRPCIsBlocked(t *testing.T) {
	store := NewStore()
	initial := block(1, 2, hash(1), "0xa")
	store.commit(-1, records([]Block{initial}, "skip-block"), time.Now())
	source := &stalledSource{head: block(2, 3, initial.Hash, "0x14"), entered: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	poller := Poller{Source: source, Store: store, Policy: "skip-block"}
	go func() { done <- poller.Sync(ctx, time.Now()) }()
	<-source.entered
	server := httptest.NewServer(Handler(store, time.Minute))
	defer server.Close()
	client := &http.Client{Timeout: time.Second}
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			response, err := client.Get(server.URL + "/")
			if err != nil {
				t.Error(err)
				return
			}
			defer response.Body.Close()
			var body map[string]any
			if err = json.NewDecoder(response.Body).Decode(&body); err != nil || response.StatusCode != 200 || body["latest_block_number"] != float64(1) {
				t.Error("HTTP blocked on RPC or observed partial snapshot", body, err)
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-done:
		t.Fatal("RPC finished before release", err)
	default:
	}
	close(source.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if store.Snapshot(time.Now(), time.Minute).Latest != 2 {
		t.Fatal("poll did not commit after RPC resumed")
	}
}
