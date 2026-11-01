package oracle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func hash(n int) string    { return fmt.Sprintf("0x%064x", n) }
func address(n int) string { return fmt.Sprintf("0x%040x", n) }
func block(n int, id int, parent string, prices ...string) Block {
	b := Block{Number: fmt.Sprintf("0x%x", n), Hash: hash(id), ParentHash: parent, Miner: address(1), Transactions: []Transaction{}}
	for _, p := range prices {
		b.Transactions = append(b.Transactions, Transaction{address(2), p})
	}
	return b
}
func records(blocks []Block, policy string) []record {
	out := make([]record, 0, len(blocks))
	for _, b := range blocks {
		n, _ := b.Height()
		out = append(out, record{b.Hash, b.ParentHash, n, priceForBlock(b, policy)})
	}
	return out
}
func TestCompatibilityFixtures(t *testing.T) {
	data, err := os.ReadFile("testdata/compat.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct {
		Name     string
		Blocks   [][]string
		Response json.RawMessage
	}
	if err = json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range fixtures {
		t.Run(fixture.Name, func(t *testing.T) {
			store := NewStore()
			var blocks []Block
			for i, prices := range fixture.Blocks {
				blocks = append(blocks, block(i, i+1, hash(i), prices...))
			}
			store.commit(-1, records(blocks, "skip-block"), time.Now())
			got := store.Snapshot(time.Now(), time.Hour)
			var expected bytes.Buffer
			if err = json.Compact(&expected, fixture.Response); err != nil {
				t.Fatal(err)
			}
			if string(got.Body) != expected.String() {
				t.Fatalf("contract mismatch\ngot  %s\nwant %s", got.Body, expected.Bytes())
			}
		})
	}
}
func TestZeroPolicyAndMiner(t *testing.T) {
	b := block(1, 2, hash(1), "0x0", "0x2faf080")
	if priceForBlock(b, "skip-block") != nil {
		t.Fatal("compatibility mode must skip the entire zero-price block")
	}
	if p := priceForBlock(b, "ignore"); p == nil || *p != 50000000 {
		t.Fatal("ignore mode lost positive price")
	}
	b.Transactions = []Transaction{{address(1), "0x0"}, {address(2), "0xa"}}
	if p := priceForBlock(b, "skip-block"); p == nil || *p != 10 {
		t.Fatal("miner exclusion")
	}
	for _, prices := range [][]string{{}, {"0x0"}, {"invalid"}, {"0x-1"}, {"0x"}, {"0x" + strings.Repeat("f", 65)}} {
		if priceForBlock(block(1, 2, hash(1), prices...), "ignore") != nil {
			t.Fatalf("accepted invalid/no positive samples: %v", prices)
		}
	}
}
func TestConfig(t *testing.T) {
	cfg, err := LoadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != ":80" || cfg.PollInterval != 5*time.Second || cfg.ZeroPolicy != "skip-block" {
		t.Fatal(cfg)
	}
	if cfg, err := LoadConfig(func(k string) string {
		if k == "POLLING_FREQUENCY_SECONDS" {
			return "3600"
		}
		return ""
	}); err != nil || cfg.StaleAfter < cfg.PollInterval {
		t.Fatal("compatible long polling interval requires no extra variables", cfg, err)
	}
	for _, pair := range [][2]string{{"ETHEREUM_URL", "file:///tmp/rpc"}, {"ETHEREUM_URL", "https://"}, {"POLLING_FREQUENCY_SECONDS", "0"}, {"POLLING_FREQUENCY_SECONDS", "1.5"}, {"POLLING_FREQUENCY_SECONDS", "3601"}, {"ZERO_GAS_PRICE_POLICY", "include"}, {"RPC_TIMEOUT", "0s"}, {"STALE_AFTER", "1s"}, {"RPC_MAX_RESPONSE_BYTES", "999999999"}, {"HTTP_ADDR", "80"}} {
		_, err := LoadConfig(func(k string) string {
			if k == pair[0] {
				return pair[1]
			}
			return ""
		})
		if err == nil {
			t.Fatalf("accepted %v", pair)
		}
	}
}

type fakeSource struct {
	polls  int
	head   Block
	byHash map[string]Block
	fail   string
	calls  int
}

func (f *fakeSource) ByNumber(context.Context, string) (Block, error) { f.polls++; return f.head, nil }
func (f *fakeSource) ByHash(_ context.Context, h string) (Block, error) {
	f.calls++
	if h == f.fail {
		return Block{}, fmt.Errorf("temporary RPC failure")
	}
	b, ok := f.byHash[h]
	if !ok {
		return Block{}, fmt.Errorf("missing parent")
	}
	return b, nil
}
func chain(start, end, offset int, parent string, price string) []Block {
	var out []Block
	for n := start; n <= end; n++ {
		b := block(n, offset+n, parent, price)
		out = append(out, b)
		parent = b.Hash
	}
	return out
}
func sourceFor(blocks []Block) *fakeSource {
	f := &fakeSource{head: blocks[len(blocks)-1], byHash: map[string]Block{}}
	for _, b := range blocks {
		f.byHash[b.Hash] = b
	}
	return f
}
func TestBootstrapCatchupAndUnchangedHead(t *testing.T) {
	blocks := chain(0, 60, 100, hash(0), "0xa")
	f := sourceFor(blocks)
	store := NewStore()
	p := Poller{Source: f, Store: store, Policy: "skip-block"}
	now := time.Now()
	if err := p.Sync(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if got := store.Snapshot(now, time.Minute); got.Samples != 51 || got.Latest != 60 || !got.Ready {
		t.Fatal(got)
	}
	more := chain(61, 65, 100, blocks[len(blocks)-1].Hash, "0xb")
	for _, b := range more {
		f.byHash[b.Hash] = b
	}
	f.head = more[len(more)-1]
	if err := p.Sync(context.Background(), now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := store.Snapshot(now.Add(time.Second), time.Minute); got.Samples != 56 || got.Latest != 65 {
		t.Fatal(got)
	}
	calls := f.calls
	if err := p.Sync(context.Background(), now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if calls != f.calls {
		t.Fatal("unchanged head fetched parents")
	}
	if got := store.Snapshot(now.Add(2*time.Minute), time.Minute); got.Ready || got.Reason != "rpc_stale" {
		t.Fatal(got)
	}
}
func TestReorgAcrossSkippedBlocksRestoresEvictedSamples(t *testing.T) {
	original := chain(0, 250, 1000, hash(0), "0xa")
	f := sourceFor(original)
	store := NewStore()
	store.commit(-1, records(original, "skip-block"), time.Now())
	// The orphan branch contained paid blocks, but the replacement does not.
	replacement := chain(246, 252, 9000, original[245].Hash, "0x0")
	for _, b := range replacement {
		f.byHash[b.Hash] = b
	}
	f.head = replacement[len(replacement)-1]
	p := Poller{Source: f, Store: store, Policy: "skip-block"}
	if err := p.Sync(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	got := store.Snapshot(time.Now(), time.Hour)
	if got.Samples != 200 || got.Latest != 252 {
		t.Fatal("lost paid history during reorg", got)
	}
	// Another replacement shares an unsampled ancestor, which must still be retained.
	second := chain(251, 253, 20000, replacement[4].Hash, "0x14")
	for _, b := range second {
		f.byHash[b.Hash] = b
	}
	f.head = second[len(second)-1]
	if err := p.Sync(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if got = store.Snapshot(time.Now(), time.Hour); got.Samples != 200 || got.Latest != 253 {
		t.Fatal(got)
	}
}
func TestFailedReorgIsAtomicAndRecovers(t *testing.T) {
	original := chain(0, 10, 100, hash(0), "0xa")
	f := sourceFor(original)
	store := NewStore()
	p := Poller{Source: f, Store: store, Policy: "skip-block"}
	if err := p.Sync(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	before := string(store.Snapshot(time.Now(), time.Hour).Body)
	fork := chain(8, 12, 200, original[7].Hash, "0x14")
	for _, b := range fork {
		f.byHash[b.Hash] = b
	}
	f.head = fork[len(fork)-1]
	f.fail = fork[1].Hash
	if err := p.Sync(context.Background(), time.Now()); err == nil {
		t.Fatal("expected RPC failure")
	}
	if got := store.Snapshot(time.Now(), time.Hour); string(got.Body) != before || got.Ready || got.Reason != "rpc_error" {
		t.Fatal(got)
	}
	f.fail = ""
	if err := p.Sync(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if got := store.Snapshot(time.Now(), time.Hour); !got.Ready || got.Latest != 12 {
		t.Fatal(got)
	}
	f.head = original[3]
	if err := p.Sync(context.Background(), time.Now()); err == nil {
		t.Fatal("accepted lagging head")
	}
	if string(store.Snapshot(time.Now(), time.Hour).Body) == before {
		t.Fatal("rolled back to lagging head")
	}
}
func TestDeepReorgAndBoundedState(t *testing.T) {
	original := chain(0, 600, 1000, hash(0), "0xa")
	store := NewStore()
	store.commit(-1, records(original, "ignore"), time.Now())
	fork := chain(0, 601, 2000, hash(0), "0x14")
	f := sourceFor(fork)
	p := Poller{Source: f, Store: store, Policy: "ignore"}
	if err := p.Sync(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if len(store.records()) != 500 || len(store.samples) != 200 || f.calls != 499 {
		t.Fatalf("bounds: records=%d samples=%d calls=%d", len(store.records()), len(store.samples), f.calls)
	}
	if got := store.Snapshot(time.Now(), time.Hour); got.Latest != 601 || !bytes.Contains(got.Body, []byte(`"percentile_100":"2e-8 nanoeth"`)) {
		t.Fatal(got)
	}
}
func TestRetainSparseSamplesBeyondAncestryRing(t *testing.T) {
	blocks := chain(0, 199, 1000, hash(0), "0xa")
	store := NewStore()
	store.commit(-1, records(blocks, "skip-block"), time.Now())
	for n := 200; n < 1000; n++ {
		old := store.records()
		b := block(n, 1000+n, old[len(old)-1].Hash, "0x0")
		store.commit(len(old)-1, records([]Block{b}, "skip-block"), time.Now())
	}
	if len(store.records()) != 500 || len(store.samples) != 200 {
		t.Fatal("sparse samples were discarded with ancestry")
	}
}
func TestHTTPContract(t *testing.T) {
	store := NewStore()
	handler := Handler(store, time.Minute)
	request := func(method, path string, headers map[string]string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, nil)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		return w
	}
	if w := request("GET", "/", nil); w.Code != 500 || w.Body.String() != coldMessage || w.Header().Get("Content-Type") != "text/plain; charset=utf-8" {
		t.Fatal(w)
	}
	if w := request("HEAD", "/", map[string]string{"Origin": "https://client.example"}); w.Code != 500 || w.Body.Len() != 0 || w.Header().Get("Access-Control-Allow-Origin") != "" || w.Header().Get("Vary") != "" || w.Header().Get("Content-Length") != "61" {
		t.Fatal("cold HEAD/error headers changed", w)
	}
	if w := request("GET", "/livez", nil); w.Code != 200 {
		t.Fatal(w)
	}
	if w := request("GET", "/readyz", nil); w.Code != 503 {
		t.Fatal(w)
	}
	store.commit(-1, records([]Block{block(1, 2, hash(1), "0xa")}, "skip-block"), time.Now())
	root := request("GET", "/", nil)
	for _, method := range []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"} {
		w := request(method, "/anything?legacy=query", map[string]string{"Origin": "https://client.example"})
		if w.Code != 200 || w.Header().Get("Content-Length") != root.Header().Get("Content-Length") || w.Header().Get("Access-Control-Allow-Origin") != "https://client.example" || w.Header().Get("Vary") != "Origin" {
			t.Fatal(method, w)
		}
		if method == "HEAD" {
			if w.Body.Len() != 0 {
				t.Fatal("HEAD body")
			}
		} else if w.Body.String() != root.Body.String() {
			t.Fatal("catch-all payload changed")
		}
	}
	w := request("OPTIONS", "/", map[string]string{"Origin": "https://client.example", "Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "X-Test"})
	if w.Code != 204 || w.Body.Len() != 0 || w.Header().Get("Access-Control-Allow-Methods") != "GET,PUT,POST,PATCH,DELETE,HEAD,OPTIONS" || w.Header().Get("Access-Control-Allow-Headers") != "X-Test" {
		t.Fatal(w)
	}
	store.markFailed()
	if request("GET", "/", nil).Code != 200 || request("GET", "/readyz", nil).Code != 503 {
		t.Fatal("outage compatibility/readiness")
	}
}
func TestStalledHead(t *testing.T) {
	store := NewStore()
	now := time.Now()
	store.commit(-1, records([]Block{block(1, 2, hash(1), "0xa")}, "skip-block"), now.Add(-time.Hour))
	store.markSuccess(now)
	if got := store.Snapshot(now, time.Minute); got.Ready || got.Reason != "head_stale" {
		t.Fatal(got)
	}
}
func TestConcurrentSnapshot(t *testing.T) {
	store := NewStore()
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				store.Snapshot(time.Now(), time.Hour)
			}
		}()
	}
	for i := 0; i < 100; i++ {
		store.commit(-1, records([]Block{block(i, i+1, hash(i), "0xa")}, "ignore"), time.Now())
	}
	wg.Wait()
}
func TestRunCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p := Poller{Source: sourceFor(chain(0, 1, 100, hash(0), "0xa")), Store: NewStore(), Policy: "skip-block", Interval: time.Second, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancellation stuck")
	}
}
