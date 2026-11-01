package oracle

import (
	"bytes"
	"context"
	"math/big"
	"math/rand"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMinerAddressCase(t *testing.T) {
	miner := "0x" + strings.Repeat("ab", 20)
	b := block(1, 2, hash(1), "0xa")
	b.Miner = miner
	b.Transactions = append(b.Transactions, Transaction{"0x" + strings.ToUpper(miner[2:]), "0x0"})
	for _, policy := range []string{"skip-block", "ignore"} {
		if got := priceForBlock(b, policy); got == nil || *got != 10 {
			t.Fatal("same address with different case affected minimum", policy, got)
		}
	}
}
func TestFastPriceRounding(t *testing.T) {
	random := rand.New(rand.NewSource(1))
	for i := 0; i < 10000; i++ {
		n := random.Uint64()
		if n == 0 {
			continue
		}
		raw := strconv.FormatUint(n, 16)
		reference, _ := new(big.Float).SetInt(new(big.Int).SetUint64(n)).Float64()
		got := priceForBlock(block(1, 2, hash(1), "0x"+raw), "skip-block")
		if got == nil || *got != reference {
			t.Fatalf("uint64 rounding changed: %s", raw)
		}
	}
	for _, raw := range []string{"0x+1", "0x-1", "0x1_0", "0xGG"} {
		if priceForBlock(block(1, 2, hash(1), raw), "ignore") != nil {
			t.Fatal("malformed quantity accepted", raw)
		}
	}
}
func TestCachedSnapshotRemainsUnchanged(t *testing.T) {
	store := benchmarkStore()
	first := store.Snapshot(time.Now(), time.Minute)
	saved := bytes.Clone(first.Body)
	store.commit(-1, records([]Block{block(1000, 2000, hash(1999), "0x14")}, "skip-block"), time.Now())
	if !bytes.Equal(first.Body, saved) {
		t.Fatal("published payload was mutated")
	}
	if bytes.Equal(first.Body, store.Snapshot(time.Now(), time.Minute).Body) {
		t.Fatal("payload cache not invalidated")
	}
	if got := testing.AllocsPerRun(100, func() { store.Snapshot(time.Now(), time.Minute) }); got != 0 {
		t.Fatalf("snapshot allocates: %v", got)
	}
}
func TestLivezDoesNotReadStore(t *testing.T) {
	w := httptest.NewRecorder()
	Handler(nil, time.Minute).ServeHTTP(w, httptest.NewRequest("GET", "/livez", nil))
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatal(w)
	}
}
func TestMemoryBudget(t *testing.T) {
	for _, fixture := range []struct {
		response, limit string
		wantOK          bool
	}{
		{"33554432", "192MiB", true}, {"67108864", "192MiB", false}, {"268435456", "1GiB", true},
		{"268435456", "off", true}, {"33554432", "invalid", false}, {"33554432", "-1", false},
		{"33554432", "99999999999999999TiB", false}, {"33554432", "536870912", true},
	} {
		_, err := LoadConfig(func(k string) string {
			switch k {
			case "RPC_MAX_RESPONSE_BYTES":
				return fixture.response
			case "GOMEMLIMIT":
				return fixture.limit
			}
			return ""
		})
		if (err == nil) != fixture.wantOK {
			t.Fatalf("response=%s limit=%s: %v", fixture.response, fixture.limit, err)
		}
	}
}
func TestZeroIntervalDoesNotBusyLoop(t *testing.T) {
	store := NewStore()
	source := sourceFor(chain(0, 1, 100, hash(0), "0xa"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	poller := Poller{Source: source, Store: store, Policy: "skip-block"}
	poller.Run(ctx)
	if source.polls > 1 {
		t.Fatal("zero interval caused repeated polls", source.polls)
	}
}

func TestRPCRateConfig(t *testing.T) {
	for _, value := range []string{"0", "-1", "101", "1.5"} {
		if _, err := LoadConfig(func(k string) string {
			if k == "RPC_MAX_REQUESTS_PER_SECOND" {
				return value
			}
			return ""
		}); err == nil {
			t.Fatal("invalid RPC rate accepted", value)
		}
	}
	c, err := LoadConfig(func(string) string { return "" })
	if err != nil || c.MaxRequestsPerSecond != 5 {
		t.Fatal(c, err)
	}
}
