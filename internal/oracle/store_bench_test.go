package oracle

import (
	"net/http/httptest"
	"testing"
	"time"
)

func benchmarkStore() *Store {
	store := NewStore()
	blocks := chain(0, 199, 1000, hash(0), "0x2faf080")
	store.commit(-1, records(blocks, "skip-block"), time.Now())
	return store
}
func BenchmarkSnapshot(b *testing.B) {
	store := benchmarkStore()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.Snapshot(time.Now(), time.Minute)
	}
}
func BenchmarkLivez(b *testing.B) {
	store := benchmarkStore()
	handler := Handler(store, time.Minute)
	req := httptest.NewRequest("GET", "/livez", nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.ServeHTTP(httptest.NewRecorder(), req)
	}
}
func BenchmarkPriceForBlock(b *testing.B) {
	prices := make([]string, 300)
	for i := range prices {
		prices[i] = "0x2faf080"
	}
	block := block(1, 2, hash(1), prices...)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		priceForBlock(block, "skip-block")
	}
}
