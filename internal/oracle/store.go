package oracle

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const retainedBlocks = 500
const sampleWindow = 200
const coldMessage = "Please wait until the service has fetched at least one block."

type record struct {
	Hash, Parent string
	Number       uint64
	Price        *float64
}
type sample struct {
	Number uint64
	Price  float64
}
type Snapshot struct {
	Body                     []byte
	Samples                  int
	Latest                   uint64
	Ready                    bool
	Reason                   string
	LastSuccess, LastAdvance time.Time
}
type Store struct {
	mu                       sync.RWMutex
	chain                    []record
	samples                  []sample
	body                     []byte // Immutable after publication; replaced on commit.
	lastSuccess, lastAdvance time.Time
	failed                   bool
}

func NewStore() *Store { return &Store{} }

func priceForBlock(b Block, policy string) *float64 {
	min := math.Inf(1)
	for _, tx := range b.Transactions {
		if strings.EqualFold(tx.From, b.Miner) {
			continue
		}
		if !validHex(tx.From, 20) || !strings.HasPrefix(tx.GasPrice, "0x") || len(tx.GasPrice) < 3 || len(tx.GasPrice) > 66 || !hexDigits(tx.GasPrice[2:]) {
			return nil
		}
		raw := strings.TrimLeft(tx.GasPrice[2:], "0")
		var f float64
		if len(raw) > 0 && len(raw) <= 16 {
			n, _ := strconv.ParseUint(raw, 16, 64) // Validated hex, fits uint64.
			f = float64(n)
		} else if len(raw) > 16 {
			n, _ := new(big.Int).SetString(raw, 16)
			f, _ = new(big.Float).SetInt(n).Float64()
		}
		if f == 0 && policy == "ignore" {
			continue
		}
		if f < min {
			min = f
		}
	}
	if min == 0 || math.IsInf(min, 0) || math.IsNaN(min) {
		return nil
	}
	return &min
}
func jsNumber(n float64) string {
	if n == 0 {
		return "0"
	}
	abs := math.Abs(n)
	if abs >= 1e-6 && abs < 1e21 {
		return strconv.FormatFloat(n, 'f', -1, 64)
	}
	s := strconv.FormatFloat(n, 'e', -1, 64)
	parts := strings.Split(s, "e")
	exp, _ := strconv.Atoi(parts[1])
	sign := ""
	if exp >= 0 {
		sign = "+"
	}
	return parts[0] + "e" + sign + strconv.Itoa(exp)
}
func payload(samples []sample, latest uint64) []byte {
	prices := make([]float64, len(samples))
	for i, s := range samples {
		prices[i] = s.Price
	}
	sort.Float64s(prices)
	// Preserve field order as well as names/types and nearest-rank percentiles.
	buf := []byte(fmt.Sprintf(`{"number_of_blocks":%d,"latest_block_number":%d`, len(prices), latest))
	for p := 1; p <= 100; p++ {
		if p > 4 && p < 96 && p%5 != 0 {
			continue
		}
		index := (len(prices)*p+99)/100 - 1
		value, _ := json.Marshal(jsNumber(prices[index]/1e9) + " nanoeth")
		buf = append(buf, []byte(fmt.Sprintf(`,"percentile_%d":`, p))...)
		buf = append(buf, value...)
	}
	return append(buf, '}')
}
func (s *Store) records() []record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]record(nil), s.chain...)
}
func (s *Store) markFailed() { s.mu.Lock(); s.failed = true; s.mu.Unlock() }
func (s *Store) markSuccess(now time.Time) {
	s.mu.Lock()
	s.lastSuccess = now
	s.failed = false
	s.mu.Unlock()
}

// commit replaces a branch atomically; unsampled blocks still belong to the chain.
func (s *Store) commit(ancestor int, branch []record, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	previousHash := ""
	if len(s.chain) > 0 {
		previousHash = s.chain[len(s.chain)-1].Hash
	}
	var older []sample
	if ancestor < 0 {
		s.chain = nil
	} else {
		// Preserve samples older than the ancestry ring, and reconstruct the
		// retained prefix to restore samples previously evicted by orphan blocks.
		for _, entry := range s.samples {
			if entry.Number < s.chain[0].Number {
				older = append(older, entry)
			}
		}
		s.chain = s.chain[:ancestor+1]
	}
	s.chain = append(s.chain, branch...)
	s.samples = older
	for _, b := range s.chain {
		if b.Price != nil {
			s.samples = append(s.samples, sample{b.Number, *b.Price})
		}
	}
	if len(s.chain) > retainedBlocks {
		s.chain = append([]record(nil), s.chain[len(s.chain)-retainedBlocks:]...)
	}
	if len(s.samples) > sampleWindow {
		s.samples = append([]sample(nil), s.samples[len(s.samples)-sampleWindow:]...)
	}
	if len(s.chain) > 0 && s.chain[len(s.chain)-1].Hash != previousHash {
		s.lastAdvance = now
	}
	s.body = nil
	if len(s.samples) > 0 {
		s.body = payload(s.samples, s.chain[len(s.chain)-1].Number)
	}
	s.lastSuccess = now
	s.failed = false
}

// Snapshot.Body is immutable and must not be modified by callers.
func (s *Store) Snapshot(now time.Time, stale time.Duration) Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := Snapshot{Samples: len(s.samples), LastSuccess: s.lastSuccess, LastAdvance: s.lastAdvance}
	if len(s.chain) > 0 {
		out.Latest = s.chain[len(s.chain)-1].Number
	}
	if len(s.samples) == 0 {
		out.Reason = "no_samples"
		return out
	}
	out.Body = s.body
	switch {
	case s.failed:
		out.Reason = "rpc_error"
	case now.Sub(s.lastSuccess) > stale:
		out.Reason = "rpc_stale"
	case now.Sub(s.lastAdvance) > stale:
		out.Reason = "head_stale"
	default:
		out.Ready = true
		out.Reason = "ready"
	}
	return out
}
