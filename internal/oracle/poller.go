package oracle

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"
)

type Poller struct {
	Source   BlockSource
	Store    *Store
	Policy   string
	Interval time.Duration
	Log      *slog.Logger
}

func (p *Poller) Sync(ctx context.Context, now time.Time) (err error) {
	defer func() {
		if err != nil {
			p.Store.markFailed()
		}
	}()
	head, err := p.Source.ByNumber(ctx, "latest")
	if err != nil {
		return err
	}
	if err = head.Validate(); err != nil {
		return err
	}
	old := p.Store.records()
	known := make(map[string]int, len(old))
	for i, b := range old {
		known[b.Hash] = i
	}
	headNumber, _ := head.Height()
	if len(old) > 0 {
		tip := old[len(old)-1]
		if headNumber < tip.Number {
			return fmt.Errorf("RPC head is behind the last accepted head")
		}
		if head.Hash == tip.Hash {
			p.Store.markSuccess(now)
			return nil
		}
	}
	limit := retainedBlocks
	if len(old) == 0 {
		limit = 51
	} // Initial head plus the preceding 50 blocks.
	reverse := make([]record, 0, limit)
	current := head
	ancestor := -1
	for {
		if i, ok := known[current.Hash]; ok {
			ancestor = i
			break
		}
		n, err := current.Height()
		if err != nil {
			return err
		}
		reverse = append(reverse, record{current.Hash, current.ParentHash, n, priceForBlock(current, p.Policy)})
		if n == 0 || len(reverse) == limit {
			break
		}
		// The parent is already validated and retained locally: do not refetch it.
		if index, ok := known[current.ParentHash]; ok && old[index].Number == n-1 {
			ancestor = index
			break
		}
		parent, err := p.Source.ByHash(ctx, current.ParentHash)
		if err != nil {
			return err
		}
		if err = parent.Validate(); err != nil {
			return err
		}
		parentNumber, _ := parent.Height()
		if parent.Hash != current.ParentHash || parentNumber != n-1 {
			return fmt.Errorf("RPC returned inconsistent block ancestry")
		}
		current = parent
	}
	slices.Reverse(reverse)
	p.Store.commit(ancestor, reverse, now)
	return nil
}
func (p *Poller) Run(ctx context.Context) {
	interval := p.Interval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if err := p.Sync(ctx, time.Now()); err != nil && ctx.Err() == nil && p.Log != nil {
			p.Log.Warn("poll failed; preserving last complete snapshot", "error", err.Error())
		}
		timer.Reset(interval) // No overlapping polls or queued retries.
	}
}
