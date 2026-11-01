package oracle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Transaction struct {
	From     string `json:"from"`
	GasPrice string `json:"gasPrice"`
}
type Block struct {
	Number       string        `json:"number"`
	Hash         string        `json:"hash"`
	ParentHash   string        `json:"parentHash"`
	Miner        string        `json:"miner"`
	Transactions []Transaction `json:"transactions"`
}

func validHex(s string, bytes int) bool {
	if len(s) != 2+bytes*2 || !strings.HasPrefix(s, "0x") {
		return false
	}
	return hexDigits(s[2:])
}
func hexDigits(raw string) bool {
	for i := range len(raw) {
		c := raw[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return len(raw) > 0
}
func (b Block) Height() (uint64, error) {
	if !strings.HasPrefix(b.Number, "0x") || len(b.Number) < 3 {
		return 0, fmt.Errorf("invalid block number")
	}
	n, err := strconv.ParseUint(b.Number[2:], 16, 64)
	if err != nil || n > 1<<53-1 {
		return 0, fmt.Errorf("block number outside compatible JSON integer range")
	}
	return n, nil
}
func (b Block) Validate() error {
	if _, err := b.Height(); err != nil {
		return err
	}
	if !validHex(b.Hash, 32) || !validHex(b.ParentHash, 32) || !validHex(b.Miner, 20) || b.Transactions == nil {
		return fmt.Errorf("invalid full block response")
	}
	return nil
}

type BlockSource interface {
	ByNumber(context.Context, string) (Block, error)
	ByHash(context.Context, string) (Block, error)
}
type RPCClient struct {
	url       string
	client    *http.Client
	limit     int64
	next      atomic.Uint64
	gate      chan struct{}
	interval  time.Duration
	lastStart time.Time
}

func NewRPCClient(c Config) *RPCClient {
	if c.MaxRequestsPerSecond <= 0 {
		c.MaxRequestsPerSecond = 5
	}
	dialer := &net.Dialer{Timeout: c.RPCTimeout, KeepAlive: 30 * time.Second}
	dialer.SetMultipathTCP(false)
	transport := &http.Transport{DialContext: dialer.DialContext, ForceAttemptHTTP2: true, DisableCompression: true, MaxIdleConns: 4, MaxIdleConnsPerHost: 1, MaxConnsPerHost: 1, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: c.RPCTimeout, ResponseHeaderTimeout: c.RPCTimeout}
	return &RPCClient{url: c.RPCURL, limit: c.MaxResponseBytes, gate: make(chan struct{}, 1), interval: time.Second / time.Duration(c.MaxRequestsPerSecond), client: &http.Client{Timeout: c.RPCTimeout, Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}
func (c *RPCClient) Close() { c.client.CloseIdleConnections() }
func (c *RPCClient) ByNumber(ctx context.Context, n string) (Block, error) {
	return c.call(ctx, "eth_getBlockByNumber", n)
}
func (c *RPCClient) ByHash(ctx context.Context, h string) (Block, error) {
	return c.call(ctx, "eth_getBlockByHash", h)
}
func (c *RPCClient) call(ctx context.Context, method, arg string) (Block, error) {
	// A single cancellable gate bounds both in-flight RPC work and start rate,
	// including bootstrap, catch-up, failures and future concurrent callers.
	select {
	case c.gate <- struct{}{}:
	case <-ctx.Done():
		return Block{}, fmt.Errorf("RPC request canceled")
	}
	defer func() { <-c.gate }()
	if err := ctx.Err(); err != nil {
		return Block{}, fmt.Errorf("RPC request canceled")
	}
	if delay := time.Until(c.lastStart.Add(c.interval)); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return Block{}, fmt.Errorf("RPC request canceled")
		}
	}
	c.lastStart = time.Now()
	id := c.next.Add(1)
	data, _ := json.Marshal(struct {
		Version string `json:"jsonrpc"`
		ID      uint64 `json:"id"`
		Method  string `json:"method"`
		Params  []any  `json:"params"`
	}{"2.0", id, method, []any{arg, true}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(data))
	if err != nil {
		return Block{}, fmt.Errorf("cannot create RPC request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	res, err := c.client.Do(req)
	if err != nil {
		return Block{}, fmt.Errorf("RPC transport failed or timed out")
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return Block{}, fmt.Errorf("RPC HTTP status %d", res.StatusCode)
	}
	if encoding := res.Header.Get("Content-Encoding"); encoding != "" && !strings.EqualFold(encoding, "identity") {
		return Block{}, fmt.Errorf("RPC returned unsupported content encoding")
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, c.limit+1))
	if err != nil {
		return Block{}, fmt.Errorf("cannot read RPC response")
	}
	if int64(len(body)) > c.limit {
		return Block{}, fmt.Errorf("RPC response exceeds configured size limit")
	}
	var response struct {
		Version string `json:"jsonrpc"`
		ID      uint64 `json:"id"`
		Result  *Block `json:"result"`
		Error   *struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	if err = json.Unmarshal(body, &response); err != nil {
		return Block{}, fmt.Errorf("invalid RPC JSON response")
	}
	if response.Version != "2.0" || response.ID != id {
		return Block{}, fmt.Errorf("RPC version or request ID mismatch")
	}
	if response.Error != nil {
		return Block{}, fmt.Errorf("RPC error code %d", response.Error.Code)
	}
	if response.Result == nil {
		return Block{}, fmt.Errorf("RPC returned no block")
	}
	if err = response.Result.Validate(); err != nil {
		return Block{}, err
	}
	b := *response.Result
	if method == "eth_getBlockByHash" && !strings.EqualFold(b.Hash, arg) {
		return Block{}, fmt.Errorf("RPC returned the wrong block hash")
	}
	if method == "eth_getBlockByNumber" && arg != "latest" {
		n, err := strconv.ParseUint(strings.TrimPrefix(arg, "0x"), 16, 64)
		got, _ := b.Height()
		if err != nil || got != n {
			return Block{}, fmt.Errorf("RPC returned the wrong block height")
		}
	}
	b.Hash = strings.ToLower(b.Hash)
	b.ParentHash = strings.ToLower(b.ParentHash)
	return b, nil
}
