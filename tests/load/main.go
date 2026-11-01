// Local load experiment. The only RPC endpoint is this process's synthetic fixture.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type fixture struct {
	calls     atomic.Int64
	active    atomic.Int64
	maxActive atomic.Int64
	head      atomic.Int64
	delay     atomic.Int64
	mu        sync.Mutex
	starts    []time.Time
}

func (f *fixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n := f.active.Add(1)
	defer f.active.Add(-1)
	for old := f.maxActive.Load(); n > old; old = f.maxActive.Load() {
		if f.maxActive.CompareAndSwap(old, n) {
			break
		}
	}
	f.calls.Add(1)
	f.mu.Lock()
	f.starts = append(f.starts, time.Now())
	f.mu.Unlock()
	if delay := time.Duration(f.delay.Load()); delay > 0 {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(delay):
		}
	}
	var request struct {
		ID     uint64
		Method string
		Params []json.RawMessage
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&request) != nil {
		http.Error(w, "invalid", 400)
		return
	}
	height := f.head.Load()
	if request.Method == "eth_getBlockByHash" {
		var h string
		if len(request.Params) < 1 || json.Unmarshal(request.Params[0], &h) != nil || len(h) != 66 {
			http.Error(w, "invalid hash", 400)
			return
		}
		n, err := strconv.ParseInt(h[2:], 16, 64)
		if err != nil || n < 1 {
			http.Error(w, "invalid height", 400)
			return
		}
		height = n - 1
	}
	transactions := make([]map[string]string, 300)
	for i := range transactions {
		transactions[i] = map[string]string{"from": "0x2222222222222222222222222222222222222222", "to": "0x3333333333333333333333333333333333333333", "gasPrice": fmt.Sprintf("0x%x", 50000000+i), "hash": fmt.Sprintf("0x%064x", i+1), "input": "0x" + strings.Repeat("ab", 32)}
	}
	block := map[string]any{"number": fmt.Sprintf("0x%x", height), "hash": fmt.Sprintf("0x%064x", height+1), "parentHash": fmt.Sprintf("0x%064x", height), "miner": "0x1111111111111111111111111111111111111111", "transactions": transactions}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": block})
}

type phase struct {
	RequestedRPS int     `json:"requested_rps"`
	Dropped      int     `json:"dropped"`
	CPUPercent   float64 `json:"oracle_cpu_percent"`
	RSSMiB       float64 `json:"oracle_rss_mib"`
	Name         string  `json:"name"`
	Concurrency  int     `json:"concurrency"`
	Requests     int     `json:"requests"`
	Errors       int     `json:"errors"`
	Seconds      float64 `json:"seconds"`
	RPS          float64 `json:"http_rps"`
	P50          float64 `json:"p50_ms"`
	P95          float64 `json:"p95_ms"`
	P99          float64 `json:"p99_ms"`
	Max          float64 `json:"max_ms"`
	InFlight     int64   `json:"max_http_in_flight"`
	RPCCalls     int64   `json:"rpc_calls"`
}
type report struct {
	Go               string          `json:"generator_go"`
	CPUs             int             `json:"host_cpus"`
	Image            string          `json:"image"`
	ImageID          string          `json:"image_id"`
	Limits           string          `json:"oracle_limits"`
	BootstrapCalls   int64           `json:"bootstrap_rpc_calls"`
	BootstrapSeconds float64         `json:"bootstrap_seconds"`
	MaxRPCInFlight   int64           `json:"max_rpc_in_flight"`
	MinRPCSpacingMS  float64         `json:"minimum_rpc_arrival_spacing_ms"`
	Phases           []phase         `json:"phases"`
	ContainerState   json.RawMessage `json:"final_container_state"`
}

func docker(args ...string) ([]byte, error) {
	command := exec.Command("docker", args...)
	output, err := command.CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("docker %s: %w: %s", args[0], err, output)
	}
	return output, nil
}
func client(concurrency int) *http.Client {
	dialer := &net.Dialer{Timeout: 3 * time.Second}
	dialer.SetMultipathTCP(false)
	return &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{DialContext: dialer.DialContext, MaxIdleConns: concurrency, MaxIdleConnsPerHost: concurrency, MaxConnsPerHost: concurrency, DisableCompression: true}}
}
func hit(c *http.Client, url string) (int, error) {
	response, err := c.Get(url)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	_, err = io.Copy(io.Discard, response.Body)
	return response.StatusCode, err
}
func load(name, url string, concurrency int, duration time.Duration, f *fixture, pid int, hz float64) (phase, error) {
	c := client(concurrency)
	defer c.CloseIdleConnections()
	cpuBefore, _ := processUsage(pid)
	start := time.Now()
	deadline := start.Add(duration)
	before := f.calls.Load()
	var inFlight, maxInFlight atomic.Int64
	type result struct {
		latencies []time.Duration
		errors    int
	}
	results := make(chan result, concurrency)
	var ready sync.WaitGroup
	ready.Add(concurrency)
	begin := make(chan struct{})
	for i := 0; i < concurrency; i++ {
		go func() {
			ready.Done()
			<-begin
			r := result{}
			for time.Now().Before(deadline) {
				n := inFlight.Add(1)
				for old := maxInFlight.Load(); n > old; old = maxInFlight.Load() {
					if maxInFlight.CompareAndSwap(old, n) {
						break
					}
				}
				sent := time.Now()
				status, err := hit(c, url)
				latency := time.Since(sent)
				inFlight.Add(-1)
				r.latencies = append(r.latencies, latency)
				if err != nil || status != 200 {
					r.errors++
				}
			}
			results <- r
		}()
	}
	ready.Wait()
	close(begin)
	var latencies []time.Duration
	failures := 0
	for i := 0; i < concurrency; i++ {
		r := <-results
		latencies = append(latencies, r.latencies...)
		failures += r.errors
	}
	elapsed := time.Since(start)
	calls := f.calls.Load() - before
	if len(latencies) == 0 {
		return phase{}, errors.New("no completed HTTP requests")
	}
	slices.Sort(latencies)
	percentile := func(p int) float64 {
		return float64(latencies[(len(latencies)*p+99)/100-1]) / float64(time.Millisecond)
	}
	out := phase{Name: name, Concurrency: concurrency, Requests: len(latencies), Errors: failures, Seconds: elapsed.Seconds(), RPS: float64(len(latencies)) / elapsed.Seconds(), P50: percentile(50), P95: percentile(95), P99: percentile(99), Max: percentile(100), InFlight: maxInFlight.Load(), RPCCalls: calls}
	cpuAfter, rss := processUsage(pid)
	if cpuBefore >= 0 && cpuAfter >= cpuBefore && hz > 0 {
		out.CPUPercent = float64(cpuAfter-cpuBefore) / hz / elapsed.Seconds() * 100
	}
	out.RSSMiB = rss
	fmt.Printf("%-16s clients=%-3d rps=%8.0f p50=%7.3fms p99=%8.3fms errors=%d rpc=%d\n", name, concurrency, out.RPS, out.P50, out.P99, out.Errors, out.RPCCalls)
	if failures > 0 {
		return out, fmt.Errorf("%s: %d HTTP failures", name, failures)
	}
	// All node work is from the one background poller, independently of HTTP RPS.
	if calls > int64(elapsed.Seconds()*5)+2 {
		return out, fmt.Errorf("%s: RPC rate exceeded budget", name)
	}
	return out, nil
}
func waitReady(url string) error {
	c := client(1)
	defer c.CloseIdleConnections()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if status, err := hit(c, url+"/readyz"); err == nil && status == 200 {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("oracle did not become ready")
}
func run() error {
	image := flag.String("image", "evm-gas-oracle:load", "local image to test")
	output := flag.String("report", "/tmp/evm-gas-oracle-load-results.json", "write JSON report")
	duration := flag.Duration("duration", 3*time.Second, "duration per concurrency phase")
	soak := flag.Duration("soak", 0, "optional additional fixed-rate soak duration")
	flag.Parse()
	if *soak < 0 || *soak > 10*time.Minute {
		return errors.New("soak must be between 0 and 10m")
	}
	if *duration < time.Second {
		return errors.New("duration must be at least 1s")
	}
	runtime.GOMAXPROCS(4)
	f := &fixture{}
	f.head.Store(1000)
	upstream := httptest.NewServer(f)
	defer upstream.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	address := listener.Addr().String()
	_ = listener.Close()
	name := "evm-oracle-load-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	defer func() { _, _ = docker("rm", "-f", name) }()
	started := time.Now()
	_, err = docker("run", "-d", "--name", name, "--network", "host", "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true", "--memory", "256m", "--cpus", "2", "--pids-limit", "128", "-e", "ETHEREUM_URL="+upstream.URL, "-e", "HTTP_ADDR="+address, "-e", "POLLING_FREQUENCY_SECONDS=1", "-e", "GOMEMLIMIT=192MiB", "-e", "GOMAXPROCS=2", "-e", "RPC_MAX_REQUESTS_PER_SECOND=5", *image)
	if err != nil {
		return err
	}
	success := false
	defer func() {
		if !success {
			logs, _ := docker("logs", "--tail", "30", name)
			fmt.Fprintln(os.Stderr, string(logs))
		}
	}()
	url := "http://" + address
	if err = waitReady(url); err != nil {
		return err
	}
	imageID, err := docker("image", "inspect", "--format", "{{.Id}}", *image)
	if err != nil {
		return err
	}
	pidData, err := docker("inspect", "--format", "{{.State.Pid}}", name)
	if err != nil {
		return err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidData)))
	if err != nil {
		return err
	}
	hzData, err := exec.Command("getconf", "CLK_TCK").Output()
	if err != nil {
		return err
	}
	hz, err := strconv.ParseFloat(strings.TrimSpace(string(hzData)), 64)
	if err != nil {
		return err
	}
	out := report{Go: runtime.Version(), CPUs: runtime.NumCPU(), Image: *image, ImageID: strings.TrimSpace(string(imageID)), Limits: "2 CPUs, GOMAXPROCS=2, memory=256MiB, GOMEMLIMIT=192MiB, pids=128, nonroot/read-only", BootstrapCalls: f.calls.Load(), BootstrapSeconds: time.Since(started).Seconds()}
	fmt.Printf("bootstrap: %d RPC calls in %.2fs (paced, 300 tx/block)\n", out.BootstrapCalls, out.BootstrapSeconds)
	controlled, err := fixedRate(url+"/", 10000, *duration, f, pid, hz)
	out.Phases = append(out.Phases, controlled)
	if err != nil {
		return err
	}
	for _, concurrency := range []int{1, 2, 32, 128, 512} {
		result, err := load("steady", url+"/", concurrency, *duration, f, pid, hz)
		out.Phases = append(out.Phases, result)
		if err != nil {
			return err
		}
	}
	// Keep many partially sent requests open; they must not occupy fixed workers.
	var slow []net.Conn
	for i := 0; i < 32; i++ {
		conn, err := net.DialTimeout("tcp", address, time.Second)
		if err != nil {
			return err
		}
		slow = append(slow, conn)
		_, _ = fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: local\r\nX-Slow: ")
	}
	result, err := load("slow-headers", url+"/", 32, *duration, f, pid, hz)
	out.Phases = append(out.Phases, result)
	for _, conn := range slow {
		_ = conn.Close()
	}
	if err != nil {
		return err
	}
	// Polling waits on a slow upstream while HTTP serves the last committed body.
	f.delay.Store(int64(2 * time.Second))
	result, err = load("slow-rpc", url+"/", 128, *duration, f, pid, hz)
	out.Phases = append(out.Phases, result)
	f.delay.Store(0)
	if err != nil {
		return err
	}
	if result.RPCCalls == 0 {
		return errors.New("slow-RPC phase did not overlap a poll")
	}
	// Exercise snapshot replacement while the HTTP reader pool is saturated.
	updateCtx, stop := context.WithCancel(context.Background())
	updated := make(chan struct{})
	go func() {
		defer close(updated)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-updateCtx.Done():
				return
			case <-ticker.C:
				f.head.Add(1)
			}
		}
	}()
	result, err = load("head-updates", url+"/", 128, 2*(*duration), f, pid, hz)
	stop()
	<-updated
	out.Phases = append(out.Phases, result)
	if err != nil {
		return err
	}
	if *soak > 0 {
		fmt.Printf("starting fixed-rate soak: %s at 10000 requests/s\n", *soak)
		soakResult, err := fixedRate(url+"/", 10000, *soak, f, pid, hz)
		soakResult.Name = "soak-fixed-rate"
		out.Phases = append(out.Phases, soakResult)
		if err != nil {
			return err
		}
	}
	c := client(1)
	response, err := c.Get(url + "/")
	if err != nil {
		return err
	}
	var final struct {
		Latest uint64 `json:"latest_block_number"`
	}
	err = json.NewDecoder(response.Body).Decode(&final)
	_ = response.Body.Close()
	c.CloseIdleConnections()
	if err != nil {
		return err
	}
	if final.Latest <= 1000 {
		return errors.New("poller made no progress under HTTP load")
	}
	out.MaxRPCInFlight = f.maxActive.Load()
	if out.MaxRPCInFlight != 1 {
		return fmt.Errorf("node RPC concurrency=%d", out.MaxRPCInFlight)
	}
	f.mu.Lock()
	spacing := time.Hour
	for i := 1; i < len(f.starts); i++ {
		spacing = min(spacing, f.starts[i].Sub(f.starts[i-1]))
	}
	f.mu.Unlock()
	out.MinRPCSpacingMS = float64(spacing) / float64(time.Millisecond)
	if spacing < 180*time.Millisecond {
		return fmt.Errorf("node RPC calls were not paced: minimum arrival spacing %s", spacing)
	}
	if _, err = docker("stop", "--time", "5", name); err != nil {
		return err
	}
	state, err := docker("inspect", "--format", "{{json .State}}", name)
	if err != nil {
		return err
	}
	out.ContainerState = json.RawMessage(bytesTrim(state))
	var container struct {
		ExitCode  int
		OOMKilled bool
	}
	if err = json.Unmarshal(state, &container); err != nil {
		return err
	}
	if container.ExitCode != 0 || container.OOMKilled {
		return fmt.Errorf("bad final container state: %s", state)
	}
	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(*output, append(data, '\n'), 0644); err != nil {
		return err
	}
	fmt.Printf("PASS: max RPC concurrency=%d; minimum RPC spacing=%.1fms; final head=%d; report=%s\n", out.MaxRPCInFlight, out.MinRPCSpacingMS, final.Latest, *output)
	success = true
	return nil
}
func bytesTrim(b []byte) []byte { return []byte(strings.TrimSpace(string(b))) }
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// Linux/local-Docker resource observations; zero RSS means /proc was unavailable.
func processUsage(pid int) (int64, float64) {
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return -1, 0
	}
	end := strings.LastIndexByte(string(stat), ')')
	if end < 0 {
		return -1, 0
	}
	fields := strings.Fields(string(stat[end+1:]))
	if len(fields) < 13 {
		return -1, 0
	}
	user, err := strconv.ParseInt(fields[11], 10, 64)
	if err != nil {
		return -1, 0
	}
	system, err := strconv.ParseInt(fields[12], 10, 64)
	if err != nil {
		return -1, 0
	}
	status, _ := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	var rss float64
	for _, line := range strings.Split(string(status), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			parts := strings.Fields(line)
			if len(parts) > 1 {
				kb, _ := strconv.ParseFloat(parts[1], 64)
				rss = kb / 1024
			}
		}
	}
	return user + system, rss
}

// Fixed-arrival experiment with a bounded queue. Latency includes scheduling and
// queue delay; excess arrivals are reported as drops, never silently omitted.
func fixedRate(url string, rate int, duration time.Duration, f *fixture, pid int, hz float64) (phase, error) {
	const workers = 128
	c := client(workers)
	defer c.CloseIdleConnections()
	tasks := make(chan time.Time, workers)
	type result struct {
		latencies []time.Duration
		failures  int
	}
	results := make(chan result, workers)
	var active, peak atomic.Int64
	for i := 0; i < workers; i++ {
		go func() {
			out := result{}
			for scheduled := range tasks {
				n := active.Add(1)
				for old := peak.Load(); n > old; old = peak.Load() {
					if peak.CompareAndSwap(old, n) {
						break
					}
				}
				status, err := hit(c, url)
				active.Add(-1)
				out.latencies = append(out.latencies, time.Since(scheduled))
				if err != nil || status != 200 {
					out.failures++
				}
			}
			results <- out
		}()
	}
	start := time.Now()
	before := f.calls.Load()
	cpuBefore, _ := processUsage(pid)
	dropped := 0
	slots := int(duration / time.Millisecond)
	for slot := 0; slot < slots; slot++ {
		scheduled := start.Add(time.Duration(slot) * time.Millisecond)
		if wait := time.Until(scheduled); wait > 0 {
			time.Sleep(wait)
		}
		count := (slot+1)*rate/1000 - slot*rate/1000
		for i := 0; i < count; i++ {
			select {
			case tasks <- scheduled:
			default:
				dropped++
			}
		}
	}
	close(tasks)
	var latencies []time.Duration
	failures := 0
	for i := 0; i < workers; i++ {
		r := <-results
		latencies = append(latencies, r.latencies...)
		failures += r.failures
	}
	elapsed := time.Since(start)
	cpuAfter, rss := processUsage(pid)
	if len(latencies) == 0 {
		return phase{}, errors.New("no fixed-rate responses")
	}
	slices.Sort(latencies)
	percentile := func(p int) float64 {
		return float64(latencies[(len(latencies)*p+99)/100-1]) / float64(time.Millisecond)
	}
	out := phase{Name: "fixed-rate", Concurrency: workers, RequestedRPS: rate, Dropped: dropped, Requests: len(latencies), Errors: failures, Seconds: elapsed.Seconds(), RPS: float64(len(latencies)) / elapsed.Seconds(), P50: percentile(50), P95: percentile(95), P99: percentile(99), Max: percentile(100), InFlight: peak.Load(), RPCCalls: f.calls.Load() - before, RSSMiB: rss}
	if cpuBefore >= 0 && cpuAfter >= cpuBefore {
		out.CPUPercent = float64(cpuAfter-cpuBefore) / hz / elapsed.Seconds() * 100
	}
	fmt.Printf("%-16s target=%-5d rps=%8.0f p50=%7.3fms p99=%8.3fms errors=%d dropped=%d rpc=%d\n", out.Name, rate, out.RPS, out.P50, out.P99, failures, dropped, out.RPCCalls)
	if failures != 0 || dropped != 0 {
		return out, fmt.Errorf("fixed-rate experiment: errors=%d drops=%d", failures, dropped)
	}
	if out.RPCCalls > int64(elapsed.Seconds()*5)+2 {
		return out, errors.New("fixed-rate load increased RPC beyond budget")
	}
	return out, nil
}
