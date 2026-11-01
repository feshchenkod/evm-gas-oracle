package main

import (
	"bytes"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProbe(t *testing.T) {
	for _, status := range []int{200, 500, 503, 302} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/readyz" {
					t.Error(r.URL.Path)
				}
				if status == 302 {
					w.Header().Set("Location", "/elsewhere")
				}
				w.WriteHeader(status)
				_, _ = w.Write([]byte("ok"))
			}))
			defer server.Close()
			_, port, _ := net.SplitHostPort(server.Listener.Addr().String())
			want := 1
			if status == 200 {
				want = 0
			}
			for _, address := range []string{":" + port, "0.0.0.0:" + port, "127.0.0.1:" + port} {
				if got := probe(address); got != want {
					t.Fatalf("probe(%s)=%d, want %d", address, got, want)
				}
			}
		})
	}
	if probe("invalid") != 1 {
		t.Fatal("accepted invalid address")
	}
	closed := httptest.NewServer(http.NotFoundHandler())
	address := closed.Listener.Addr().String()
	closed.Close()
	if probe(address) != 1 {
		t.Fatal("unavailable port is healthy")
	}
	truncated := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		_, _ = w.Write([]byte("short"))
	}))
	defer truncated.Close()
	if probe(truncated.Listener.Addr().String()) != 1 {
		t.Fatal("truncated response is healthy")
	}
}
func TestProbeIPv6(t *testing.T) {
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skip("IPv6 loopback unavailable")
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	_ = server.Listener.Close()
	server.Listener = listener
	server.Start()
	defer server.Close()
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	if probe("[::]:"+port) != 0 || probe("[::1]:"+port) != 0 {
		t.Fatal("IPv6 probe failed")
	}
}
func TestRunArgs(t *testing.T) {
	if runArgs([]string{"-version"}) != 0 {
		t.Fatal("version flag failed")
	}
	if runArgs([]string{"-not-a-flag"}) != 2 {
		t.Fatal("invalid flag accepted")
	}
	t.Setenv("ETHEREUM_URL", "file:///invalid")
	if runArgs(nil) != 1 {
		t.Fatal("invalid configuration accepted")
	}
	t.Setenv("ETHEREUM_URL", "http://127.0.0.1:1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer server.Close()
	t.Setenv("HTTP_ADDR", server.Listener.Addr().String())
	if runArgs([]string{"-healthcheck"}) != 0 {
		t.Fatal("healthcheck flag failed")
	}
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	if runArgs(nil) != 1 {
		t.Fatal("occupied port accepted")
	}
	if !strings.Contains(logs.String(), "cannot listen") || !strings.Contains(logs.String(), "error=") {
		t.Fatal("listen failure lost cause", logs.String())
	}
}
