package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/feshchenkod/evm-gas-oracle/internal/oracle"
)

var version = "dev"

func main()    { os.Exit(run()) }
func run() int { return runArgs(os.Args[1:]) }
func runArgs(args []string) int {
	flags := flag.NewFlagSet("evm-gas-oracle", flag.ContinueOnError)
	health := flags.Bool("healthcheck", false, "check local readiness and exit")
	showVersion := flags.Bool("version", false, "print build version")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *showVersion {
		fmt.Println(version)
		return 0
	}
	c, err := oracle.LoadConfig(os.Getenv)
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		return 1
	}
	if *health {
		return probe(c.Listen)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	client := oracle.NewRPCClient(c)
	defer client.Close()
	store := oracle.NewStore()
	poller := &oracle.Poller{Source: client, Store: store, Policy: c.ZeroPolicy, Interval: c.PollInterval, Log: slog.Default()}
	listenerConfig := net.ListenConfig{}
	listenerConfig.SetMultipathTCP(false)
	listener, err := listenerConfig.Listen(ctx, "tcp", c.Listen)
	if err != nil {
		slog.Error("cannot listen", "error", err)
		return 1
	}
	server := &http.Server{Handler: oracle.Handler(store, c.StaleAfter), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	go poller.Run(ctx)
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	slog.Info("oracle listening", "address", listener.Addr().String(), "version", version, "zero_gas_price_policy", c.ZeroPolicy)
	if err = server.Serve(listener); err != nil && err != http.ErrServerClosed {
		slog.Error("HTTP server failed", "error", err)
		return 1
	}
	return 0
}
func probe(address string) int {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return 1
	}
	if host == "" || host == "0.0.0.0" {
		host = "127.0.0.1"
	}
	if host == "::" {
		host = "::1"
	}
	dialer := &net.Dialer{Timeout: 3 * time.Second}
	dialer.SetMultipathTCP(false)
	transport := &http.Transport{DialContext: dialer.DialContext, DisableCompression: true}
	defer transport.CloseIdleConnections()
	client := http.Client{Timeout: 3 * time.Second, Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Get("http://" + net.JoinHostPort(host, port) + "/readyz")
	if err != nil {
		return 1
	}
	defer response.Body.Close()
	if _, err := io.Copy(io.Discard, io.LimitReader(response.Body, 4096)); err != nil {
		return 1
	}
	if response.StatusCode != 200 {
		return 1
	}
	return 0
}
