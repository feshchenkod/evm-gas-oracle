package oracle

import (
	"fmt"
	"math"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	RPCURL               string
	Listen               string
	PollInterval         time.Duration
	RPCTimeout           time.Duration
	StaleAfter           time.Duration
	MaxRequestsPerSecond int
	MaxResponseBytes     int64
	ZeroPolicy           string
}

func LoadConfig(getenv func(string) string) (Config, error) {
	value := func(key, fallback string) string {
		if v := getenv(key); v != "" {
			return v
		}
		return fallback
	}
	c := Config{RPCURL: value("ETHEREUM_URL", "http://localhost:8545/"), Listen: value("HTTP_ADDR", ":80"), ZeroPolicy: value("ZERO_GAS_PRICE_POLICY", "skip-block")}
	u, err := url.Parse(c.RPCURL)
	if err != nil || u == nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.Fragment != "" {
		return c, fmt.Errorf("ETHEREUM_URL must be an HTTP(S) URL without a fragment")
	}
	if _, _, err = net.SplitHostPort(c.Listen); err != nil {
		return c, fmt.Errorf("HTTP_ADDR must be host:port")
	}
	if c.ZeroPolicy != "skip-block" && c.ZeroPolicy != "ignore" {
		return c, fmt.Errorf("ZERO_GAS_PRICE_POLICY must be skip-block or ignore")
	}
	seconds, err := strconv.Atoi(value("POLLING_FREQUENCY_SECONDS", "5"))
	if err != nil || seconds < 1 || seconds > 3600 {
		return c, fmt.Errorf("POLLING_FREQUENCY_SECONDS must be an integer from 1 to 3600")
	}
	c.PollInterval = time.Duration(seconds) * time.Second
	defaultStale := max(2*time.Minute, 3*c.PollInterval)
	for _, entry := range []struct {
		key, fallback string
		dest          *time.Duration
	}{
		{"RPC_TIMEOUT", "10s", &c.RPCTimeout}, {"STALE_AFTER", defaultStale.String(), &c.StaleAfter},
	} {
		*entry.dest, err = time.ParseDuration(value(entry.key, entry.fallback))
		if err != nil || *entry.dest <= 0 || *entry.dest > 24*time.Hour {
			return c, fmt.Errorf("%s must be a positive duration up to 24h", entry.key)
		}
	}
	if c.StaleAfter < c.PollInterval {
		return c, fmt.Errorf("STALE_AFTER must be at least POLLING_FREQUENCY_SECONDS")
	}
	c.MaxRequestsPerSecond, err = strconv.Atoi(value("RPC_MAX_REQUESTS_PER_SECOND", "5"))
	if err != nil || c.MaxRequestsPerSecond < 1 || c.MaxRequestsPerSecond > 100 {
		return c, fmt.Errorf("RPC_MAX_REQUESTS_PER_SECOND must be an integer from 1 to 100")
	}
	c.MaxResponseBytes, err = strconv.ParseInt(value("RPC_MAX_RESPONSE_BYTES", "33554432"), 10, 64)
	if err != nil || c.MaxResponseBytes < 1024 || c.MaxResponseBytes > 256<<20 {
		return c, fmt.Errorf("RPC_MAX_RESPONSE_BYTES must be between 1024 and 268435456")
	}
	if raw := getenv("GOMEMLIMIT"); raw != "" && raw != "off" {
		limit, err := memoryBytes(raw)
		if err != nil {
			return c, err
		}
		if c.MaxResponseBytes > limit/4 {
			return c, fmt.Errorf("RPC_MAX_RESPONSE_BYTES must not exceed one quarter of GOMEMLIMIT; raise the container memory budget together with GOMEMLIMIT")
		}
	}
	if strings.ContainsAny(c.Listen, "\r\n") {
		return c, fmt.Errorf("invalid HTTP_ADDR")
	}
	return c, nil
}

func memoryBytes(raw string) (int64, error) {
	for _, unit := range []struct {
		suffix string
		factor int64
	}{
		{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"B", 1}, {"", 1},
	} {
		if !strings.HasSuffix(raw, unit.suffix) {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSuffix(raw, unit.suffix), 10, 64)
		if err == nil && n > 0 && n <= math.MaxInt64/unit.factor {
			return n * unit.factor, nil
		}
	}
	return 0, fmt.Errorf("GOMEMLIMIT must be off or a positive integer with an optional B/KiB/MiB/GiB/TiB suffix")
}
