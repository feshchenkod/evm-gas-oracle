"""Linux Docker smoke: real read-only container, local RPC, no public chain."""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import threading
import tempfile
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", default="evm-gas-oracle:smoke")
    args = parser.parse_args()
    failing = threading.Event()

    class RPC(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if failing.is_set():
                self.send_error(503)
                return
            n = 2 if request["method"] == "eth_getBlockByNumber" else int(request["params"][0], 16) - 1
            result = {
                "number": hex(n), "hash": f"0x{n + 1:064x}", "parentHash": f"0x{n:064x}",
                "miner": "0x" + "11" * 20,
                "transactions": [
                    {"from": "0x" + "22" * 20, "gasPrice": "0x0"},
                    {"from": "0x" + "22" * 20, "gasPrice": "0x2faf080"},
                ],
            }
            body = json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": result}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    upstream = ThreadingHTTPServer(("127.0.0.1", 0), RPC)
    thread = threading.Thread(target=upstream.serve_forever, daemon=True)
    thread.start()
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    name = "evm-oracle-smoke-" + str(port)
    default_name = name + "-default"
    stub_name = name + "-rpc"
    network_name = name + "-network"
    workspace = tempfile.TemporaryDirectory(prefix="evm-oracle-smoke-")

    def docker(*command, **kwargs):
        return subprocess.run(["docker", *command], text=True, capture_output=True, **kwargs)

    def get(path):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=2) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    def wait_status(path, expected):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                status, body = get(path)
                if status == expected:
                    return body
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(0.1)
        raise AssertionError(f"{path} did not return {expected}")

    try:
        start = docker("run", "-d", "--name", name, "--network", "host", "--read-only",
                       "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
                       "--memory", "256m", "--pids-limit", "128", "--cpus", "2",
                       "-e", f"ETHEREUM_URL=http://127.0.0.1:{upstream.server_port}",
                       "-e", f"HTTP_ADDR=127.0.0.1:{port}",
                       "-e", "POLLING_FREQUENCY_SECONDS=1", "-e", "ZERO_GAS_PRICE_POLICY=ignore",
                       args.image)
        assert start.returncode == 0, start.stderr
        wait_status("/readyz", 200)
        status, body = get("/")
        payload = json.loads(body)
        assert status == 200 and payload["number_of_blocks"] == 3, payload
        assert payload["latest_block_number"] == 2 and payload["percentile_50"] == "0.05 nanoeth", payload
        assert len(payload) == 30, payload
        assert docker("exec", name, "/evm-gas-oracle", "-healthcheck").returncode == 0
        config = json.loads(docker("inspect", name).stdout)[0]
        assert config["Config"]["User"] == "65532:65532"
        assert config["HostConfig"]["ReadonlyRootfs"] is True
        # An upstream outage must fail readiness, not erase the compatible snapshot.
        failing.set()
        wait_status("/readyz", 503)
        assert get("/") == (200, body)
        assert get("/livez")[0] == 200
        assert docker("exec", name, "/evm-gas-oracle", "-healthcheck").returncode == 1
        failing.clear()
        wait_status("/readyz", 200)
        assert docker("stop", "--time", "5", name).returncode == 0
        state = json.loads(docker("inspect", name).stdout)[0]["State"]
        assert state["ExitCode"] == 0 and not state["OOMKilled"], state

        # Resolve the real Compose defaults with a long polling interval. An
        # unset STALE_AFTER must allow the binary to compute its own default.
        root = Path(__file__).resolve().parents[1]

        # Isolate the second RPC fixture in Docker too: connecting containers to
        # arbitrary host ports can be blocked by a host firewall.
        stub_binary = Path(workspace.name) / "rpcstub"
        image_arch = json.loads(docker("image", "inspect", args.image).stdout)[0]["Architecture"]
        subprocess.run(["go", "build", "-buildvcs=false", "-o", str(stub_binary), str(root / "tests/testdata/rpcstub.go")],
                       env=dict(os.environ, CGO_ENABLED="0", GOOS="linux", GOARCH=image_arch), check=True)
        assert docker("network", "create", network_name).returncode == 0
        fixture = docker("run", "-d", "--name", stub_name, "--network", network_name,
                         "--network-alias", "rpc", "--read-only", "--cap-drop", "ALL",
                         "--security-opt", "no-new-privileges:true",
                         "--mount", f"type=bind,src={stub_binary},dst=/rpcstub,readonly",
                         "--entrypoint", "/rpcstub", args.image)
        assert fixture.returncode == 0, fixture.stderr
        compose_env = dict(os.environ, ETHEREUM_URL="http://rpc:18545",
                           POLLING_FREQUENCY_SECONDS="300", ZERO_GAS_PRICE_POLICY="ignore", STALE_AFTER="",
                           BIND_ADDR="127.0.0.1", PORT="8080", GOMEMLIMIT="192MiB", MEMORY_LIMIT="256m",
                           RPC_MAX_RESPONSE_BYTES="33554432")
        resolved = json.loads(subprocess.check_output(
            ["docker", "compose", "-f", str(root / "docker-compose.yml"), "config", "--format", "json"],
            text=True, env=compose_env))["services"]["oracle"]
        assert resolved["environment"]["STALE_AFTER"] == "", resolved
        assert resolved["ports"][0]["host_ip"] == "127.0.0.1", resolved
        assert "HTTP_ADDR" not in resolved["environment"], resolved
        environment = [item for key, value in resolved["environment"].items() for item in ("-e", f"{key}={value}")]
        start = docker("run", "-d", "--name", default_name, "--network", network_name, "--read-only",
                       "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
                       "--memory", "256m", "--pids-limit", "128", "--cpus", "2",
                       "--sysctl", "net.ipv4.ip_unprivileged_port_start=0", "-p", "127.0.0.1::80",
                       *environment, args.image)
        assert start.returncode == 0, start.stderr
        config = json.loads(docker("inspect", default_name).stdout)[0]
        port = int(config["NetworkSettings"]["Ports"]["80/tcp"][0]["HostPort"])
        assert config["Config"]["User"] == "65532:65532" and config["HostConfig"]["ReadonlyRootfs"]
        wait_status("/readyz", 200)
        assert json.loads(get("/")[1])["percentile_50"] == "0.05 nanoeth"
        assert docker("exec", default_name, "/evm-gas-oracle", "-healthcheck").returncode == 0
        print("Container smoke passed: nonroot/read-only, default :80, Compose polling=300, BSC samples, RPC outage/recovery, graceful stop")
    except BaseException:
        for container in (name, default_name, stub_name):
            logs = docker("logs", container)
            print(logs.stdout)
            print(logs.stderr)
        raise
    finally:
        docker("rm", "-f", name, default_name, stub_name)
        docker("network", "rm", network_name)
        workspace.cleanup()
        upstream.shutdown()
        upstream.server_close()
        thread.join(timeout=5)


if __name__ == "__main__":
    main()
