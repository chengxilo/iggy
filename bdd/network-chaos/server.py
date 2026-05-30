# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
network-chaos: iptables sidecar for BDD reconnection tests.

Shares iggy-server's network namespace so iptables rules applied
here affect iggy-server's traffic.

Endpoints:
    POST /disrupt  - block all Iggy transport ports
    POST /resume   - remove the block rules
    GET  /status   - "disrupted" or "normal"
    GET  /health   - liveness probe
"""

import os
import subprocess
from http.server import HTTPServer, BaseHTTPRequestHandler

CHAIN = "IGGY_CHAOS"

RULES = [
    ["-p", "tcp", "--dport", "8090", "-j", "REJECT", "--reject-with", "tcp-reset"],
]


def iptables(*args):
    subprocess.run(["iptables", *args], check=True)


def setup_chain():
    subprocess.run(["iptables", "-N", CHAIN], capture_output=True)
    check = subprocess.run(["iptables", "-C", "INPUT", "-j", CHAIN], capture_output=True)
    if check.returncode != 0:
        iptables("-A", "INPUT", "-j", CHAIN)


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/disrupt":
            self._disrupt()
        elif self.path == "/resume":
            self._resume()
        else:
            self._respond(404, "not found")

    def do_GET(self):
        if self.path == "/status":
            self._status()
        elif self.path == "/health":
            self._respond(200, "ok")
        else:
            self._respond(404, "not found")

    def _disrupt(self):
        iptables("-F", CHAIN)
        for rule in RULES:
            iptables("-A", CHAIN, *rule)
        print("network disrupted", flush=True)
        self._respond(200, "disrupted")

    def _resume(self):
        iptables("-F", CHAIN)
        print("network resumed", flush=True)
        self._respond(200, "resumed")

    def _status(self):
        result = subprocess.run(
            ["iptables", "-L", CHAIN, "--line-numbers"],
            capture_output=True, text=True,
        )
        count = result.stdout.count("REJECT")
        self._respond(200, "disrupted" if count > 0 else "normal")

    def _respond(self, code, body):
        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(f"{body}\n".encode())

    def log_message(self, fmt, *args):
        print(f"network-chaos: {fmt % args}", flush=True)


if __name__ == "__main__":
    setup_chain()
    port = int(os.environ.get("LISTEN_PORT", "8475"))
    server = HTTPServer(("", port), Handler)
    print(f"network-chaos listening on :{port}", flush=True)
    server.serve_forever()
