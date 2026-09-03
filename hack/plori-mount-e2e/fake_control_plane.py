#!/usr/bin/env python3
"""A stand-in for the control-plane's /v1/internal/storage routes.

It is not a model of the server; it is the smallest thing that lets the worker
walk its whole lifecycle in CI. It records which routes were called, so the
test can assert that the ordered shutdown really reached /lease/release, and it
refuses a request whose bearer token is not the one on disk, so a worker that
caches the rotating token fails here rather than in production.

Usage: fake_control_plane.py PORT TOKEN_FILE JOURNAL_FILE
"""

import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = int(sys.argv[1])
TOKEN_FILE = sys.argv[2]
JOURNAL = sys.argv[3]

# Long enough that the worker never trips its own write-stop margin during the
# test, short enough that a broken renew loop still shows up as a fence trip.
LEASE_TTL_SECONDS = 120
GRANT = {"bytes": 1 << 30, "inodes": 100000, "epoch": 1, "acked_epoch": 0}


def now_plus(seconds):
    import datetime

    return (
        datetime.datetime.now(datetime.timezone.utc)
        + datetime.timedelta(seconds=seconds)
    ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # keep the CI log readable
        pass

    def _record(self, route):
        with open(JOURNAL, "a", encoding="utf-8") as fh:
            fh.write(route + "\n")

    def _json(self, status, body):
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        payload = self.rfile.read(length)
        route = self.path

        with open(TOKEN_FILE, encoding="utf-8") as fh:
            expected = "Bearer " + fh.read().strip()
        if self.headers.get("Authorization") != expected:
            self._record(route + " UNAUTHORIZED")
            self._json(401, {"code": "token_invalid", "error": "bad bearer token"})
            return

        if route.endswith("/format-ack"):
            # The one route whose BODY the harness asserts on. /format-ack is
            # what tells the control-plane which filesystem a freshly formatted
            # volume is; a worker that mounts without making this call leaves
            # the volume `allocating` forever, and the Files router serves the
            # Agent out of the other storage plane (PLO-420). Recording the
            # uuid is what lets run.sh build the SECOND publish out of what the
            # control-plane learned rather than out of the local metadata.
            body = json.loads(payload or b"{}")
            self.server.format_uuid = body.get("format_uuid", "")
            self._record(
                "%s uuid=%s epoch=%s"
                % (route, self.server.format_uuid, body.get("fence_epoch"))
            )
            self._json(
                200,
                {
                    "storage_volume_id": "e2e",
                    "state": "active",
                    "grant": GRANT,
                    "used_bytes": 0,
                    "used_inodes": 0,
                },
            )
            return

        self._record(route)
        if route.endswith("/lease/renew"):
            self._json(
                200,
                {
                    "storage_volume_id": "e2e",
                    "fence_epoch": 1,
                    "lease_expires_at": now_plus(LEASE_TTL_SECONDS),
                    "grant": GRANT,
                    "released": False,
                },
            )
        elif route.endswith("/lease/release"):
            self._json(200, {"storage_volume_id": "e2e", "released": True})
        else:
            self._json(
                200,
                {
                    "storage_volume_id": "e2e",
                    "state": "active",
                    "grant": GRANT,
                    "used_bytes": 0,
                    "used_inodes": 0,
                },
            )


if __name__ == "__main__":
    server = HTTPServer(("127.0.0.1", PORT), Handler)
    # The Format.UUID the worker acknowledged, held the way the real control
    # plane holds it: on the server, learned from the worker, not from the
    # metadata database the harness could read for itself.
    server.format_uuid = ""
    server.serve_forever()
