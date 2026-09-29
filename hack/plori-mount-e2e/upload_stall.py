#!/usr/bin/env python3
"""Local real-process upload watchdog proof (requires boto3, FUSE and MinIO).

Usage: upload_stall.py JUICEFS LITESTREAM MINIO OUTPUT_DIR
All servers bind loopback ephemeral ports. Only metadata PUTs are stalled.
The output directory retains logs and timing evidence; child processes are reaped.
"""
import contextlib
import datetime as dt
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

import boto3

from replication_faults import events, matching, require, timestamp, wait_for


def main():
    juicefs, litestream, minio, output = sys.argv[1:]
    out = Path(output).resolve()
    out.mkdir(parents=True, exist_ok=True)
    stalled = threading.Event()
    closing = threading.Event()
    uploads = []
    renewals = []
    processes = []
    servers = []
    handles = []
    fd = None

    def start(args, name, env=None):
        log = (out / (name + '.log')).open('w')
        handles.append(log)
        p = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT, env=env)
        processes.append(p)
        return p

    def serve(handler):
        server = ThreadingHTTPServer(('127.0.0.1', 0), handler)
        servers.append(server)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        return server.server_port

    def expires():
        return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=120)).isoformat()

    grant = {'bytes': 1 << 30, 'inodes': 100000, 'epoch': 1, 'acked_epoch': 0}

    class Control(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers.get('Content-Length', 0))) or '{}')
            response = {'state': 'active', 'grant': grant}
            if self.path.endswith('/lease/renew'):
                expiry = expires()
                renewals.append({'at': time.time(), 'expiry': expiry})
                response.update(storage_volume_id=body['volume_id'], fence_epoch=body['fence_epoch'], lease_expires_at=expiry)
            data = json.dumps(response).encode()
            self.send_response(200)
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    # Reserve an ephemeral port until immediately before starting MinIO.
    import socket
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        s3port = sock.getsockname()[1]

    class Proxy(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *_):
            pass

        def forward(self):
            if self.command == 'PUT' and '/agents-meta/' in self.path and stalled.is_set():
                uploads.append({'at': time.time(), 'path': self.path})
                while stalled.is_set() and not closing.wait(0.05):
                    pass
                # No acknowledgement and no forwarding while the fault is armed.
                if closing.is_set():
                    self.close_connection = True
                    return
            conn = http.client.HTTPConnection('127.0.0.1', s3port, timeout=10)
            try:
                body = self.rfile.read(int(self.headers.get('Content-Length', 0)))
                conn.request(self.command, self.path, body, dict(self.headers))
                response = conn.getresponse()
                data = response.read()
                self.send_response(response.status)
                for key, value in response.getheaders():
                    if key.lower() not in ('transfer-encoding', 'connection', 'content-length'):
                        self.send_header(key, value)
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            except (OSError, http.client.HTTPException):
                self.close_connection = True
            finally:
                conn.close()

        do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = forward

    try:
        # These are disposable local fixture credentials, not external secrets.
        env = dict(os.environ, MINIO_ROOT_USER='forkwdlocal', MINIO_ROOT_PASSWORD='forkwdlocalpassword',
                   AWS_ACCESS_KEY_ID='forkwdlocal', AWS_SECRET_ACCESS_KEY='forkwdlocalpassword', AWS_REGION='us-east-1')
        start([minio, 'server', str(out / 'objects'), '--address', f'127.0.0.1:{s3port}', '--console-address', '127.0.0.1:0'], 'minio', env)
        client = boto3.client('s3', endpoint_url=f'http://127.0.0.1:{s3port}',
                              aws_access_key_id='forkwdlocal', aws_secret_access_key='forkwdlocalpassword', region_name='us-east-1')
        def s3_ready():
            try:
                client.list_buckets()
                return True
            except Exception:
                return False
        wait_for(s3_ready, 30, 'MinIO did not start')
        client.create_bucket(Bucket='watchdog')
        endpoint = f'http://127.0.0.1:{serve(Proxy)}'
        cpport = serve(Control)
        for name in ('mnt', 'state', 'cache'):
            (out / name).mkdir()
        token = out / 'token'
        with os.fdopen(os.open(token, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as f:
            f.write('local-fixture-token')
        spec = {
            'storage_volume_id': 'watchdog', 'format_uuid': '', 'generation': 1, 'volume_state': 'allocating',
            'fence_epoch': 1, 'lease_expires_at': expires(), 'lease_renew_interval': '2s', 'write_stop_margin': '20s',
            'data_prefix': 'agents/watchdog/', 'meta_prefix': 'agents-meta/watchdog/g1/',
            'fence_marker_key': 'agents-meta/watchdog/g1/fence', 'grant': grant,
            'object_store': {'endpoint': endpoint, 'bucket': 'watchdog', 'region': 'us-east-1', 'credential_source': 'node_secret'},
            'format': {'volume_id': 'watchdog', 'bucket': endpoint + '/watchdog', 'data_prefix': 'agents/watchdog/',
                       'meta_prefix': 'agents-meta/watchdog/', 'trash_days': 1, 'capacity_bytes': 1 << 30, 'inodes': 100000, 'grant_epoch': 1},
            'may_format': True, 'mount_options': ['writeback', 'heartbeat=30', 'barrier_interval=5'], 'issued_at': expires(),
        }
        (out / 'spec.json').write_text(json.dumps(spec))
        worker = start([juicefs, 'plori-mount', '--spec-file', str(out / 'spec.json'), '--mount-point', str(out / 'mnt'),
                        '--state-dir', str(out / 'state'), '--cache-dir', str(out / 'cache'),
                        '--control-plane-url', f'http://127.0.0.1:{cpport}', '--token-file', str(token), '--litestream-bin', litestream], 'mount', env)
        log = out / 'mount.log'
        def ready():
            require(worker.poll() is None, 'mount exited before ready; see mount.log')
            return (out / 'state/ready').exists()
        wait_for(ready, 90, 'mount not ready')
        fd = os.open(out / 'mnt/held-open', os.O_CREAT | os.O_RDWR, 0o600)
        os.write(fd, b'initial\n')
        os.fsync(fd)
        time.sleep(12)
        require(not matching(events(log), 'replication_probe_failed'), 'idle mount failed its probe')

        # A short upload stall must start recovery and then return to healthy.
        stalled.set()
        (out / 'mnt/pending-short').mkdir()
        wait_for(lambda: matching(events(log), 'replication_probe_failed'), 25, 'upload stall went undetected')
        require(uploads, 'no metadata upload reached the fault proxy')
        stalled.clear()
        wait_for(lambda: matching(events(log), 'replication_recovered'), 25, 'short stall did not recover')
        require(worker.poll() is None, 'mount exited during short stall')
        recovered_at = timestamp(matching(events(log), 'replication_recovered')[-1])
        print('PASS idle, pending upload detected, short stall recovered', flush=True)

        # A persistent stall must survive child restart and exhaust the window.
        stalled.set()
        (out / 'mnt/pending-long').mkdir()
        def failures():
            return [e for e in matching(events(log), 'replication_probe_failed') if timestamp(e) > recovered_at]
        wait_for(failures, 25, 'persistent upload stall went undetected')
        first = timestamp(failures()[0])
        wait_for(lambda: matching(events(log), 'replication_failed_stop'), 40, 'recovery window did not close')
        stop_at = timestamp(matching(events(log), 'replication_failed_stop')[0])
        require(29 <= stop_at - first <= 35, f'wrong recovery window: {stop_at-first}')
        require(matching(events(log), 'replication_restarted'), 'missing child restart')
        during = [r['at'] for r in renewals if first <= r['at'] <= stop_at]
        require(len(during) >= 5, 'replication work blocked lease renewal during recovery')
        require(max(b-a for a, b in zip(during, during[1:])) < 5, 'lease scheduler stalled during recovery')
        time.sleep(3)
        stopped_count = len(renewals)
        require(renewals[-1]['at'] <= stop_at + 1, 'lease renewed after recovery window')
        expiry = dt.datetime.fromisoformat(renewals[-1]['expiry']).timestamp()
        wait_for(lambda: time.time() > expiry + 1, 130, 'lease did not expire')
        require(len(renewals) == stopped_count, 'lease renewal continued after stop')
        try:
            os.write(fd, b'after-expiry\n')
            os.fsync(fd)
        except OSError as error:
            write_errno = error.errno
        else:
            raise AssertionError('write through held FUSE descriptor succeeded after expiry')
        stalled.clear()
        wait_for(lambda: worker.poll() is not None, 35, 'worker did not finish bounded shutdown')
        require(worker.returncode == 69, f'worker exit {worker.returncode}, expected 69')
        terminal = matching(events(log), 'plori_mount_terminal')
        require(len(terminal) == 1 and terminal[0].get('error') == 'E_REPLICATION_FAILED',
                f'wrong terminal event: {terminal}')
        receipt = {'first_failure': first, 'stop': stop_at, 'window_seconds': stop_at-first,
                   'last_renewal': renewals[-1], 'renewals': renewals, 'blocked_uploads': uploads,
                   'post_expiry_write_errno': write_errno, 'exit': worker.returncode}
        (out / 'receipt.json').write_text(json.dumps(receipt, indent=2))
        print(f'PASS persistent stall: window={stop_at-first:.3f}s, renewals stopped, post-expiry errno={write_errno}, exit=69', flush=True)
    finally:
        closing.set()
        stalled.clear()
        if fd is not None:
            # FUSE flush on close can also fail after the worker exits.
            with contextlib.suppress(OSError):
                os.close(fd)
        for proc in reversed(processes):
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=40)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
        for server in servers:
            server.shutdown()
            server.server_close()
        for handle in handles:
            handle.close()
        if os.path.ismount(out / 'mnt'):
            subprocess.run(['fusermount3', '-u', str(out / 'mnt')], check=True)
        (out / 'token').unlink(missing_ok=True)
        print('Stopped and reaped fixture processes:', [p.pid for p in processes], flush=True)


if __name__ == '__main__':
    main()
