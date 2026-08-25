"""A real broker for the tests to talk to.

There is no mock: the point of this library is interoperating with the C
broker's wire format, and a mock would only assert that it agrees with
itself.
"""
from __future__ import annotations

import http.client
import os
import random
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
BROKER = ROOT / "bin" / "sukkal"


def wait_for(predicate, timeout=5.0, message="timed out"):
    deadline = time.time() + timeout
    while True:
        try:
            if predicate():
                return
        except Exception:                              # noqa: BLE001
            pass
        if time.time() > deadline:
            raise AssertionError(message() if callable(message) else message)
        time.sleep(0.02)


@pytest.fixture(scope="session")
def broker():
    if not BROKER.exists():
        pytest.skip(f"run `make` first — no {BROKER}")

    port = random.randint(20000, 39999)
    directory = Path(tempfile.mkdtemp(prefix="sukkal-py-"))
    proc = subprocess.Popen(
        [str(BROKER), "serve", "--port", str(port), "--dir", str(directory / "data")],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )

    def healthy():
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
        try:
            conn.request("GET", "/health")
            return conn.getresponse().status == 200
        finally:
            conn.close()

    try:
        wait_for(healthy, 5.0, "broker did not start")
    except AssertionError:
        proc.kill()
        raise

    yield f"http://127.0.0.1:{port}"

    proc.send_signal(2)          # SIGINT, so it flushes on the way out
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
    shutil.rmtree(directory, ignore_errors=True)


@pytest.fixture
def client(broker):
    from sukkal import Client
    c = Client(broker, heartbeat_seconds=0)
    yield c
    c.close()


@pytest.fixture
def make_client(broker):
    """A second (or third) client, closed for you at the end."""
    from sukkal import Client
    made = []

    def factory(**kwargs):
        c = Client(broker, heartbeat_seconds=0, **kwargs)
        made.append(c)
        return c

    yield factory
    for c in made:
        c.close()
