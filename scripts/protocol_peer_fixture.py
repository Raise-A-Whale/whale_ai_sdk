#!/usr/bin/env python3
"""Deterministic incompatible stdio peer; never used by production clients.

WHALE_PROTOCOL_PEER_MODE selects old_peer, old_peer_ignore_term, early_eof,
eof, no_reply, or a shared fixture case.
WHALE_PROTOCOL_PEER_LOG records process lifecycle and received method names.
"""
import json
import os
from pathlib import Path
import signal
import sys

ROOT = Path(__file__).resolve().parents[1]
MODE = os.environ.get("WHALE_PROTOCOL_PEER_MODE", "old_peer")
LOG = os.environ.get("WHALE_PROTOCOL_PEER_LOG")
LANGUAGE = os.environ.get("WHALE_PROTOCOL_PEER_LANGUAGE", "root")


def record(event, **fields):
    if LOG:
        with open(LOG, "a", encoding="utf-8") as stream:
            stream.write(json.dumps(dict(event=event, pid=os.getpid(), mode=MODE,
                                         language=LANGUAGE, **fields)) + "\n")


def stop(signum, frame):
    raise SystemExit(0)


def main():
    fixture = json.loads((ROOT / "fixtures/protocol/initialization-v1.json").read_text())
    cases = {case["name"]: case["result"] for case in fixture["response_cases"]}
    if MODE not in cases and MODE not in ("old_peer", "old_peer_ignore_term", "early_eof", "eof", "no_reply"):
        raise ValueError(f"Unknown fixture mode: {MODE}")
    signal.signal(signal.SIGTERM, signal.SIG_IGN if MODE == "old_peer_ignore_term" else stop)
    signal.signal(signal.SIGINT, stop)
    record("started")
    try:
        if MODE == "early_eof":
            os.close(sys.stdout.fileno())
            record("stdout_closed")
        for line in sys.stdin:
            request = json.loads(line)
            method = request.get("method")
            record("request", method=method)
            if MODE == "eof":
                return
            if MODE in ("no_reply", "early_eof"):
                continue
            response = {"jsonrpc": "2.0", "id": request.get("id")}
            if method == "protocol.initialize" and MODE not in ("old_peer", "old_peer_ignore_term"):
                response["result"] = cases[MODE]
            elif method == "session.start_thread":
                # An incorrect fallback is observable as both a request and a fake session.
                response["result"] = {"thread_id": "unexpected-business-session", "created_at": "fixture"}
            else:
                response["error"] = {"code": -32601, "message": "Method not found"}
            print(json.dumps(response), flush=True)
    finally:
        record("exited")


if __name__ == "__main__":
    main()
