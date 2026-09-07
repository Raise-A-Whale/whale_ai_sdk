"""Transport layer for Whale AI SDK communication with whale-daemon."""

from __future__ import annotations

import abc
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import threading
from typing import Any, Callable, Dict, Optional, Union

logger = logging.getLogger("whale_ai_sdk.transport")


class Transport(abc.ABC):
    """Abstract base class for JSON-RPC transport layers."""

    @abc.abstractmethod
    def send(self, message: Union[Dict[str, Any], str]) -> None:
        """Sends a JSON-RPC message line to the daemon."""
        raise NotImplementedError

    @abc.abstractmethod
    def set_message_handler(self, handler: Callable[[Dict[str, Any]], None]) -> None:
        """Sets the incoming JSON-RPC message callback handler."""
        raise NotImplementedError

    @abc.abstractmethod
    def close(self) -> None:
        """Closes the transport connection and releases resources."""
        raise NotImplementedError

    @abc.abstractmethod
    def is_alive(self) -> bool:
        """Checks if the transport connection is active."""
        raise NotImplementedError


def find_daemon_binary(custom_path: Optional[Union[str, Path]] = None) -> Path:
    """Discovers the whale-daemon binary.

    Search priority:
    1. Explicitly provided custom_path.
    2. WHALE_DAEMON_PATH environment variable.
    3. Cargo target directories (debug/release) traversing up from current file or cwd.
    4. System PATH via shutil.which("whale-daemon").
    """
    if custom_path:
        p = Path(custom_path).expanduser().resolve()
        if p.is_file() and os.access(p, os.X_OK):
            return p
        raise FileNotFoundError(f"Specified whale-daemon binary not found or not executable: {p}")

    env_path = os.environ.get("WHALE_DAEMON_PATH")
    if env_path:
        p = Path(env_path).expanduser().resolve()
        if p.is_file() and os.access(p, os.X_OK):
            return p
        logger.warning("WHALE_DAEMON_PATH set to %s, but file not found or not executable", env_path)

    # Search upwards from current directory and file location for target/debug or target/release
    search_roots = [
        Path.cwd().resolve(),
        Path(__file__).resolve().parent,
    ]

    candidate_names = ["whale-daemon", "whale-daemon.exe"]
    for root in search_roots:
        curr = root
        for _ in range(6):  # Check up to 6 parent directories
            for variant in ("debug", "release"):
                for name in candidate_names:
                    candidate = curr / "target" / variant / name
                    if candidate.is_file() and os.access(candidate, os.X_OK):
                        return candidate
            if curr.parent == curr:
                break
            curr = curr.parent

    # System PATH
    path_bin = shutil.which("whale-daemon")
    if path_bin:
        return Path(path_bin).resolve()

    raise FileNotFoundError(
        "Could not locate 'whale-daemon' binary in PATH or cargo target directories. "
        "Please build the daemon with `cargo build -p whale-daemon` or specify WHALE_DAEMON_PATH."
    )


class SubprocessStdioTransport(Transport):
    """Transport communicating with whale-daemon via standard input/output streams."""

    def __init__(
        self,
        daemon_path: Optional[Union[str, Path]] = None,
        extra_args: Optional[list[str]] = None,
        log_level: str = "info",
    ) -> None:
        self.binary_path = find_daemon_binary(daemon_path)
        self._cmd = [
            str(self.binary_path),
            "--listen",
            "stdio",
            "--log-level",
            log_level,
        ]
        if extra_args:
            self._cmd.extend(extra_args)

        self._proc: Optional[subprocess.Popen[str]] = None
        self._handler: Optional[Callable[[Dict[str, Any]], None]] = None
        self._write_lock = threading.Lock()
        self._running = False
        self._reader_thread: Optional[threading.Thread] = None
        self._stderr_thread: Optional[threading.Thread] = None

        self._start_process()

    def _start_process(self) -> None:
        logger.debug("Starting whale-daemon subprocess: %s", self._cmd)
        self._proc = subprocess.Popen(
            self._cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,  # Line-buffered
        )
        self._running = True

        self._reader_thread = threading.Thread(
            target=self._read_stdout_loop,
            name="whale-daemon-stdout-reader",
            daemon=True,
        )
        self._reader_thread.start()

        self._stderr_thread = threading.Thread(
            target=self._read_stderr_loop,
            name="whale-daemon-stderr-reader",
            daemon=True,
        )
        self._stderr_thread.start()

    def set_message_handler(self, handler: Callable[[Dict[str, Any]], None]) -> None:
        self._handler = handler

    def send(self, message: Union[Dict[str, Any], str]) -> None:
        if not self.is_alive() or not self._proc or not self._proc.stdin:
            raise ConnectionError("whale-daemon process is not running or stdin is closed")

        if isinstance(message, dict):
            line = json.dumps(message)
        else:
            line = message.strip()

        with self._write_lock:
            try:
                self._proc.stdin.write(line + "\n")
                self._proc.stdin.flush()
                logger.debug("Sent line: %s", line)
            except (BrokenPipeError, OSError) as e:
                self._running = False
                raise ConnectionError(f"Failed to send to daemon: {e}") from e

    def _read_stdout_loop(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        while self._running:
            try:
                line = self._proc.stdout.readline()
                if not line:
                    logger.debug("Daemon stdout reached EOF")
                    break
                trimmed = line.strip()
                if not trimmed:
                    continue
                logger.debug("Received line: %s", trimmed)
                try:
                    data = json.loads(trimmed)
                except json.JSONDecodeError as e:
                    logger.warning("Failed to decode JSON from daemon stdout: %s (raw: %r)", e, trimmed)
                    continue

                if self._handler:
                    try:
                        self._handler(data)
                    except Exception as handler_err:
                        logger.exception("Error in JSON-RPC message handler: %s", handler_err)
            except (ValueError, OSError) as e:
                logger.debug("Error reading daemon stdout: %s", e)
                break

        self._running = False

    def _read_stderr_loop(self) -> None:
        assert self._proc is not None and self._proc.stderr is not None
        while self._running:
            try:
                line = self._proc.stderr.readline()
                if not line:
                    break
                trimmed = line.strip()
                if trimmed:
                    logger.info("[daemon-stderr] %s", trimmed)
            except Exception:
                break

    def is_alive(self) -> bool:
        return self._running and self._proc is not None and (self._proc.poll() is None)

    def close(self) -> None:
        self._running = False
        if self._proc:
            try:
                if self._proc.stdin:
                    self._proc.stdin.close()
            except Exception:
                pass

            try:
                self._proc.terminate()
                self._proc.wait(timeout=2.0)
            except Exception:
                try:
                    self._proc.kill()
                except Exception:
                    pass
            self._proc = None


class MockTransport(Transport):
    """In-memory mock transport for unit testing and deterministic simulation."""

    def __init__(self) -> None:
        self._handler: Optional[Callable[[Dict[str, Any]], None]] = None
        self._sent_messages: list[Dict[str, Any]] = []
        self._alive = True
        self._on_send_hook: Optional[Callable[[Dict[str, Any]], None]] = None

    def set_send_hook(self, hook: Callable[[Dict[str, Any]], None]) -> None:
        self._on_send_hook = hook

    def set_message_handler(self, handler: Callable[[Dict[str, Any]], None]) -> None:
        self._handler = handler

    def send(self, message: Union[Dict[str, Any], str]) -> None:
        if not self._alive:
            raise ConnectionError("MockTransport is closed")
        data = json.loads(message) if isinstance(message, str) else message
        self._sent_messages.append(data)
        if self._on_send_hook:
            self._on_send_hook(data)

    def simulate_incoming(self, message: Dict[str, Any]) -> None:
        """Simulate an incoming message from the daemon to the client."""
        if self._handler:
            self._handler(message)

    @property
    def sent_messages(self) -> list[Dict[str, Any]]:
        return list(self._sent_messages)

    def is_alive(self) -> bool:
        return self._alive

    def close(self) -> None:
        self._alive = False
