"""Cross-process claims and durable terminal-message replay."""

import asyncio
import base64
import json
import os
import tempfile
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import TracebackType
from typing import Any, Protocol
from uuid import UUID

import portalocker

from graphx_workers.runtime.errors import RuntimeCapacityError
from graphx_workers.runtime.terminal import PreparedTerminalMessage


class ReplayStoreCapacityError(RuntimeCapacityError):
    """The replay journal cannot safely reserve another command."""

    def __init__(
        self,
        message: str,
        *,
        entries: int | None = None,
        bytes_used: int | None = None,
    ) -> None:
        super().__init__(message)
        self.entries = entries
        self.bytes_used = bytes_used


class ReplayEntryTooLargeError(ReplayStoreCapacityError):
    """A terminal message cannot fit within the configured per-entry quota."""


@dataclass(frozen=True, slots=True)
class StoredTerminalMessage:
    command_fingerprint: str
    message: PreparedTerminalMessage


@dataclass(frozen=True, slots=True)
class ReplayStoreStats:
    entries: int
    bytes_used: int


@dataclass(slots=True)
class _JournalState:
    committed_entries: int
    bytes_used: int
    pending: dict[str, float]


class ReplayClaim(Protocol):
    async def __aenter__(self) -> None: ...

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...


class TerminalReplayStore(Protocol):
    async def load(self, command_id: UUID) -> StoredTerminalMessage | None: ...

    def entry_fits(
        self,
        command_fingerprint: str,
        message: PreparedTerminalMessage,
    ) -> bool: ...

    def claim(
        self,
        command_id: UUID,
        *,
        timeout: float,
    ) -> ReplayClaim: ...

    async def put_if_absent(
        self,
        command_id: UUID,
        command_fingerprint: str,
        message: PreparedTerminalMessage,
    ) -> StoredTerminalMessage: ...

    async def cleanup(self) -> ReplayStoreStats: ...


class _VolatileClaim:
    def __init__(self, lock: asyncio.Lock) -> None:
        self._lock = lock

    async def __aenter__(self) -> None:
        await self._lock.acquire()

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exception_type, exception, traceback
        self._lock.release()


class VolatileTerminalReplayStore:
    """Process-local store for tests; production adapters require durable storage."""

    def __init__(self) -> None:
        self._entries: dict[UUID, StoredTerminalMessage] = {}
        self._claims: dict[UUID, asyncio.Lock] = {}
        self._lock = asyncio.Lock()

    async def load(self, command_id: UUID) -> StoredTerminalMessage | None:
        async with self._lock:
            return self._entries.get(command_id)

    def entry_fits(
        self,
        command_fingerprint: str,
        message: PreparedTerminalMessage,
    ) -> bool:
        del command_fingerprint, message
        return True

    def claim(
        self,
        command_id: UUID,
        *,
        timeout: float,
    ) -> ReplayClaim:
        del timeout
        return _VolatileClaim(self._claims.setdefault(command_id, asyncio.Lock()))

    async def put_if_absent(
        self,
        command_id: UUID,
        command_fingerprint: str,
        message: PreparedTerminalMessage,
    ) -> StoredTerminalMessage:
        async with self._lock:
            stored = StoredTerminalMessage(command_fingerprint, message)
            return self._entries.setdefault(command_id, stored)

    async def cleanup(self) -> ReplayStoreStats:
        async with self._lock:
            return ReplayStoreStats(
                entries=len(self._entries),
                bytes_used=sum(len(entry.message.body) for entry in self._entries.values()),
            )


class _FileClaim:
    def __init__(
        self,
        store: FileTerminalReplayStore,
        command_id: UUID,
        timeout: float,
    ) -> None:
        self._store = store
        self._command_id = command_id
        self._timeout = max(timeout, 0.0)
        self._lock: Any | None = None

    async def __aenter__(self) -> None:
        path = await asyncio.to_thread(
            self._store._reserve_claim,
            self._command_id,
        )
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._timeout
        while True:
            attempt = asyncio.create_task(asyncio.to_thread(self._store._try_lock, path))
            try:
                lock = await asyncio.shield(attempt)
            except asyncio.CancelledError:
                lock = await attempt
                if lock is not None:
                    await asyncio.to_thread(lock.release)
                raise
            if lock is not None:
                validation = asyncio.create_task(
                    asyncio.to_thread(self._store._claim_is_current, lock, path)
                )
                try:
                    is_current = await asyncio.shield(validation)
                except asyncio.CancelledError:
                    await validation
                    await asyncio.to_thread(lock.release)
                    raise
                if not is_current:
                    await asyncio.to_thread(lock.release)
                    path = await asyncio.to_thread(
                        self._store._reserve_claim,
                        self._command_id,
                    )
                    continue
                self._lock = lock
                return
            if loop.time() >= deadline:
                raise TimeoutError("timed out waiting for terminal replay claim")
            await asyncio.sleep(min(0.05, max(deadline - loop.time(), 0.0)))

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exception_type, exception, traceback
        if self._lock is not None:
            lock = self._lock
            self._lock = None
            await asyncio.shield(asyncio.to_thread(lock.release))


class FileTerminalReplayStore:
    """Immutable journal with process-recoverable claims and bounded retention."""

    _GLOBAL_LOCK = "_journal.lock"
    _LAST_CLEANUP = "_last-cleanup"
    _STATE = "_journal-state.json"

    def __init__(
        self,
        root: Path,
        *,
        retention: timedelta,
        cleanup_interval: timedelta,
        max_entries: int,
        max_bytes: int,
        max_entry_bytes: int,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._root = root
        self._retention = retention
        self._cleanup_interval = cleanup_interval
        self._max_entries = max_entries
        self._max_bytes = max_bytes
        self._max_entry_bytes = max_entry_bytes
        self._clock = clock or (lambda: datetime.now(UTC))

    async def load(self, command_id: UUID) -> StoredTerminalMessage | None:
        return await asyncio.to_thread(self._load, command_id)

    def entry_fits(
        self,
        command_fingerprint: str,
        message: PreparedTerminalMessage,
    ) -> bool:
        return len(self._serialize_entry(command_fingerprint, message)) <= self._max_entry_bytes

    def claim(
        self,
        command_id: UUID,
        *,
        timeout: float,
    ) -> ReplayClaim:
        return _FileClaim(self, command_id, timeout)

    async def put_if_absent(
        self,
        command_id: UUID,
        command_fingerprint: str,
        message: PreparedTerminalMessage,
    ) -> StoredTerminalMessage:
        return await asyncio.to_thread(
            self._put_if_absent,
            command_id,
            command_fingerprint,
            message,
        )

    async def cleanup(self) -> ReplayStoreStats:
        return await asyncio.to_thread(self._cleanup)

    def _load(self, command_id: UUID) -> StoredTerminalMessage | None:
        path = self._result_path(command_id)
        try:
            raw = path.read_bytes()
        except FileNotFoundError:
            return None
        try:
            document = json.loads(raw)
            return StoredTerminalMessage(
                command_fingerprint=str(document["command_fingerprint"]),
                message=PreparedTerminalMessage(
                    message_id=UUID(document["message_id"]),
                    message_type=str(document["message_type"]),
                    correlation_id=UUID(document["correlation_id"]),
                    body=base64.b64decode(document["body"], validate=True),
                ),
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise ValueError(f"invalid terminal replay entry for command {command_id}") from error

    def _reserve_claim(self, command_id: UUID) -> Path:
        self._root.mkdir(parents=True, exist_ok=True)
        with portalocker.Lock(self._global_lock_path(), mode="a+b", timeout=10):
            self._cleanup_if_due_locked()
            state = self._load_state_locked()
            result_path = self._result_path(command_id)
            if result_path.exists():
                self._commit_pending_locked(state, command_id, result_path)
                return self._claim_path(command_id)
            claim_path = self._claim_path(command_id)
            command_key = str(command_id)
            if command_key not in state.pending:
                entries = state.committed_entries + len(state.pending)
                if entries >= self._max_entries:
                    raise ReplayStoreCapacityError(
                        f"terminal replay entry quota reached: {entries}/{self._max_entries}",
                        entries=entries,
                        bytes_used=state.bytes_used,
                    )
                reserved_bytes = state.bytes_used + len(state.pending) * self._max_entry_bytes
                if reserved_bytes + self._max_entry_bytes > self._max_bytes:
                    raise ReplayStoreCapacityError(
                        f"terminal replay byte quota reached: {reserved_bytes}/{self._max_bytes}",
                        entries=entries,
                        bytes_used=state.bytes_used,
                    )
                state.pending[command_key] = self._clock().timestamp()
                self._write_state_locked(state)
            if not claim_path.exists():
                claim_path.touch(exist_ok=True)
            return claim_path

    def _put_if_absent(
        self,
        command_id: UUID,
        command_fingerprint: str,
        message: PreparedTerminalMessage,
    ) -> StoredTerminalMessage:
        content = self._serialize_entry(command_fingerprint, message)
        if len(content) > self._max_entry_bytes:
            raise ReplayEntryTooLargeError(
                f"terminal replay entry exceeds {self._max_entry_bytes} bytes"
            )
        target = self._result_path(command_id)
        if target.exists():
            stored = self._load(command_id)
            if stored is None:
                raise RuntimeError("terminal replay entry disappeared")
            return stored

        with portalocker.Lock(self._global_lock_path(), mode="a+b", timeout=10):
            state = self._load_state_locked()
            if target.exists():
                self._commit_pending_locked(state, command_id, target)
                stored = self._load(command_id)
                if stored is None:
                    raise RuntimeError("terminal replay entry disappeared")
                return stored
            command_key = str(command_id)
            reserved_bytes = state.bytes_used + len(state.pending) * self._max_entry_bytes
            claim_reservation = self._max_entry_bytes if command_key in state.pending else 0
            if reserved_bytes - claim_reservation + len(content) > self._max_bytes:
                raise ReplayStoreCapacityError(
                    f"terminal replay byte quota reached: {reserved_bytes}/{self._max_bytes}",
                    entries=state.committed_entries + len(state.pending),
                    bytes_used=state.bytes_used,
                )
            self._write_immutable(target, content)
            self._commit_pending_locked(state, command_id, target)

        stored = self._load(command_id)
        if stored is None:
            raise RuntimeError("terminal replay entry was not persisted")
        return stored

    @staticmethod
    def _serialize_entry(
        command_fingerprint: str,
        message: PreparedTerminalMessage,
    ) -> bytes:
        document = {
            "command_fingerprint": command_fingerprint,
            "message_id": str(message.message_id),
            "message_type": message.message_type,
            "correlation_id": str(message.correlation_id),
            "body": base64.b64encode(message.body).decode("ascii"),
        }
        return json.dumps(document, separators=(",", ":"), sort_keys=True).encode()

    def _cleanup(self) -> ReplayStoreStats:
        self._root.mkdir(parents=True, exist_ok=True)
        with portalocker.Lock(self._global_lock_path(), mode="a+b", timeout=10):
            self._cleanup_locked()
            return self._stats_from_state(self._load_state_locked())

    def _cleanup_if_due_locked(self) -> None:
        marker = self._root / self._LAST_CLEANUP
        now = self._clock().timestamp()
        try:
            last_cleanup = marker.stat().st_mtime
        except FileNotFoundError:
            last_cleanup = 0.0
        if now - last_cleanup >= self._cleanup_interval.total_seconds():
            self._cleanup_locked()

    def _cleanup_locked(self) -> None:
        cutoff = self._clock().timestamp() - self._retention.total_seconds()
        for result_path in self._root.glob("*.json"):
            if result_path.name == self._STATE:
                continue
            if result_path.stat().st_mtime >= cutoff:
                continue
            self._remove_expired(result_path.stem, result_path)
        for claim_path in self._root.glob("*.lock"):
            if claim_path.name.startswith("_") or claim_path.stat().st_mtime >= cutoff:
                continue
            if not self._result_path(UUID(claim_path.stem)).exists():
                self._remove_expired(claim_path.stem, None)
        (self._root / self._LAST_CLEANUP).touch()
        self._reconcile_locked()

    def _remove_expired(self, command_stem: str, result_path: Path | None) -> None:
        claim_path = self._root / f"{command_stem}.lock"
        lock = self._try_lock(claim_path)
        if lock is None:
            return
        try:
            if result_path is not None:
                result_path.unlink(missing_ok=True)
        finally:
            lock.release()
        claim_path.unlink(missing_ok=True)

    def _load_state_locked(self) -> _JournalState:
        try:
            document = json.loads((self._root / self._STATE).read_bytes())
            pending = {
                str(UUID(command_id)): float(created_at)
                for command_id, created_at in dict(document["pending"]).items()
            }
            return _JournalState(
                committed_entries=int(document["committed_entries"]),
                bytes_used=int(document["bytes_used"]),
                pending=pending,
            )
        except FileNotFoundError, KeyError, TypeError, ValueError, json.JSONDecodeError:
            return self._reconcile_locked()

    def _write_state_locked(self, state: _JournalState) -> None:
        content = json.dumps(
            {
                "version": 1,
                "committed_entries": state.committed_entries,
                "bytes_used": state.bytes_used,
                "pending": state.pending,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        self._write_replace(self._root / self._STATE, content)

    def _reconcile_locked(self) -> _JournalState:
        results = tuple(path for path in self._root.glob("*.json") if path.name != self._STATE)
        try:
            previous = json.loads((self._root / self._STATE).read_bytes())
            previous_pending = dict(previous.get("pending", {}))
        except FileNotFoundError, TypeError, ValueError, json.JSONDecodeError:
            previous_pending = {}
        completed_commands = {path.stem for path in results}
        pending: dict[str, float] = {}
        for command_id, created_at in previous_pending.items():
            try:
                normalized_id = str(UUID(command_id))
            except TypeError, ValueError:
                continue
            if normalized_id in completed_commands:
                continue
            if self._claim_path(UUID(normalized_id)).exists():
                pending[normalized_id] = float(created_at)
        for claim_path in self._root.glob("*.lock"):
            if claim_path.name.startswith("_") or claim_path.stem in completed_commands:
                continue
            try:
                normalized_id = str(UUID(claim_path.stem))
            except ValueError:
                continue
            pending.setdefault(normalized_id, claim_path.stat().st_mtime)
        state = _JournalState(
            committed_entries=len(results),
            bytes_used=sum(path.stat().st_size for path in results),
            pending=pending,
        )
        self._write_state_locked(state)
        return state

    def _commit_pending_locked(
        self,
        state: _JournalState,
        command_id: UUID,
        target: Path,
    ) -> None:
        if state.pending.pop(str(command_id), None) is None:
            return
        state.committed_entries += 1
        state.bytes_used += target.stat().st_size
        self._write_state_locked(state)

    @staticmethod
    def _stats_from_state(state: _JournalState) -> ReplayStoreStats:
        return ReplayStoreStats(
            entries=state.committed_entries + len(state.pending),
            bytes_used=state.bytes_used,
        )

    @staticmethod
    def _try_lock(path: Path) -> Any | None:
        lock = portalocker.Lock(path, mode="a+b", timeout=0, fail_when_locked=True)
        try:
            lock.acquire()
        except portalocker.AlreadyLocked:
            return None
        return lock

    def _claim_is_current(self, lock: Any, path: Path) -> bool:
        with portalocker.Lock(self._global_lock_path(), mode="a+b", timeout=10):
            try:
                path_stat = path.stat()
                handle_stat = os.fstat(lock.fh.fileno())
            except AttributeError, FileNotFoundError, OSError:
                return False
            return (path_stat.st_dev, path_stat.st_ino) == (
                handle_stat.st_dev,
                handle_stat.st_ino,
            )

    def _write_immutable(self, target: Path, content: bytes) -> None:
        temporary: str | None = None
        try:
            with tempfile.NamedTemporaryFile(dir=self._root, delete=False) as handle:
                temporary = handle.name
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            with suppress(FileExistsError):
                os.link(temporary, target)
                self._sync_directory()
        finally:
            if temporary is not None:
                Path(temporary).unlink(missing_ok=True)

    def _write_replace(self, target: Path, content: bytes) -> None:
        temporary: str | None = None
        try:
            with tempfile.NamedTemporaryFile(dir=self._root, delete=False) as handle:
                temporary = handle.name
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
            temporary = None
            self._sync_directory()
        finally:
            if temporary is not None:
                Path(temporary).unlink(missing_ok=True)

    def _sync_directory(self) -> None:
        if os.name == "nt":
            return
        directory_flag = int(os.__dict__.get("O_DIRECTORY", 0))
        descriptor = os.open(self._root, os.O_RDONLY | directory_flag)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _result_path(self, command_id: UUID) -> Path:
        return self._root / f"{command_id}.json"

    def _claim_path(self, command_id: UUID) -> Path:
        return self._root / f"{command_id}.lock"

    def _global_lock_path(self) -> Path:
        return self._root / self._GLOBAL_LOCK
