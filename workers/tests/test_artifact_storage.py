import asyncio
import io
import logging
import subprocess
import sys
import tempfile
import threading
import warnings
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, cast
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import pytest
from botocore.exceptions import (  # type: ignore[import-untyped]
    ClientError,
    EndpointConnectionError,
)

from graphx_workers.config import WorkerSettings
from graphx_workers.contracts.messages import (
    CollectLimits,
    SourceCollectCompleted,
    SourceCollectRequested,
    SourceCollectRequestedPayload,
    SourceQuery,
    parse_source_message,
)
from graphx_workers.contracts.schema import ContractSchemaRegistry, decode_contract_json
from graphx_workers.runtime.artifacts import (
    ArtifactPersistenceDrainError,
    ArtifactPolicyError,
    ArtifactSession,
    sanitize_http_metadata,
    sanitize_original_filename,
)
from graphx_workers.runtime.context import ArtifactContextFactory
from graphx_workers.runtime.errors import RuntimeFatalError
from graphx_workers.runtime.finalization import ManifestResultFinalizer
from graphx_workers.runtime.processor import AdapterRegistration, SourceCommandProcessor
from graphx_workers.runtime.replay import StoredTerminalMessage, VolatileTerminalReplayStore
from graphx_workers.runtime.s3 import (
    ImmutableS3Store,
    ObjectConflictError,
    ObjectIntegrityError,
    ObjectReconciliationError,
    ObjectStoreCapacityError,
    ObjectStoreOperationError,
)
from graphx_workers.runtime.terminal import PreparedTerminalMessage, TerminalMessageFactory
from graphx_workers.sources.fixture import FixtureAdapter

NOW = datetime(2026, 11, 20, tzinfo=UTC)


def test_hung_daemon_s3_call_does_not_block_interpreter_shutdown() -> None:
    script = """
import asyncio
import threading
from graphx_workers.runtime.s3 import _run_daemon

async def main():
    slots = threading.BoundedSemaphore(1)
    task = asyncio.create_task(_run_daemon(slots, threading.Event().wait))
    await asyncio.sleep(0.01)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

asyncio.run(main())
"""
    subprocess.run([sys.executable, "-c", script], check=True, timeout=5)


def test_hung_download_does_not_block_interpreter_shutdown() -> None:
    script = """
import asyncio
import threading
from graphx_workers.runtime.s3 import ImmutableS3Store

class Body:
    def read(self, size):
        threading.Event().wait()
    def close(self):
        pass

class Client:
    def get_object(self, **kwargs):
        return {"Body": Body()}

async def main():
    store = ImmutableS3Store(Client(), "bucket", require_versioning=False)
    task = asyncio.create_task(anext(store.iter_bytes("key")))
    await asyncio.sleep(0.01)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

asyncio.run(main())
"""
    subprocess.run([sys.executable, "-c", script], check=True, timeout=5)


class MemoryS3Client:
    def __init__(self, *, versioning: str | None = None) -> None:
        self.versioning = versioning
        self.objects: dict[str, tuple[bytes, str, dict[str, str]]] = {}
        self.lock = threading.Lock()
        self.lose_put_response = False
        self.conditional_conflict_status = 412
        self.hidden_head_requests = 0
        self.head_service_errors: list[tuple[int, str]] = []

    def head_bucket(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        return {}

    def get_bucket_versioning(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        return {} if self.versioning is None else {"Status": self.versioning}

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        key = kwargs["Key"]
        body = kwargs["Body"]
        content = body.read()
        with self.lock:
            if key in self.objects:
                status = self.conditional_conflict_status
                raise ClientError(
                    {
                        "Error": {
                            "Code": (
                                "ConditionalRequestConflict"
                                if status == 409
                                else "PreconditionFailed"
                            )
                        },
                        "ResponseMetadata": {"HTTPStatusCode": status},
                    },
                    "PutObject",
                )
            self.objects[key] = (content, kwargs["ContentType"], kwargs["Metadata"])
            if self.lose_put_response:
                self.lose_put_response = False
                raise EndpointConnectionError(endpoint_url="http://minio.test")
        return {}

    def head_object(self, **kwargs: Any) -> dict[str, Any]:
        if self.head_service_errors:
            status, code = self.head_service_errors.pop(0)
            raise ClientError(
                {
                    "Error": {"Code": code},
                    "ResponseMetadata": {"HTTPStatusCode": status},
                },
                "HeadObject",
            )
        if self.hidden_head_requests > 0:
            self.hidden_head_requests -= 1
            raise ClientError(
                {
                    "Error": {"Code": "NoSuchKey"},
                    "ResponseMetadata": {"HTTPStatusCode": 404},
                },
                "HeadObject",
            )
        try:
            content, media_type, metadata = self.objects[kwargs["Key"]]
        except KeyError as error:
            raise ClientError(
                {
                    "Error": {"Code": "NoSuchKey"},
                    "ResponseMetadata": {"HTTPStatusCode": 404},
                },
                "HeadObject",
            ) from error
        return {
            "ContentLength": len(content),
            "ContentType": media_type,
            "Metadata": metadata,
        }

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        content, _, _ = self.objects[kwargs["Key"]]
        return {"Body": io.BytesIO(content)}

    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        prefix = kwargs["Prefix"]
        return {
            "Contents": [{"Key": key} for key in sorted(self.objects) if key.startswith(prefix)],
            "IsTruncated": False,
        }


class BlockingPutClient(MemoryS3Client):
    def __init__(self, stage: Literal["before_read", "after_commit"]) -> None:
        super().__init__()
        self.stage = stage
        self.started = threading.Event()
        self.committed = threading.Event()
        self.allow_finish = threading.Event()
        self.put_thread_is_daemon: bool | None = None

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        self.put_thread_is_daemon = threading.current_thread().daemon
        self.started.set()
        if self.stage == "before_read":
            if not self.allow_finish.wait(timeout=2):
                raise TimeoutError("test did not release PUT before body read")
            result = super().put_object(**kwargs)
            self.committed.set()
            return result
        result = super().put_object(**kwargs)
        self.committed.set()
        if not self.allow_finish.wait(timeout=2):
            raise TimeoutError("test did not release PUT response")
        return result


class AmbiguousPutClient(MemoryS3Client):
    def __init__(self, status: int, code: str, *, commit: bool) -> None:
        super().__init__()
        self.status = status
        self.code = code
        self.commit = commit

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        if self.commit:
            super().put_object(**kwargs)
        else:
            kwargs["Body"].read()
        raise ClientError(
            {
                "Error": {"Code": self.code},
                "ResponseMetadata": {"HTTPStatusCode": self.status},
            },
            "PutObject",
        )


class BlockingReadBody:
    def __init__(self) -> None:
        self.started = threading.Event()
        self.allow_finish = threading.Event()
        self.closed = threading.Event()
        self.read_thread_is_daemon: bool | None = None

    def read(self, size: int) -> bytes:
        del size
        self.read_thread_is_daemon = threading.current_thread().daemon
        self.started.set()
        self.allow_finish.wait(timeout=2)
        return b""

    def close(self) -> None:
        self.closed.set()


class BlockingDownloadClient(MemoryS3Client):
    def __init__(self) -> None:
        super().__init__()
        self.body = BlockingReadBody()

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        return {"Body": self.body}


class DelayedFailureStore:
    def __init__(self, error: RuntimeError) -> None:
        self.error = error
        self.started = asyncio.Event()
        self.finish = asyncio.Event()

    async def put_file_if_absent(self, **kwargs: Any) -> bool:
        del kwargs
        self.started.set()
        await self.finish.wait()
        raise self.error


def session(
    client: MemoryS3Client,
    *,
    max_bytes: int = 1024,
    persistence_drain_timeout: float = 1.0,
) -> ArtifactSession:
    return ArtifactSession(
        ImmutableS3Store(client, "artifacts", require_versioning=False),
        job_id=uuid4(),
        attempt_no=1,
        max_bytes=max_bytes,
        spool_max_memory_bytes=16,
        allowed_media_types=frozenset({"application/json", "text/plain"}),
        persistence_drain_timeout=persistence_drain_timeout,
    )


async def chunks(*values: bytes) -> AsyncIterator[bytes]:
    for value in values:
        await asyncio.sleep(0)
        yield value


@pytest.mark.asyncio
async def test_runtime_context_factory_recovers_attempt_artifacts() -> None:
    client = MemoryS3Client()
    store = ImmutableS3Store(client, "artifacts", require_versioning=False)
    job_id = uuid4()
    first = ArtifactSession(
        store,
        job_id=job_id,
        attempt_no=1,
        max_bytes=1024,
        spool_max_memory_bytes=16,
        allowed_media_types=frozenset({"application/json"}),
    )
    expected = await first.save(
        b'{"source":"fixture"}',
        "application/json",
        local_ref="artifact:raw:fixture-response",
        kind="raw_response",
        source_uri="https://fixture.invalid/responses/success",
        retrieved_at=NOW,
    )

    context = await ArtifactContextFactory(
        store,
        WorkerSettings(
            artifact_max_bytes=4096,
            artifact_spool_max_memory_bytes=16,
            artifact_allowed_media_types="application/json",
            source_result_inline_max_bytes=4096,
            source_result_manifest_max_bytes=4096,
        ),
        logging.getLogger(__name__),
    ).create(
        job_id=job_id,
        attempt_no=1,
        correlation_id=uuid4(),
        deadline=NOW + timedelta(minutes=1),
    )

    assert context.saved_artifacts == (expected,)
    assert b"".join([chunk async for chunk in context.download_artifact(expected)]) == (
        b'{"source":"fixture"}'
    )


@pytest.mark.asyncio
async def test_manifest_recovers_after_put_and_reconciles_without_overwrite() -> None:
    client = MemoryS3Client()
    store = ImmutableS3Store(client, "artifacts", require_versioning=False)
    settings = WorkerSettings(
        artifact_max_bytes=4096,
        artifact_spool_max_memory_bytes=16,
        artifact_allowed_media_types="application/json",
        source_result_inline_max_bytes=4096,
        source_result_manifest_max_bytes=4096,
    )
    factory = ArtifactContextFactory(store, settings, logging.getLogger(__name__))
    job_id = uuid4()
    first = await factory.create(
        job_id=job_id,
        attempt_no=1,
        correlation_id=uuid4(),
        deadline=NOW + timedelta(minutes=1),
    )
    await first.save_artifact(
        b'{"source":"fixture"}',
        "application/json",
        local_ref="artifact:raw:1",
        kind="raw_response",
        source_uri="https://example.test/fixture/1",
        retrieved_at=NOW,
    )
    manifest_bytes = b'{"schema_version":1,"records":[],"claims":[]}'
    manifest = await first.save_manifest(
        chunks(manifest_bytes),
        local_ref="artifact:manifest:source-result:v1",
        source_uri="https://example.test/fixture/1",
        retrieved_at=NOW,
        max_bytes=4096,
    )

    restarted = await factory.create(
        job_id=job_id,
        attempt_no=1,
        correlation_id=uuid4(),
        deadline=NOW + timedelta(minutes=1),
    )
    assert manifest in restarted.saved_artifacts
    reconciled = await restarted.save_manifest(
        chunks(manifest_bytes),
        local_ref="artifact:manifest:source-result:v1",
        source_uri="https://example.test/fixture/1",
        retrieved_at=NOW,
        max_bytes=4096,
    )
    assert reconciled == manifest

    with pytest.raises(ObjectConflictError):
        await restarted.save_manifest(
            chunks(manifest_bytes + b" "),
            local_ref="artifact:manifest:source-result:v1",
            source_uri="https://example.test/fixture/1",
            retrieved_at=NOW,
            max_bytes=4096,
        )
    assert client.objects[manifest.object_key][0] == manifest_bytes


@pytest.mark.asyncio
async def test_concurrent_manifest_finalization_reconciles_identical_bytes() -> None:
    client = MemoryS3Client()
    store = ImmutableS3Store(client, "artifacts", require_versioning=False)
    settings = WorkerSettings(
        artifact_max_bytes=4096,
        artifact_spool_max_memory_bytes=16,
        artifact_allowed_media_types="application/json",
        source_result_inline_max_bytes=4096,
        source_result_manifest_max_bytes=4096,
    )
    factory = ArtifactContextFactory(store, settings, logging.getLogger(__name__))
    job_id = uuid4()
    contexts = await asyncio.gather(
        *(
            factory.create(
                job_id=job_id,
                attempt_no=1,
                correlation_id=uuid4(),
                deadline=NOW + timedelta(minutes=1),
            )
            for _ in range(2)
        )
    )
    content = b'{"schema_version":1,"records":[],"claims":[]}'

    references = await asyncio.gather(
        *(
            context.save_manifest(
                chunks(content),
                local_ref="artifact:manifest:source-result:v1",
                source_uri="https://example.test/fixture/1",
                retrieved_at=NOW,
                max_bytes=4096,
            )
            for context in contexts
        )
    )

    assert references[0] == references[1]
    assert len(client.objects) == 1


@pytest.mark.asyncio
async def test_worker_restart_after_manifest_put_rebuilds_same_terminal_result() -> None:
    class CrashBeforeReplayStore(VolatileTerminalReplayStore):
        def __init__(self) -> None:
            super().__init__()
            self.attempted_message: PreparedTerminalMessage | None = None

        async def put_if_absent(
            self,
            command_id: UUID,
            command_fingerprint: str,
            message: PreparedTerminalMessage,
        ) -> StoredTerminalMessage:
            del command_id, command_fingerprint
            self.attempted_message = message
            raise RuntimeError("simulated crash before replay commit")

    client = MemoryS3Client()
    store = ImmutableS3Store(client, "artifacts", require_versioning=False)
    settings = WorkerSettings(
        artifact_max_bytes=1024 * 1024,
        artifact_spool_max_memory_bytes=1024,
        source_result_manifest_max_bytes=1024 * 1024,
    )
    registry = ContractSchemaRegistry(Path(__file__).resolve().parents[2] / "contracts" / "schemas")
    command_id = uuid4()
    command = SourceCollectRequested(
        message_id=command_id,
        type="source.collect.requested.v1",
        schema_version=1,
        occurred_at=NOW,
        correlation_id=uuid4(),
        causation_id=None,
        producer="graphx-test",
        root_command_id=command_id,
        payload=SourceCollectRequestedPayload(
            job_id=uuid4(),
            attempt_no=1,
            investigation_id=uuid4(),
            source="fixture",
            operation="search_subject",
            purpose="subject_discovery",
            query=SourceQuery(
                type="person",
                name="Crash Recovery",
                identifiers={"fixture_case": "success"},
            ),
            limits=CollectLimits(max_records=10, max_bytes=1024, timeout_seconds=30),
            deadline=NOW + timedelta(minutes=1),
        ),
    )
    raw = command.model_dump_json().encode()

    def runtime(replay: VolatileTerminalReplayStore) -> SourceCommandProcessor:
        return SourceCommandProcessor(
            registry,
            TerminalMessageFactory(registry, clock=lambda: NOW + timedelta(seconds=1)),
            replay,
            ArtifactContextFactory(store, settings, logging.getLogger(__name__)),
            (AdapterRegistration(adapter=FixtureAdapter()),),
            job_timeout=timedelta(minutes=1),
            result_finalizer=ManifestResultFinalizer(
                registry,
                inline_max_bytes=1,
                manifest_max_bytes=settings.source_result_manifest_max_bytes,
            ),
            clock=lambda: NOW,
        )

    crashing_replay = CrashBeforeReplayStore()
    with pytest.raises(RuntimeError, match="simulated crash"):
        await runtime(crashing_replay).prepare(raw)
    assert len(client.objects) == 2

    prepared = await runtime(VolatileTerminalReplayStore()).prepare(raw)
    message = parse_source_message(decode_contract_json(prepared.body))

    assert isinstance(message, SourceCollectCompleted)
    assert crashing_replay.attempted_message is not None
    assert prepared.body == crashing_replay.attempted_message.body
    assert message.payload.manifest is not None
    assert len(client.objects) == 2
    assert {artifact.kind for artifact in message.payload.artifacts} == {"raw_response"}


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["before_read", "after_commit"])
async def test_cancelled_save_finishes_put_and_registers_artifact_before_propagating(
    stage: Literal["before_read", "after_commit"],
) -> None:
    client = BlockingPutClient(stage)
    artifact_session = session(client)
    saving = asyncio.create_task(
        artifact_session.save(
            b"evidence",
            "text/plain",
            local_ref="artifact:raw:1",
            kind="raw_response",
            source_uri="https://example.test/source/1",
            retrieved_at=NOW,
        )
    )
    assert await asyncio.to_thread(client.started.wait, 1)
    if stage == "after_commit":
        assert await asyncio.to_thread(client.committed.wait, 1)

    saving.cancel()
    await asyncio.sleep(0.01)
    assert not saving.done()
    client.allow_finish.set()
    with pytest.raises(asyncio.CancelledError):
        await saving

    assert len(artifact_session.saved_artifacts) == 1
    assert next(iter(client.objects.values()))[0] == b"evidence"


@pytest.mark.asyncio
async def test_timeout_observes_registered_artifact_after_committed_put() -> None:
    client = BlockingPutClient("after_commit")
    artifact_session = session(client)

    async def release_response() -> None:
        assert await asyncio.to_thread(client.committed.wait, 1)
        await asyncio.sleep(0.05)
        client.allow_finish.set()

    release = asyncio.create_task(release_response())
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.01):
            await artifact_session.save(
                b"evidence",
                "text/plain",
                local_ref="artifact:raw:1",
                kind="raw_response",
                source_uri="https://example.test/source/1",
                retrieved_at=NOW,
            )
    await release
    assert len(artifact_session.saved_artifacts) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "code"),
    [(500, "InternalError"), (503, "SlowDown"), (429, "SlowDown")],
)
async def test_ambiguous_service_error_without_visible_object_is_uncertain(
    status: int,
    code: str,
) -> None:
    client = AmbiguousPutClient(status, code, commit=False)
    artifact_session = ArtifactSession(
        ImmutableS3Store(
            client,
            "artifacts",
            require_versioning=False,
            reconciliation_timeout=0.01,
        ),
        job_id=uuid4(),
        attempt_no=1,
        max_bytes=1024,
        spool_max_memory_bytes=16,
        allowed_media_types=frozenset({"text/plain"}),
    )

    with pytest.raises(ObjectReconciliationError, match="timeout"):
        await artifact_session.save(
            b"evidence",
            "text/plain",
            local_ref="artifact:raw:1",
            kind="raw_response",
            source_uri="https://example.test/source/1",
            retrieved_at=NOW,
        )


@pytest.mark.asyncio
async def test_ambiguous_service_error_reconciles_committed_object() -> None:
    artifact_session = session(AmbiguousPutClient(503, "SlowDown", commit=True))

    reference = await artifact_session.save(
        b"evidence",
        "text/plain",
        local_ref="artifact:raw:1",
        kind="raw_response",
        source_uri="https://example.test/source/1",
        retrieved_at=NOW,
    )

    assert artifact_session.saved_artifacts == (reference,)


@pytest.mark.asyncio
async def test_definitive_store_rejection_is_infrastructure_error() -> None:
    artifact_session = session(AmbiguousPutClient(403, "AccessDenied", commit=False))

    with pytest.raises(ObjectStoreOperationError, match="rejected"):
        await artifact_session.save(
            b"evidence",
            "text/plain",
            local_ref="artifact:raw:1",
            kind="raw_response",
            source_uri="https://example.test/source/1",
            retrieved_at=NOW,
        )


@pytest.mark.asyncio
async def test_hung_put_has_bounded_cancellation_and_retains_spool_ownership() -> None:
    client = BlockingPutClient("before_read")
    artifact_session = session(client, persistence_drain_timeout=0.02)
    saving = asyncio.create_task(
        artifact_session.save(
            b"evidence",
            "text/plain",
            local_ref="artifact:raw:1",
            kind="raw_response",
            source_uri="https://example.test/source/1",
            retrieved_at=NOW,
        )
    )
    assert await asyncio.to_thread(client.started.wait, 1)

    saving.cancel()
    with pytest.raises(ArtifactPersistenceDrainError, match="drain timeout"):
        await asyncio.wait_for(saving, timeout=0.2)

    assert client.put_thread_is_daemon is True
    assert len(artifact_session._persistence_tasks) == 1
    client.allow_finish.set()
    async with asyncio.timeout(1):
        while artifact_session._persistence_tasks:
            await asyncio.sleep(0)
    assert len(artifact_session.saved_artifacts) == 1
    assert next(iter(client.objects.values()))[0] == b"evidence"


@pytest.mark.asyncio
async def test_repeated_cancellation_aborts_persistence_drain() -> None:
    client = BlockingPutClient("before_read")
    artifact_session = session(client, persistence_drain_timeout=1.0)
    saving = asyncio.create_task(
        artifact_session.save(
            b"evidence",
            "text/plain",
            local_ref="artifact:raw:1",
            kind="raw_response",
            source_uri="https://example.test/source/1",
            retrieved_at=NOW,
        )
    )
    assert await asyncio.to_thread(client.started.wait, 1)

    saving.cancel()
    await asyncio.sleep(0)
    saving.cancel()
    with pytest.raises(ArtifactPersistenceDrainError, match="interrupted"):
        await asyncio.wait_for(saving, timeout=0.2)

    client.allow_finish.set()
    async with asyncio.timeout(1):
        while artifact_session._persistence_tasks:
            await asyncio.sleep(0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "persistence_error",
    [
        ObjectStoreOperationError("access denied"),
        ObjectStoreCapacityError("capacity exhausted"),
    ],
)
async def test_persistence_infrastructure_error_takes_priority_over_cancellation(
    persistence_error: RuntimeError,
) -> None:
    store = DelayedFailureStore(persistence_error)
    artifact_session = ArtifactSession(
        cast(Any, store),
        job_id=uuid4(),
        attempt_no=1,
        max_bytes=1024,
        spool_max_memory_bytes=16,
        allowed_media_types=frozenset({"text/plain"}),
    )
    saving = asyncio.create_task(
        artifact_session.save(
            b"evidence",
            "text/plain",
            local_ref="artifact:raw:1",
            kind="raw_response",
            source_uri="https://example.test/source/1",
            retrieved_at=NOW,
        )
    )
    await store.started.wait()

    saving.cancel()
    store.finish.set()

    with pytest.raises(type(persistence_error), match=str(persistence_error)):
        await saving


@pytest.mark.asyncio
async def test_artifact_session_isolates_authoritative_reference_from_callers() -> None:
    artifact_session = session(MemoryS3Client())
    reference = await artifact_session.save(
        b"evidence",
        "text/plain",
        local_ref="artifact:raw:1",
        kind="raw_response",
        source_uri="https://example.test/source/1",
        retrieved_at=NOW,
        http_metadata={"etag": "source-v1"},
    )
    expected_sha256 = sha256(b"evidence").hexdigest()

    reference.sha256 = "0" * 64
    reference.http_metadata["etag"] = "mutated-return"
    exposed = artifact_session.saved_artifacts[0]
    assert exposed.sha256 == expected_sha256
    assert exposed.http_metadata == {"etag": "source-v1"}

    exposed.sha256 = "1" * 64
    exposed.http_metadata["etag"] = "mutated-property"
    assert artifact_session.saved_artifacts[0].sha256 == expected_sha256
    assert artifact_session.saved_artifacts[0].http_metadata == {"etag": "source-v1"}


@pytest.mark.asyncio
async def test_artifact_session_rejects_postgres_incompatible_metadata_before_put() -> None:
    client = MemoryS3Client()
    artifact_session = session(client)

    with pytest.raises(ArtifactPolicyError, match="local_ref"):
        await artifact_session.save(
            b"evidence",
            "text/plain",
            local_ref=" ",
            kind="raw_response",
            source_uri="https://example.test/source/1",
            retrieved_at=NOW,
        )
    with pytest.raises(ArtifactPolicyError, match="HTTP metadata"):
        await artifact_session.save(
            b"evidence",
            "text/plain",
            local_ref="artifact:raw:1",
            kind="raw_response",
            source_uri="https://example.test/source/1",
            retrieved_at=NOW,
            http_metadata={"etag": "bad\x00value"},
        )

    assert client.objects == {}


@pytest.mark.asyncio
async def test_artifact_reference_comparison_uses_exact_wire_datetime() -> None:
    client = MemoryS3Client()
    artifact_session = session(client)
    retrieved_at = datetime(2024, 11, 3, 1, 30, tzinfo=ZoneInfo("America/New_York"), fold=1)
    first = await artifact_session.save(
        b"evidence",
        "text/plain",
        local_ref="artifact:raw:dst",
        kind="raw_response",
        source_uri="https://example.test/source/dst",
        retrieved_at=retrieved_at,
    )
    repeated = await artifact_session.save(
        b"evidence",
        "text/plain",
        local_ref="artifact:raw:dst",
        kind="raw_response",
        source_uri="https://example.test/source/dst",
        retrieved_at=retrieved_at,
    )
    assert repeated.model_dump(mode="json") == first.model_dump(mode="json")

    recovered = ArtifactSession(
        artifact_session._store,
        job_id=artifact_session._job_id,
        attempt_no=1,
        max_bytes=1024,
        spool_max_memory_bytes=16,
        allowed_media_types=frozenset({"text/plain"}),
    )
    await recovered.recover()
    assert b"".join([chunk async for chunk in recovered.download(first)]) == b"evidence"


@pytest.mark.asyncio
async def test_download_rejects_mutated_reference_without_serialization_warning() -> None:
    artifact_session = session(MemoryS3Client())
    reference = await artifact_session.save(
        b"evidence",
        "text/plain",
        local_ref="artifact:raw:1",
        kind="raw_response",
        source_uri="https://example.test/source/1",
        retrieved_at=NOW,
        http_metadata={"etag": "source-v1"},
    )
    mutations = (
        lambda value: setattr(value, "media_type", cast(Any, {"private": "document text"})),
        lambda value: setattr(value, "byte_size", True),
        lambda value: value.http_metadata.__setitem__(
            "etag", cast(Any, {"private": "document text"})
        ),
    )

    for mutate in mutations:
        candidate = reference.model_copy(deep=True)
        mutate(candidate)
        with (
            warnings.catch_warnings(record=True) as captured,
            pytest.raises(ArtifactPolicyError, match="not registered"),
        ):
            warnings.simplefilter("always")
            b"".join([chunk async for chunk in artifact_session.download(candidate)])
        assert captured == []


@pytest.mark.asyncio
async def test_streaming_save_download_and_recovery_are_deterministic() -> None:
    client = MemoryS3Client()
    artifact_session = session(client)
    reference = await artifact_session.save(
        chunks(b'{"name":', b'"Example"}'),
        "application/json",
        local_ref="artifact:raw:1",
        kind="raw_response",
        source_uri="https://example.test/source/1",
        retrieved_at=NOW,
        http_metadata={"ETag": '"source-etag"', "Authorization": "must-not-survive"},
        original_filename="../../response.json",
    )

    assert reference.object_key.startswith(f"artifacts/v1/{artifact_session._job_id}/1/")
    assert reference.original_filename == "response.json"
    assert reference.http_metadata == {"etag": '"source-etag"'}
    assert b"".join([chunk async for chunk in artifact_session.download(reference)]) == (
        b'{"name":"Example"}'
    )

    restarted = ArtifactSession(
        artifact_session._store,
        job_id=artifact_session._job_id,
        attempt_no=1,
        max_bytes=1024,
        spool_max_memory_bytes=16,
        allowed_media_types=frozenset({"application/json"}),
    )
    await restarted.recover()
    assert restarted.saved_artifacts == (reference,)
    retried = await restarted.save(
        b'{"name":"Example"}',
        "application/json",
        local_ref="artifact:raw:1",
        kind="raw_response",
        source_uri="https://example.test/source/1",
        retrieved_at=NOW + timedelta(seconds=1),
    )
    assert retried == reference


@pytest.mark.asyncio
async def test_concurrent_identical_uploads_are_idempotent() -> None:
    client = MemoryS3Client()
    first = session(client)
    second = ArtifactSession(
        first._store,
        job_id=first._job_id,
        attempt_no=1,
        max_bytes=1024,
        spool_max_memory_bytes=16,
        allowed_media_types=frozenset({"text/plain"}),
    )
    references = await asyncio.gather(
        first.save(
            b"same",
            "text/plain",
            local_ref="artifact:raw:1",
            kind="raw_response",
            source_uri="https://example.test/source/1",
            retrieved_at=NOW,
        ),
        second.save(
            b"same",
            "text/plain",
            local_ref="artifact:raw:1",
            kind="raw_response",
            source_uri="https://example.test/source/1",
            retrieved_at=NOW,
        ),
    )

    assert references[0] == references[1]
    assert len(client.objects) == 1


@pytest.mark.asyncio
async def test_concurrent_different_uploads_conflict_without_overwrite() -> None:
    client = MemoryS3Client()
    first = session(client)
    second = ArtifactSession(
        first._store,
        job_id=first._job_id,
        attempt_no=1,
        max_bytes=1024,
        spool_max_memory_bytes=16,
        allowed_media_types=frozenset({"text/plain"}),
    )

    outcomes = await asyncio.gather(
        first.save(
            b"first",
            "text/plain",
            local_ref="artifact:raw:1",
            kind="raw_response",
            source_uri="https://example.test/source/1",
            retrieved_at=NOW,
        ),
        second.save(
            b"second",
            "text/plain",
            local_ref="artifact:raw:1",
            kind="raw_response",
            source_uri="https://example.test/source/1",
            retrieved_at=NOW,
        ),
        return_exceptions=True,
    )

    assert sum(isinstance(value, ObjectConflictError) for value in outcomes) == 1
    assert len(client.objects) == 1


@pytest.mark.asyncio
async def test_lost_put_response_is_reconciled_without_a_second_write() -> None:
    client = MemoryS3Client()
    client.lose_put_response = True
    artifact_session = session(client)

    reference = await artifact_session.save(
        b"committed",
        "text/plain",
        local_ref="artifact:raw:1",
        kind="raw_response",
        source_uri="https://example.test/source/1",
        retrieved_at=NOW,
    )

    assert reference.sha256
    assert len(client.objects) == 1
    assert artifact_session.saved_artifacts == (reference,)


@pytest.mark.asyncio
async def test_policy_rejects_oversize_media_and_unknown_parent_before_registration() -> None:
    client = MemoryS3Client()
    artifact_session = session(client, max_bytes=4)
    with pytest.raises(ArtifactPolicyError, match="byte limit"):
        await artifact_session.save(
            b"12345",
            "text/plain",
            local_ref="artifact:raw:1",
            kind="raw_response",
            source_uri="https://example.test/source/1",
            retrieved_at=NOW,
        )
    with pytest.raises(ArtifactPolicyError, match="not allowed"):
        await artifact_session.save(
            b"1",
            "application/octet-stream",
            local_ref="artifact:raw:1",
            kind="raw_response",
            source_uri="https://example.test/source/1",
            retrieved_at=NOW,
        )
    with pytest.raises(ArtifactPolicyError, match="parent_local_ref"):
        await artifact_session.save(
            b"1",
            "text/plain",
            local_ref="artifact:raw:1",
            kind="raw_response",
            source_uri="https://example.test/source/1",
            retrieved_at=NOW,
            parent_local_ref="artifact:missing",
        )
    with pytest.raises(ArtifactPolicyError, match="transport contract"):
        await artifact_session.save(
            b"1",
            "text/plain",
            local_ref="artifact:raw:1",
            kind="raw_response",
            source_uri="not a uri",
            retrieved_at=NOW,
        )
    with pytest.raises(ArtifactPolicyError, match="local_ref"):
        await artifact_session.save(
            b"1",
            "text/plain",
            local_ref="🧪" * 200,
            kind="raw_response",
            source_uri="https://example.test/source/1",
            retrieved_at=NOW,
        )
    assert artifact_session.saved_artifacts == ()
    assert client.objects == {}


@pytest.mark.asyncio
async def test_large_stream_rolls_bounded_spool_to_disk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_spool = tempfile.SpooledTemporaryFile
    rolled: list[bool] = []

    class TrackingSpool:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.file = real_spool(*args, **kwargs)

        def __getattr__(self, name: str) -> Any:
            return getattr(self.file, name)

        def close(self) -> None:
            rolled.append(bool(cast(Any, self.file)._rolled))
            self.file.close()

    monkeypatch.setattr(
        "graphx_workers.runtime.artifacts.tempfile.SpooledTemporaryFile", TrackingSpool
    )
    artifact_session = session(MemoryS3Client())
    await artifact_session.save(
        chunks(b"a" * 12, b"b" * 12),
        "text/plain",
        local_ref="artifact:raw:1",
        kind="raw_response",
        source_uri="https://example.test/source/1",
        retrieved_at=NOW,
    )
    assert rolled == [True]


@pytest.mark.asyncio
async def test_same_bytes_reuse_first_writer_provenance() -> None:
    client = MemoryS3Client()
    first = session(client)
    second = ArtifactSession(
        first._store,
        job_id=first._job_id,
        attempt_no=1,
        max_bytes=1024,
        spool_max_memory_bytes=16,
        allowed_media_types=frozenset({"text/plain"}),
    )
    authoritative = await first.save(
        b"same",
        "text/plain",
        local_ref="artifact:raw:1",
        kind="raw_response",
        source_uri="https://example.test/source/1",
        retrieved_at=NOW,
    )

    duplicate = await second.save(
        b"same",
        "text/plain",
        local_ref="artifact:raw:1",
        kind="raw_response",
        source_uri="https://example.test/source/2",
        retrieved_at=NOW + timedelta(seconds=1),
    )
    assert duplicate == authoritative


@pytest.mark.asyncio
async def test_recovery_rejects_body_corruption_even_when_metadata_is_unchanged() -> None:
    client = MemoryS3Client()
    original = session(client)
    reference = await original.save(
        b"first",
        "text/plain",
        local_ref="artifact:raw:1",
        kind="raw_response",
        source_uri="https://example.test/source/1",
        retrieved_at=NOW,
    )
    _, media_type, metadata = client.objects[reference.object_key]
    client.objects[reference.object_key] = (b"other", media_type, metadata)
    restarted = ArtifactSession(
        original._store,
        job_id=original._job_id,
        attempt_no=1,
        max_bytes=1024,
        spool_max_memory_bytes=16,
        allowed_media_types=frozenset({"text/plain"}),
    )

    with pytest.raises(ObjectIntegrityError, match="bytes"):
        await restarted.recover()


@pytest.mark.asyncio
async def test_download_rejects_artifacts_outside_the_current_session_before_get() -> None:
    client = MemoryS3Client()
    owner = session(client)
    reference = await owner.save(
        b"private",
        "text/plain",
        local_ref="artifact:raw:1",
        kind="raw_response",
        source_uri="https://example.test/source/1",
        retrieved_at=NOW,
    )
    other = session(client)

    with pytest.raises(ArtifactPolicyError, match="not registered"):
        _ = b"".join([chunk async for chunk in other.download(reference)])


@pytest.mark.asyncio
async def test_download_does_not_release_corrupted_bytes_before_integrity_check() -> None:
    client = MemoryS3Client()
    artifact_session = session(client)
    reference = await artifact_session.save(
        b"first",
        "text/plain",
        local_ref="artifact:raw:1",
        kind="raw_response",
        source_uri="https://example.test/source/1",
        retrieved_at=NOW,
    )
    _, media_type, metadata = client.objects[reference.object_key]
    client.objects[reference.object_key] = (b"other", media_type, metadata)
    received: list[bytes] = []

    with pytest.raises(ObjectIntegrityError, match="does not match"):
        async for chunk in artifact_session.download(reference):
            received.append(chunk)
    assert received == []


@pytest.mark.asyncio
async def test_cancelled_download_leaves_body_with_owning_daemon_thread() -> None:
    client = BlockingDownloadClient()
    store = ImmutableS3Store(
        client,
        "artifacts",
        require_versioning=False,
        max_inflight_operations=1,
    )
    artifact_session = ArtifactSession(
        store,
        job_id=uuid4(),
        attempt_no=1,
        max_bytes=1024,
        spool_max_memory_bytes=16,
        allowed_media_types=frozenset({"text/plain"}),
    )
    reference = await artifact_session.save(
        b"evidence",
        "text/plain",
        local_ref="artifact:raw:1",
        kind="raw_response",
        source_uri="https://example.test/source/1",
        retrieved_at=NOW,
    )

    async def download_first_chunk() -> bytes:
        return await anext(artifact_session.download(reference))

    downloading = asyncio.create_task(download_first_chunk())
    assert await asyncio.to_thread(client.body.started.wait, 1)

    downloading.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(downloading, timeout=0.2)
    assert client.body.read_thread_is_daemon is True
    assert not client.body.closed.is_set()
    with pytest.raises(ObjectStoreCapacityError, match="capacity"):
        await store.head(reference.object_key)

    client.body.allow_finish.set()
    assert await asyncio.to_thread(client.body.closed.wait, 1)
    async with asyncio.timeout(1):
        while True:
            try:
                await store.head(reference.object_key)
                break
            except ObjectStoreCapacityError:
                await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_detached_store_operations_have_hard_capacity_limit() -> None:
    client = BlockingPutClient("before_read")
    store = ImmutableS3Store(
        client,
        "artifacts",
        require_versioning=False,
        max_inflight_operations=1,
    )
    first = ArtifactSession(
        store,
        job_id=uuid4(),
        attempt_no=1,
        max_bytes=1024,
        spool_max_memory_bytes=16,
        allowed_media_types=frozenset({"text/plain"}),
    )
    second = ArtifactSession(
        store,
        job_id=uuid4(),
        attempt_no=1,
        max_bytes=1024,
        spool_max_memory_bytes=16,
        allowed_media_types=frozenset({"text/plain"}),
    )
    saving = asyncio.create_task(
        first.save(
            b"first",
            "text/plain",
            local_ref="artifact:raw:1",
            kind="raw_response",
            source_uri="https://example.test/source/1",
            retrieved_at=NOW,
        )
    )
    assert await asyncio.to_thread(client.started.wait, 1)

    with pytest.raises(ObjectStoreCapacityError, match="capacity"):
        await second.save(
            b"second",
            "text/plain",
            local_ref="artifact:raw:1",
            kind="raw_response",
            source_uri="https://example.test/source/2",
            retrieved_at=NOW,
        )

    client.allow_finish.set()
    await saving


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [409, 412])
async def test_conditional_conflict_reconciliation_uses_bounded_backoff_until_visible(
    status: int,
) -> None:
    client = MemoryS3Client()
    artifact_session = session(client)
    authoritative = await artifact_session.save(
        b"same",
        "text/plain",
        local_ref="artifact:raw:1",
        kind="raw_response",
        source_uri="https://example.test/source/1",
        retrieved_at=NOW,
    )
    client.conditional_conflict_status = status
    client.hidden_head_requests = 4
    duplicate_session = ArtifactSession(
        ImmutableS3Store(client, "artifacts", require_versioning=False, reconciliation_timeout=1.0),
        job_id=artifact_session._job_id,
        attempt_no=1,
        max_bytes=1024,
        spool_max_memory_bytes=16,
        allowed_media_types=frozenset({"text/plain"}),
    )

    duplicate = await duplicate_session.save(
        b"same",
        "text/plain",
        local_ref="artifact:raw:1",
        kind="raw_response",
        source_uri="https://example.test/source/1",
        retrieved_at=NOW,
    )
    assert duplicate == authoritative


@pytest.mark.asyncio
async def test_reconciliation_retries_transient_head_service_error() -> None:
    client = MemoryS3Client()
    artifact_session = session(client)
    authoritative = await artifact_session.save(
        b"same",
        "text/plain",
        local_ref="artifact:raw:1",
        kind="raw_response",
        source_uri="https://example.test/source/1",
        retrieved_at=NOW,
    )
    client.head_service_errors = [(503, "ServiceUnavailable")]
    duplicate_session = ArtifactSession(
        artifact_session._store,
        job_id=artifact_session._job_id,
        attempt_no=1,
        max_bytes=1024,
        spool_max_memory_bytes=16,
        allowed_media_types=frozenset({"text/plain"}),
    )

    duplicate = await duplicate_session.save(
        b"same",
        "text/plain",
        local_ref="artifact:raw:1",
        kind="raw_response",
        source_uri="https://example.test/source/1",
        retrieved_at=NOW,
    )

    assert duplicate == authoritative


@pytest.mark.asyncio
async def test_reconciliation_preserves_definitive_head_error_as_fatal() -> None:
    client = MemoryS3Client()
    artifact_session = session(client)
    await artifact_session.save(
        b"same",
        "text/plain",
        local_ref="artifact:raw:1",
        kind="raw_response",
        source_uri="https://example.test/source/1",
        retrieved_at=NOW,
    )
    client.head_service_errors = [(403, "AccessDenied")]
    duplicate_session = ArtifactSession(
        artifact_session._store,
        job_id=artifact_session._job_id,
        attempt_no=1,
        max_bytes=1024,
        spool_max_memory_bytes=16,
        allowed_media_types=frozenset({"text/plain"}),
    )

    with pytest.raises(ObjectStoreOperationError) as caught:
        await duplicate_session.save(
            b"same",
            "text/plain",
            local_ref="artifact:raw:1",
            kind="raw_response",
            source_uri="https://example.test/source/1",
            retrieved_at=NOW,
        )

    assert isinstance(caught.value, RuntimeFatalError)


@pytest.mark.asyncio
async def test_409_reconciliation_timeout_is_typed() -> None:
    client = MemoryS3Client()
    artifact_session = session(client)
    await artifact_session.save(
        b"same",
        "text/plain",
        local_ref="artifact:raw:1",
        kind="raw_response",
        source_uri="https://example.test/source/1",
        retrieved_at=NOW,
    )
    client.conditional_conflict_status = 409
    client.hidden_head_requests = 100
    duplicate_session = ArtifactSession(
        ImmutableS3Store(
            client, "artifacts", require_versioning=False, reconciliation_timeout=0.01
        ),
        job_id=artifact_session._job_id,
        attempt_no=1,
        max_bytes=1024,
        spool_max_memory_bytes=16,
        allowed_media_types=frozenset({"text/plain"}),
    )

    with pytest.raises(ObjectReconciliationError, match="timeout"):
        await duplicate_session.save(
            b"same",
            "text/plain",
            local_ref="artifact:raw:1",
            kind="raw_response",
            source_uri="https://example.test/source/1",
            retrieved_at=NOW,
        )


@pytest.mark.asyncio
async def test_required_versioning_is_checked() -> None:
    enabled = ImmutableS3Store(
        MemoryS3Client(versioning="Enabled"), "artifacts", require_versioning=True
    )
    await enabled.check_ready()
    disabled = ImmutableS3Store(MemoryS3Client(), "artifacts", require_versioning=True)
    with pytest.raises(RuntimeError, match="versioning"):
        await disabled.check_ready()


def test_filename_and_http_metadata_are_safely_normalized() -> None:
    assert sanitize_original_filename("C:\\temp\\ report.json ") == "report.json"
    assert sanitize_http_metadata({"Cookie": "secret", "ETag": "abc"}) == {"etag": "abc"}
    with pytest.raises(ArtifactPolicyError):
        sanitize_original_filename("../..")
    with pytest.raises(ArtifactPolicyError):
        sanitize_http_metadata({"ETag": "bad\r\nheader"})
