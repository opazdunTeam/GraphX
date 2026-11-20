"""Narrow S3-compatible client used by immutable artifact storage."""

import asyncio
import queue
import threading
import time
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import suppress
from hashlib import sha256 as new_sha256
from typing import Any, BinaryIO, Protocol, cast

import boto3  # type: ignore[import-untyped]
from botocore.config import Config  # type: ignore[import-untyped]
from botocore.exceptions import BotoCoreError, ClientError  # type: ignore[import-untyped]

from graphx_workers.config import WorkerSettings
from graphx_workers.runtime.errors import (
    RuntimeInfrastructureCapacityError,
    RuntimeInfrastructureFatalError,
    RuntimeInfrastructureUncertainError,
)


class S3Client(Protocol):
    def head_bucket(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def get_bucket_versioning(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def put_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def head_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def get_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def list_objects_v2(self, **kwargs: Any) -> Mapping[str, Any]: ...


class ObjectNotFoundError(FileNotFoundError):
    """Requested object does not exist."""


class ObjectConflictError(RuntimeError):
    """An immutable key already contains different bytes."""


class ObjectIntegrityError(RuntimeError):
    """Stored bytes do not match their immutable reference."""


class ObjectReconciliationError(RuntimeInfrastructureUncertainError):
    """A conditional write outcome did not become observable in bounded time."""


class ObjectStoreOperationError(RuntimeInfrastructureFatalError):
    """The object store rejected or could not complete an infrastructure operation."""


class ObjectStoreTransientError(RuntimeInfrastructureUncertainError):
    """A retryable object-store read or metadata operation failed."""


class ObjectStoreCapacityError(RuntimeInfrastructureCapacityError):
    """Too many blocking object-store operations remain active."""


def create_s3_client(
    *,
    endpoint: str,
    region: str,
    access_key: str,
    secret_key: str,
    use_path_style: bool,
    connect_timeout: float,
    read_timeout: float,
) -> S3Client:
    addressing_style = "path" if use_path_style else "virtual"
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            retries={"max_attempts": 3, "mode": "standard"},
            s3={"addressing_style": addressing_style},
        ),
    )
    return cast(S3Client, client)


class ImmutableS3Store:
    def __init__(
        self,
        client: S3Client,
        bucket: str,
        *,
        require_versioning: bool,
        reconciliation_timeout: float = 3.0,
        max_inflight_operations: int = 16,
    ) -> None:
        if reconciliation_timeout <= 0:
            raise ValueError("reconciliation_timeout must be positive")
        if max_inflight_operations < 1:
            raise ValueError("max_inflight_operations must be positive")
        self._client = client
        self._bucket = bucket
        self._require_versioning = require_versioning
        self._reconciliation_timeout = reconciliation_timeout
        self._operation_slots = threading.BoundedSemaphore(max_inflight_operations)

    async def check_ready(self) -> None:
        await self._run(self._client.head_bucket, Bucket=self._bucket)
        if self._require_versioning:
            response = await self._run(
                self._client.get_bucket_versioning,
                Bucket=self._bucket,
            )
            if response.get("Status") != "Enabled":
                raise RuntimeError("artifact bucket versioning is required but not enabled")

    async def put_file_if_absent(
        self,
        *,
        key: str,
        body: BinaryIO,
        size: int,
        sha256: str,
        media_type: str,
        metadata: Mapping[str, str],
    ) -> bool:
        try:
            # Ownership of body transfers here. The daemon operation closes it only
            # after boto has stopped reading, even if the awaiting task is cancelled.
            await self._run(
                self._put_file_and_close,
                key,
                body,
                size,
                media_type,
                dict(metadata),
            )
            return True
        except ObjectStoreCapacityError:
            body.close()
            raise
        except ClientError as error:
            status, code = _error_identity(error)
            if _is_ambiguous_write_error(status, code):
                await self._reconcile_existing(key, sha256=sha256, size=size)
                return False
            raise ObjectStoreOperationError(
                "artifact store rejected the conditional write"
            ) from error
        except BotoCoreError as error:
            # The connection may be lost after MinIO committed the object but before
            # the response reached the worker. Resolve that uncertain outcome by HEAD.
            try:
                await self._reconcile_existing(key, sha256=sha256, size=size)
            except ObjectReconciliationError as reconciliation_error:
                raise reconciliation_error from error
            return False

    def _put_file_and_close(
        self,
        key: str,
        body: BinaryIO,
        size: int,
        media_type: str,
        metadata: dict[str, str],
    ) -> None:
        try:
            self._put_file(key, body, size, media_type, metadata)
        finally:
            body.close()

    async def _run[**P, T](
        self,
        function: Callable[P, T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        return await _run_daemon(self._operation_slots, function, *args, **kwargs)

    async def _reconcile_existing(self, key: str, *, sha256: str, size: int) -> None:
        head = await self.head_reconciled(key)
        await self._require_same_object(key, head, sha256=sha256, size=size)

    async def head_reconciled(self, key: str) -> Mapping[str, Any]:
        deadline = time.monotonic() + self._reconciliation_timeout
        delay = 0.05
        while True:
            try:
                return await self.head(key)
            except (ObjectNotFoundError, ObjectStoreTransientError) as error:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ObjectReconciliationError(
                        "artifact object did not become observable before timeout"
                    ) from error
                await asyncio.sleep(min(delay, remaining))
                delay = min(delay * 2, 0.5)

    def _put_file(
        self,
        key: str,
        body: BinaryIO,
        size: int,
        media_type: str,
        metadata: dict[str, str],
    ) -> None:
        body.seek(0)
        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=body,
            ContentLength=size,
            ContentType=media_type,
            Metadata=metadata,
            IfNoneMatch="*",
        )

    async def head(self, key: str) -> Mapping[str, Any]:
        try:
            return await self._run(
                self._client.head_object,
                Bucket=self._bucket,
                Key=key,
            )
        except ClientError as error:
            status, code = _error_identity(error)
            if status == 404 or code in {"404", "NoSuchKey", "NotFound"}:
                raise ObjectNotFoundError(key) from error
            if _is_retryable_service_error(status, code):
                raise ObjectStoreTransientError(
                    "artifact metadata lookup is unavailable"
                ) from error
            raise ObjectStoreOperationError("artifact metadata lookup failed") from error
        except BotoCoreError as error:
            raise ObjectStoreTransientError("artifact metadata lookup is unavailable") from error

    async def list_keys(self, prefix: str) -> tuple[str, ...]:
        keys: list[str] = []
        continuation: str | None = None
        while True:
            arguments: dict[str, Any] = {"Bucket": self._bucket, "Prefix": prefix}
            if continuation is not None:
                arguments["ContinuationToken"] = continuation
            try:
                response = await self._run(self._client.list_objects_v2, **arguments)
            except (ClientError, BotoCoreError) as error:
                raise ObjectStoreOperationError("artifact listing failed") from error
            for item in response.get("Contents", ()):
                key = item.get("Key")
                if isinstance(key, str):
                    keys.append(key)
            if not response.get("IsTruncated"):
                break
            token = response.get("NextContinuationToken")
            if not isinstance(token, str) or not token:
                raise RuntimeError("truncated S3 listing did not provide a continuation token")
            continuation = token
        return tuple(keys)

    async def iter_bytes(self, key: str, *, chunk_size: int = 64 * 1024) -> AsyncIterator[bytes]:
        if not self._operation_slots.acquire(blocking=False):
            raise ObjectStoreCapacityError("object-store operation capacity is exhausted")
        items: queue.Queue[bytes | BaseException | object] = queue.Queue(maxsize=2)
        finished = object()
        stopped = threading.Event()
        available = asyncio.Event()
        loop = asyncio.get_running_loop()

        def publish(item: bytes | BaseException | object) -> bool:
            while not stopped.is_set():
                try:
                    items.put(item, timeout=0.05)
                except queue.Full:
                    continue
                with suppress(RuntimeError):
                    loop.call_soon_threadsafe(available.set)
                return True
            return False

        def produce() -> None:
            body: Any | None = None
            failure: BaseException | None = None
            try:
                response = self._client.get_object(Bucket=self._bucket, Key=key)
                body = response["Body"]
                while not stopped.is_set():
                    chunk = body.read(chunk_size)
                    if not chunk:
                        break
                    if not publish(bytes(chunk)):
                        break
            except BaseException as error:
                failure = error
            finally:
                try:
                    if body is not None:
                        body.close()
                except BaseException as error:
                    if failure is None:
                        failure = error
                finally:
                    self._operation_slots.release()
            if not stopped.is_set():
                publish(failure if failure is not None else finished)

        thread = threading.Thread(target=produce, name="graphx-s3-stream", daemon=True)
        try:
            thread.start()
        except BaseException as error:
            self._operation_slots.release()
            raise ObjectStoreCapacityError("could not start artifact download worker") from error
        try:
            while True:
                try:
                    item = items.get_nowait()
                except queue.Empty:
                    available.clear()
                    try:
                        item = items.get_nowait()
                    except queue.Empty:
                        await available.wait()
                        continue
                if item is finished:
                    return
                if isinstance(item, BaseException):
                    if isinstance(item, ClientError):
                        status, code = _error_identity(item)
                        if status == 404 or code in {"404", "NoSuchKey", "NotFound"}:
                            raise ObjectNotFoundError(key) from item
                    raise ObjectStoreOperationError("artifact download failed") from item
                yield cast(bytes, item)
        finally:
            stopped.set()

    async def verify_object(self, key: str, *, sha256: str, size: int) -> None:
        digest = new_sha256()
        actual_size = 0
        async for chunk in self.iter_bytes(key):
            actual_size += len(chunk)
            if actual_size > size:
                raise ObjectIntegrityError("stored artifact is larger than its reference")
            digest.update(chunk)
        if actual_size != size or digest.hexdigest() != sha256:
            raise ObjectIntegrityError("stored artifact bytes do not match their reference")

    async def _require_same_object(
        self,
        key: str,
        head: Mapping[str, Any],
        *,
        sha256: str,
        size: int,
    ) -> None:
        metadata = head.get("Metadata")
        stored_hash = metadata.get("gx-sha256") if isinstance(metadata, Mapping) else None
        if stored_hash != sha256 or head.get("ContentLength") != size:
            raise ObjectConflictError("immutable artifact key already contains different bytes")
        try:
            await self.verify_object(key, sha256=sha256, size=size)
        except ObjectIntegrityError as error:
            raise ObjectConflictError(
                "immutable artifact key already contains different bytes"
            ) from error


def build_artifact_store(settings: WorkerSettings) -> ImmutableS3Store:
    endpoint, bucket, access_key, secret_key = settings.require_object_store()
    client = create_s3_client(
        endpoint=endpoint,
        region=settings.s3_region,
        access_key=access_key,
        secret_key=secret_key,
        use_path_style=settings.s3_use_path_style,
        connect_timeout=settings.s3_connect_timeout.total_seconds(),
        read_timeout=settings.s3_read_timeout.total_seconds(),
    )
    return ImmutableS3Store(
        client,
        bucket,
        require_versioning=settings.s3_require_versioning,
        reconciliation_timeout=settings.s3_reconciliation_timeout.total_seconds(),
        max_inflight_operations=settings.s3_max_inflight_operations,
    )


def _error_identity(error: ClientError) -> tuple[int | None, str]:
    response = error.response
    metadata = response.get("ResponseMetadata", {})
    status = metadata.get("HTTPStatusCode")
    code = str(response.get("Error", {}).get("Code", ""))
    return (status if isinstance(status, int) else None, code)


def _is_ambiguous_write_error(status: int | None, code: str) -> bool:
    return (
        status in {409, 412}
        or code in {"409", "412", "ConditionalRequestConflict", "PreconditionFailed"}
        or _is_retryable_service_error(status, code)
    )


def _is_retryable_service_error(status: int | None, code: str) -> bool:
    return (
        status in {408, 429}
        or (status is not None and 500 <= status <= 599)
        or code
        in {
            "408",
            "429",
            "InternalError",
            "RequestTimeout",
            "RequestTimeoutException",
            "ServiceUnavailable",
            "SlowDown",
            "Throttling",
            "ThrottlingException",
        }
    )


async def _run_daemon[**P, T](
    slots: threading.BoundedSemaphore,
    function: Callable[P, T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    """Run blocking SDK work without making interpreter shutdown wait for it."""
    if not slots.acquire(blocking=False):
        raise ObjectStoreCapacityError("object-store operation capacity is exhausted")
    loop = asyncio.get_running_loop()
    future: asyncio.Future[T] = loop.create_future()

    def deliver(result: T | None, error: BaseException | None) -> None:
        if future.done():
            return
        if error is None:
            future.set_result(cast(T, result))
        else:
            future.set_exception(error)

    def invoke() -> None:
        result: T | None = None
        error: BaseException | None = None
        try:
            result = function(*args, **kwargs)
        except BaseException as caught:
            error = caught
        finally:
            slots.release()
        with suppress(RuntimeError):
            loop.call_soon_threadsafe(deliver, result, error)

    thread = threading.Thread(target=invoke, name="graphx-s3", daemon=True)
    try:
        thread.start()
    except BaseException as error:
        slots.release()
        raise ObjectStoreCapacityError("could not start object-store operation") from error
    return await future
