import asyncio
import os
from datetime import UTC, datetime
from uuid import uuid4

import boto3  # type: ignore[import-untyped]
import pytest
from botocore.config import Config  # type: ignore[import-untyped]

from graphx_workers.runtime.artifacts import ArtifactSession
from graphx_workers.runtime.s3 import ImmutableS3Store, ObjectConflictError

TEST_URL = os.getenv("MINIO_TEST_URL")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(TEST_URL is None, reason="MINIO_TEST_URL is not configured"),
]


def client() -> object:
    return boto3.client(
        "s3",
        endpoint_url=TEST_URL,
        region_name="local",
        aws_access_key_id=os.environ.get("MINIO_TEST_ACCESS_KEY", "test_access"),
        aws_secret_access_key=os.environ.get("MINIO_TEST_SECRET_KEY", "test_secret_password"),
        config=Config(s3={"addressing_style": "path"}),
    )


@pytest.mark.asyncio
async def test_real_minio_immutable_streaming_and_versioning() -> None:
    s3 = client()
    bucket = f"graphx-test-{uuid4()}"
    s3.create_bucket(Bucket=bucket)  # type: ignore[attr-defined]
    try:
        store = ImmutableS3Store(s3, bucket, require_versioning=False)  # type: ignore[arg-type]
        await store.check_ready()
        job_id = uuid4()

        def artifact_session() -> ArtifactSession:
            return ArtifactSession(
                store,
                job_id=job_id,
                attempt_no=1,
                max_bytes=1024,
                spool_max_memory_bytes=4,
                allowed_media_types=frozenset({"application/json"}),
            )

        retrieved_at = datetime.now(UTC)

        async def stream() -> object:
            for chunk in (b'{"name":', b'"Example"}'):
                yield chunk

        first, duplicate = await asyncio.gather(
            artifact_session().save(
                stream(),  # type: ignore[arg-type]
                "application/json",
                local_ref="artifact:raw:1",
                kind="raw_response",
                source_uri="https://example.test/source/1",
                retrieved_at=retrieved_at,
            ),
            artifact_session().save(
                b'{"name":"Example"}',
                "application/json",
                local_ref="artifact:raw:1",
                kind="raw_response",
                source_uri="https://example.test/source/1",
                retrieved_at=retrieved_at,
            ),
        )
        assert first == duplicate

        recovered = artifact_session()
        await recovered.recover()
        assert len(recovered.saved_artifacts) == 1
        assert b"".join([chunk async for chunk in recovered.download(first)]) == (
            b'{"name":"Example"}'
        )

        conflict = await asyncio.gather(
            artifact_session().save(
                b"different",
                "application/json",
                local_ref="artifact:raw:1",
                kind="raw_response",
                source_uri="https://example.test/source/1",
                retrieved_at=first.retrieved_at,
            ),
            return_exceptions=True,
        )
        assert isinstance(conflict[0], ObjectConflictError)

        requiring_versioning = ImmutableS3Store(s3, bucket, require_versioning=True)  # type: ignore[arg-type]
        with pytest.raises(RuntimeError, match="versioning"):
            await requiring_versioning.check_ready()
        s3.put_bucket_versioning(  # type: ignore[attr-defined]
            Bucket=bucket,
            VersioningConfiguration={"Status": "Enabled"},
        )
        await requiring_versioning.check_ready()
    finally:
        listed = s3.list_object_versions(Bucket=bucket)  # type: ignore[attr-defined]
        objects = [
            {"Key": item["Key"], "VersionId": item["VersionId"]}
            for field in ("Versions", "DeleteMarkers")
            for item in listed.get(field, [])
        ]
        if objects:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})  # type: ignore[attr-defined]
        s3.delete_bucket(Bucket=bucket)  # type: ignore[attr-defined]
