"""Exercise command-to-result flow through the built production worker image."""

import asyncio
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import aio_pika
import boto3  # type: ignore[import-untyped]
from botocore.config import Config  # type: ignore[import-untyped]

from graphx_workers.contracts.messages import (
    CollectLimits,
    SourceCollectCompleted,
    SourceCollectRequested,
    SourceCollectRequestedPayload,
    SourceQuery,
    parse_source_message,
)
from graphx_workers.contracts.results import (
    SourceResultManifest,
    validate_manifest_semantics,
)
from graphx_workers.contracts.schema import ContractSchemaRegistry, decode_contract_json
from graphx_workers.runtime.rabbitmq import COMMANDS_EXCHANGE, EVENTS_EXCHANGE

SCHEMA_ROOT = Path(__file__).resolve().parents[2] / "contracts" / "schemas"
SMOKE_BUCKET = "graphx-smoke-artifacts"


def object_bytes(object_key: str) -> bytes:
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_TEST_URL"],
        region_name="local",
        aws_access_key_id=os.environ["MINIO_TEST_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_TEST_SECRET_KEY"],
        config=Config(s3={"addressing_style": "path"}),
    )
    return client.get_object(Bucket=SMOKE_BUCKET, Key=object_key)["Body"].read()


async def main() -> None:
    url = os.environ["RABBITMQ_TEST_URL"]
    connection = await aio_pika.connect(url)
    try:
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        commands = await channel.declare_exchange(
            COMMANDS_EXCHANGE,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        events = await channel.declare_exchange(
            EVENTS_EXCHANGE,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        result_queue = await channel.declare_queue(exclusive=True, auto_delete=True)
        await result_queue.bind(events, routing_key="source.collect.completed.v1")

        now = datetime.now(UTC)

        def make_command(fixture_case: str) -> SourceCollectRequested:
            command_id = uuid4()
            return SourceCollectRequested(
                message_id=command_id,
                type="source.collect.requested.v1",
                schema_version=1,
                occurred_at=now,
                correlation_id=uuid4(),
                causation_id=None,
                producer="graphx-container-smoke",
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
                        name="Container Smoke",
                        identifiers={"fixture_case": fixture_case},
                    ),
                    limits=CollectLimits(
                        max_records=1,
                        max_bytes=1024,
                        timeout_seconds=10,
                    ),
                    deadline=now + timedelta(seconds=30),
                ),
            )

        async def publish_and_receive(
            requested: SourceCollectRequested,
        ) -> tuple[SourceCollectCompleted, bytes]:
            await commands.publish(
                aio_pika.Message(
                    requested.model_dump_json().encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key="source.collect.requested.v1",
                mandatory=True,
            )
            async with asyncio.timeout(10):
                while True:
                    result = await result_queue.get(fail=False, timeout=1)
                    if result is None:
                        await asyncio.sleep(0.05)
                        continue
                    body = result.body
                    message = parse_source_message(decode_contract_json(body))
                    await result.ack()
                    if not isinstance(message, SourceCollectCompleted):
                        raise RuntimeError("production worker returned an unexpected result")
                    return message, body

        command = make_command("success")
        first, first_body = await publish_and_receive(command)
        duplicate, duplicate_body = await publish_and_receive(command)
        if duplicate_body != first_body or duplicate != first:
            raise RuntimeError("duplicate command did not replay the exact terminal result")

        result = first.payload
        if result.manifest is None or result.records or result.claims:
            raise RuntimeError("production worker did not use exclusive manifest mode")
        if len(result.artifacts) != 1 or result.artifacts[0].kind != "raw_response":
            raise RuntimeError("production worker did not preserve the original artifact")

        raw = object_bytes(result.artifacts[0].object_key)
        if raw != (
            b'{"case":"success","person":{"external_id":"fixture-person-1","name":"Ada Fixture"}}'
        ):
            raise RuntimeError("stored original does not match the fixture response")
        manifest_bytes = object_bytes(result.manifest.object_key)
        registry = ContractSchemaRegistry(SCHEMA_ROOT)
        registry.validate_manifest_json(manifest_bytes)
        manifest = SourceResultManifest.model_validate_json(manifest_bytes)
        validate_manifest_semantics(result, manifest, manifest_bytes)

        not_found, _ = await publish_and_receive(make_command("not_found"))
        if (
            not_found.payload.outcome != "not_found"
            or not_found.payload.manifest is not None
            or not_found.payload.records
            or not_found.payload.claims
        ):
            raise RuntimeError("production not_found result did not remain inline")
        if len(not_found.payload.artifacts) != 1:
            raise RuntimeError("production not_found result lost its original artifact")
        not_found_raw = object_bytes(not_found.payload.artifacts[0].object_key)
        if not_found_raw != b'{"case":"not_found","person":null}':
            raise RuntimeError("stored not_found original does not match fixture response")
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
