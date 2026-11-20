"""Exercise command-to-result flow through the built production worker image."""

import asyncio
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import aio_pika

from graphx_workers.contracts.errors import ErrorCode
from graphx_workers.contracts.messages import (
    CollectLimits,
    SourceCollectFailed,
    SourceCollectRequested,
    SourceCollectRequestedPayload,
    SourceQuery,
    parse_source_message,
)
from graphx_workers.contracts.schema import decode_contract_json
from graphx_workers.runtime.rabbitmq import COMMANDS_EXCHANGE, EVENTS_EXCHANGE


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
        await result_queue.bind(events, routing_key="source.collect.failed.v1")

        now = datetime.now(UTC)
        command_id = uuid4()
        command = SourceCollectRequested(
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
                source="container-smoke",
                operation="collect",
                purpose="subject_discovery",
                query=SourceQuery(type="person", name="Container Smoke"),
                limits=CollectLimits(
                    max_records=1,
                    max_bytes=1024,
                    timeout_seconds=10,
                ),
                deadline=now + timedelta(seconds=30),
            ),
        )
        await commands.publish(
            aio_pika.Message(
                command.model_dump_json().encode(),
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
                message = parse_source_message(decode_contract_json(result.body))
                await result.ack()
                if isinstance(message, SourceCollectFailed):
                    if message.payload.error.code is not ErrorCode.SOURCE_UNSUPPORTED_OPERATION:
                        raise RuntimeError("production worker returned an unexpected error")
                    return
        raise RuntimeError("production worker did not publish a terminal result")
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
