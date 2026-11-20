"""Lifecycle shared by source, document, and browser entrypoints."""

import argparse
import asyncio
import logging
import signal
from collections.abc import Sequence
from datetime import datetime
from uuid import UUID

from graphx_workers.config import WorkerKind, WorkerSettings
from graphx_workers.contracts.schema import ContractSchemaRegistry
from graphx_workers.runtime.logging import configure_logging
from graphx_workers.runtime.processor import (
    AdapterContextFactory,
    RuntimeAdapterContext,
    SourceCommandProcessor,
)
from graphx_workers.runtime.rabbitmq import SourceRabbitRuntime
from graphx_workers.runtime.replay import FileTerminalReplayStore
from graphx_workers.runtime.terminal import TerminalMessageFactory


class UnconfiguredContextFactory(AdapterContextFactory):
    async def create(
        self,
        *,
        job_id: UUID,
        attempt_no: int,
        correlation_id: UUID,
        deadline: datetime,
    ) -> RuntimeAdapterContext:
        del job_id, attempt_no, correlation_id, deadline
        raise RuntimeError("adapter context is not configured")


def build_parser(kind: WorkerKind) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=f"graphx-{kind.value}-worker")
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate configuration and exit without connecting to dependencies",
    )
    return parser


async def run_forever(settings: WorkerSettings, logger: logging.Logger) -> None:
    if settings.worker_kind is WorkerKind.SOURCE:
        await run_source_worker(settings, logger)
        return

    logger.info(
        "worker scaffold started",
        extra={"context": settings.safe_summary()},
    )
    try:
        while True:
            await asyncio.sleep(settings.worker_heartbeat_interval.total_seconds())
            logger.debug(
                "worker scaffold heartbeat",
                extra={"context": {"worker_kind": settings.worker_kind.value}},
            )
    except asyncio.CancelledError:
        logger.info(
            "worker scaffold stopped",
            extra={"context": {"worker_kind": settings.worker_kind.value}},
        )
        raise


async def run_source_worker(settings: WorkerSettings, logger: logging.Logger) -> None:
    registry = ContractSchemaRegistry(settings.resolve_contract_schema_root())
    replay_store = FileTerminalReplayStore(
        settings.terminal_replay_root,
        retention=settings.terminal_replay_retention,
        cleanup_interval=settings.terminal_replay_cleanup_interval,
        max_entries=settings.terminal_replay_max_entries,
        max_bytes=settings.terminal_replay_max_bytes,
        max_entry_bytes=settings.terminal_replay_max_entry_bytes,
    )
    replay_stats = await replay_store.cleanup()
    logger.info(
        "terminal replay journal ready",
        extra={
            "context": {
                "replay_entries": replay_stats.entries,
                "replay_bytes": replay_stats.bytes_used,
            }
        },
    )
    processor = SourceCommandProcessor(
        registry,
        TerminalMessageFactory(registry),
        replay_store,
        UnconfiguredContextFactory(),
        (),
        job_timeout=settings.worker_job_timeout,
        logger=logger,
    )
    runtime = SourceRabbitRuntime(
        url=settings.require_rabbitmq_url(),
        processor=processor,
        prefetch=settings.rabbitmq_prefetch_source,
        concurrency=settings.worker_concurrency,
        connect_timeout=settings.rabbitmq_connect_timeout,
        publish_timeout=settings.rabbitmq_publish_timeout,
        shutdown_timeout=settings.worker_shutdown_timeout,
        logger=logger,
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    registered_signals: list[signal.Signals] = []
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop.set)
            registered_signals.append(signum)
        except NotImplementedError, RuntimeError:
            pass
    try:
        await runtime.run(stop)
    finally:
        for signum in registered_signals:
            loop.remove_signal_handler(signum)


def run(kind: WorkerKind, argv: Sequence[str] | None = None) -> int:
    args = build_parser(kind).parse_args(argv)
    settings = WorkerSettings(worker_kind=kind)
    logger = configure_logging(settings.log_level, settings.log_format)

    if args.check:
        logger.info("configuration valid", extra={"context": settings.safe_summary()})
        return 0

    try:
        asyncio.run(run_forever(settings, logger))
    except KeyboardInterrupt:
        logger.info("worker interrupted", extra={"context": {"worker_kind": kind.value}})
    return 0
