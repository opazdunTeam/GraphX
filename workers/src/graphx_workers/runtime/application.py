"""Lifecycle shared by source, document, and browser entrypoints."""

import argparse
import asyncio
import logging
from collections.abc import Sequence

from graphx_workers.config import WorkerKind, WorkerSettings
from graphx_workers.runtime.logging import configure_logging


def build_parser(kind: WorkerKind) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=f"graphx-{kind.value}-worker")
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate configuration and exit without connecting to dependencies",
    )
    return parser


async def run_forever(settings: WorkerSettings, logger: logging.Logger) -> None:
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


def run(kind: WorkerKind, argv: Sequence[str] | None = None) -> int:
    args = build_parser(kind).parse_args(argv)
    settings = WorkerSettings(worker_kind=kind)
    logger = configure_logging(settings.log_level)

    if args.check:
        logger.info("configuration valid", extra={"context": settings.safe_summary()})
        return 0

    try:
        asyncio.run(run_forever(settings, logger))
    except KeyboardInterrupt:
        logger.info("worker interrupted", extra={"context": {"worker_kind": kind.value}})
    return 0
