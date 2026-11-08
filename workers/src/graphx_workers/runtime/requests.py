"""Runtime mapping from transport commands to the adapter SDK."""

from graphx_workers.contracts.messages import SourceCollectRequestedPayload
from graphx_workers.sdk.adapter import CollectRequest


def to_collect_request(payload: SourceCollectRequestedPayload) -> CollectRequest:
    return CollectRequest(
        operation=payload.operation,
        purpose=payload.purpose,
        query=payload.query,
        limits=payload.limits,
        deadline=payload.deadline,
    )
