"""Select inline or immutable-manifest transport for collected source data."""

from collections.abc import Mapping
from typing import Any

from graphx_workers.config import DEFAULT_SOURCE_RESULT_MAX_TOTAL_SCALAR_CHARS
from graphx_workers.contracts.messages import SourceCollectRequested
from graphx_workers.contracts.results import SourceResult
from graphx_workers.contracts.schema import ContractSchemaRegistry
from graphx_workers.runtime.processor import RuntimeAdapterContext
from graphx_workers.runtime.results import (
    build_manifest_source_result,
    compact_json_fits,
    finalize_inline_source_result,
    prepare_inline_source_result,
    prepare_manifest,
    validate_result_serialization_limits,
)
from graphx_workers.sdk import CollectResult

MANIFEST_LOCAL_REF = "artifact:manifest:source-result:v1"


class ManifestResultFinalizer:
    def __init__(
        self,
        schema_registry: ContractSchemaRegistry,
        *,
        inline_max_bytes: int,
        manifest_max_bytes: int,
        terminal_max_bytes: int = 1024 * 1024,
        max_scalar_chars: int = 256 * 1024,
        max_total_scalar_chars: int = DEFAULT_SOURCE_RESULT_MAX_TOTAL_SCALAR_CHARS,
        max_json_nodes: int = 20_000,
    ) -> None:
        if inline_max_bytes < 1:
            raise ValueError("inline result byte limit must be positive")
        self._schema_registry = schema_registry
        self._inline_max_bytes = inline_max_bytes
        if manifest_max_bytes < 1:
            raise ValueError("manifest byte limit must be positive")
        self._manifest_max_bytes = manifest_max_bytes
        if terminal_max_bytes < 1:
            raise ValueError("terminal result byte limit must be positive")
        self._terminal_max_bytes = terminal_max_bytes
        if max_scalar_chars < 1 or max_total_scalar_chars < 1 or max_json_nodes < 1:
            raise ValueError("result serialization limits must be positive")
        self._max_scalar_chars = max_scalar_chars
        self._max_total_scalar_chars = max_total_scalar_chars
        self._max_json_nodes = max_json_nodes

    async def finalize(
        self,
        command: SourceCollectRequested,
        collected: CollectResult,
        context: RuntimeAdapterContext,
        *,
        force_manifest: bool = False,
    ) -> SourceResult:
        validate_result_serialization_limits(
            self._serialization_preflight(collected, context),
            max_scalar_chars=self._max_scalar_chars,
            max_total_scalar_chars=self._max_total_scalar_chars,
            max_json_nodes=self._max_json_nodes,
        )
        reconciled = self._reconcile_source_artifacts(collected, context)
        validate_result_serialization_limits(
            reconciled,
            max_scalar_chars=self._max_scalar_chars,
            max_total_scalar_chars=self._max_total_scalar_chars,
            max_json_nodes=self._max_json_nodes,
        )
        inline_candidate = prepare_inline_source_result(
            command.payload.job_id,
            command.payload.attempt_no,
            reconciled,
        )
        recovered_manifests = [
            artifact for artifact in context.saved_artifacts if artifact.kind == "manifest"
        ]
        if any(artifact.local_ref != MANIFEST_LOCAL_REF for artifact in recovered_manifests):
            raise ValueError("artifact session contains an unexpected manifest reference")
        if reconciled.outcome == "not_found":
            if recovered_manifests:
                raise ValueError("not_found attempt cannot recover a result manifest")
            return finalize_inline_source_result(
                inline_candidate,
                max_bytes=self._terminal_max_bytes,
            )
        if (
            compact_json_fits(inline_candidate.model_dump(mode="json"), self._inline_max_bytes)
            and not recovered_manifests
            and not force_manifest
        ):
            return finalize_inline_source_result(
                inline_candidate,
                max_bytes=self._inline_max_bytes,
            )

        prepared = prepare_manifest(
            command.payload.job_id,
            command.payload.attempt_no,
            reconciled,
            max_bytes=self._manifest_max_bytes,
        )
        self._schema_registry.validate_manifest(prepared.schema_payload)
        anchor = min(reconciled.artifacts, key=lambda artifact: artifact.local_ref)
        reference = await context.save_manifest(
            prepared.iter_bytes(),
            local_ref=MANIFEST_LOCAL_REF,
            source_uri=str(anchor.source_uri),
            retrieved_at=anchor.retrieved_at,
            max_bytes=self._manifest_max_bytes,
        )
        return build_manifest_source_result(
            command.payload.job_id,
            command.payload.attempt_no,
            reconciled,
            prepared,
            reference,
            terminal_max_bytes=self._terminal_max_bytes,
        )

    @staticmethod
    def _serialization_preflight(
        collected: CollectResult,
        context: RuntimeAdapterContext,
    ) -> Mapping[str, Any]:
        return {
            "outcome": collected.outcome,
            "adapter_version": collected.adapter_version,
            "parser_version": collected.parser_version,
            "mapping_version": collected.mapping_version,
            "normalization_version": collected.normalization_version,
            "artifacts": context.saved_artifacts,
            "records": collected.records,
            "claims": collected.claims,
            "warnings": collected.warnings,
        }

    @staticmethod
    def _reconcile_source_artifacts(
        collected: CollectResult,
        context: RuntimeAdapterContext,
    ) -> CollectResult:
        source_artifacts = sorted(
            (artifact for artifact in context.saved_artifacts if artifact.kind != "manifest"),
            key=lambda artifact: artifact.local_ref,
        )
        saved_by_ref = {artifact.local_ref: artifact for artifact in source_artifacts}
        for artifact in collected.artifacts:
            if saved_by_ref.get(artifact.local_ref) != artifact:
                raise ValueError(
                    "adapter returned an artifact not registered by its runtime context"
                )
        if source_artifacts == collected.artifacts:
            return collected
        document = collected.model_dump(mode="python")
        document["artifacts"] = [
            artifact.model_dump(mode="python") for artifact in source_artifacts
        ]
        return CollectResult.model_validate(document)
