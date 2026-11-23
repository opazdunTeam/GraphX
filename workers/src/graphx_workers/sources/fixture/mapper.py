"""Map parsed fixture data into portable source records and claims."""

from graphx_workers.sdk import ArtifactReference, Claim, SourceRecord
from graphx_workers.sources.fixture.metadata import PERSON_LOCAL_REF, SCHEMA_VERSION
from graphx_workers.sources.fixture.parser import FixturePerson


def map_person(
    person: FixturePerson,
    artifact: ArtifactReference,
) -> tuple[SourceRecord, Claim]:
    record = SourceRecord(
        local_ref=PERSON_LOCAL_REF,
        artifact_local_ref=artifact.local_ref,
        record_type="person",
        external_id=person.external_id,
        schema_version=SCHEMA_VERSION,
        raw_data={"external_id": person.external_id, "name": person.name},
    )
    claim = Claim(
        claim_kind="attribute",
        subject_local_ref=PERSON_LOCAL_REF,
        predicate="name",
        object_local_ref=None,
        raw_value=person.name,
        normalized_value=person.name.casefold(),
        qualifiers={},
        valid_from=None,
        valid_to=None,
        valid_from_precision="unknown",
        valid_to_precision="unknown",
        temporal_basis="unknown",
        snapshot_at=None,
        evidence_locator={"json_pointer": "/person/name"},
        evidence_excerpt=person.name,
    )
    return record, claim
