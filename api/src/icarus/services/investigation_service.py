import uuid

from neo4j import AsyncSession, Record

from icarus.models.investigation import Annotation, InvestigationResponse, Tag
from icarus.services.neo4j_service import execute_query, execute_query_single


def _str(value: object) -> str:
    """Coerce Neo4j temporal or other types to string."""
    return str(value) if value is not None else ""


def _record_to_investigation(record: Record) -> InvestigationResponse:
    """Convert a Neo4j Record to InvestigationResponse."""
    return InvestigationResponse(
        id=record["id"],
        title=record["title"],
        description=record["description"],
        created_at=_str(record["created_at"]),
        updated_at=_str(record["updated_at"]),
        entity_ids=record["entity_ids"],
        share_token=record["share_token"],
    )


def _record_to_annotation(record: Record) -> Annotation:
    return Annotation(
        id=record["id"],
        entity_id=record["entity_id"],
        investigation_id=record["investigation_id"],
        text=record["text"],
        created_at=_str(record["created_at"]),
    )


def _record_to_tag(record: Record) -> Tag:
    return Tag(
        id=record["id"],
        investigation_id=record["investigation_id"],
        name=record["name"],
        color=record["color"],
    )


async def create_investigation(
    session: AsyncSession,
    title: str,
    description: str | None,
    user_id: str,
) -> InvestigationResponse:
    record = await execute_query_single(
        session,
        "investigation_create",
        {
            "id": str(uuid.uuid4()),
            "title": title,
            "description": description or "",
            "user_id": user_id,
        },
    )
    if record is None:
        msg = "Failed to create investigation"
        raise RuntimeError(msg)
    return _record_to_investigation(record)


async def get_investigation(
    session: AsyncSession,
    investigation_id: str,
) -> InvestigationResponse | None:
    record = await execute_query_single(
        session,
        "investigation_get",
        {"id": investigation_id},
    )
    if record is None:
        return None
    return _record_to_investigation(record)


async def list_investigations(
    session: AsyncSession,
    page: int,
    size: int,
    user_id: str,
) -> tuple[list[InvestigationResponse], int]:
    skip = (page - 1) * size
    records = await execute_query(
        session,
        "investigation_list",
        {"skip": skip, "limit": size, "user_id": user_id},
    )
    if not records:
        return [], 0
    total = int(records[0]["total"])
    investigations = [_record_to_investigation(r) for r in records]
    return investigations, total


async def update_investigation(
    session: AsyncSession,
    investigation_id: str,
    title: str | None,
    description: str | None,
    user_id: str,
) -> InvestigationResponse | None:
    record = await execute_query_single(
        session,
        "investigation_update",
        {"id": investigation_id, "title": title, "description": description, "user_id": user_id},
    )
    if record is None:
        return None
    return _record_to_investigation(record)


async def delete_investigation(
    session: AsyncSession,
    investigation_id: str,
    user_id: str,
) -> bool:
    record = await execute_query_single(
        session,
        "investigation_delete",
        {"id": investigation_id, "user_id": user_id},
    )
    if record is None:
        return False
    return int(record["deleted"]) > 0


async def add_entity_to_investigation(
    session: AsyncSession,
    investigation_id: str,
    entity_id: str,
    user_id: str,
) -> bool:
    record = await execute_query_single(
        session,
        "investigation_add_entity",
        {"investigation_id": investigation_id, "entity_id": entity_id, "user_id": user_id},
    )
    return record is not None


async def create_annotation(
    session: AsyncSession,
    investigation_id: str,
    entity_id: str,
    text: str,
    user_id: str,
) -> Annotation:
    record = await execute_query_single(
        session,
        "annotation_create",
        {
            "id": str(uuid.uuid4()),
            "investigation_id": investigation_id,
            "entity_id": entity_id,
            "text": text,
            "user_id": user_id,
        },
    )
    if record is None:
        msg = "Failed to create annotation"
        raise RuntimeError(msg)
    return _record_to_annotation(record)


async def list_annotations(
    session: AsyncSession,
    investigation_id: str,
) -> list[Annotation]:
    records = await execute_query(
        session,
        "annotation_list",
        {"investigation_id": investigation_id},
    )
    return [_record_to_annotation(r) for r in records]


async def create_tag(
    session: AsyncSession,
    investigation_id: str,
    name: str,
    color: str,
    user_id: str,
) -> Tag:
    record = await execute_query_single(
        session,
        "tag_create",
        {
            "id": str(uuid.uuid4()),
            "investigation_id": investigation_id,
            "name": name,
            "color": color,
            "user_id": user_id,
        },
    )
    if record is None:
        msg = "Failed to create tag"
        raise RuntimeError(msg)
    return _record_to_tag(record)


async def list_tags(
    session: AsyncSession,
    investigation_id: str,
) -> list[Tag]:
    records = await execute_query(
        session,
        "tag_list",
        {"investigation_id": investigation_id},
    )
    return [_record_to_tag(r) for r in records]


async def generate_share_token(
    session: AsyncSession,
    investigation_id: str,
    user_id: str,
) -> str | None:
    token = str(uuid.uuid4())
    record = await execute_query_single(
        session,
        "investigation_share",
        {"id": investigation_id, "share_token": token, "user_id": user_id},
    )
    if record is None:
        return None
    return str(record["share_token"])


async def get_by_share_token(
    session: AsyncSession,
    token: str,
) -> InvestigationResponse | None:
    record = await execute_query_single(
        session,
        "investigation_by_token",
        {"token": token},
    )
    if record is None:
        return None
    return _record_to_investigation(record)
