"""Integration tests for link_persons.cypher.

Runs against a real Neo4j testcontainer to verify SAME_AS relationships
are created with correct confidence, method, and uniqueness guards.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from neo4j import Driver

LINK_SCRIPT = (
    Path(__file__).parent.parent.parent.parent
    / "scripts"
    / "link_persons.cypher"
)


def _parse_phases() -> dict[int, str]:
    """Parse link_persons.cypher into phase number → Cypher blocks."""
    text = LINK_SCRIPT.read_text()
    blocks: dict[int, str] = {}
    current_phase: int | None = None
    lines: list[str] = []
    for line in text.splitlines():
        if line.startswith("// ── Phase "):
            if current_phase is not None:
                blocks[current_phase] = "\n".join(lines)
            phase_str = line.split("Phase ")[1].split(":")[0]
            current_phase = int(phase_str)
            lines = []
        else:
            lines.append(line)
    if current_phase is not None:
        blocks[current_phase] = "\n".join(lines)
    return blocks


PHASE_BLOCKS = _parse_phases()


def _strip_comments(cypher: str) -> str:
    """Remove // comment lines from a Cypher block."""
    return "\n".join(
        line for line in cypher.splitlines()
        if not line.strip().startswith("//")
    )


def _run_cypher(driver: Driver, cypher: str) -> None:
    """Run one or more semicolon-separated Cypher statements."""
    with driver.session() as session:
        for stmt in cypher.split(";"):
            stmt = _strip_comments(stmt).strip()
            if stmt:
                session.run(stmt).consume()


def _run_phases(driver: Driver, phases: list[int]) -> None:
    """Run specific phases from link_persons.cypher."""
    for phase in phases:
        _run_cypher(driver, PHASE_BLOCKS[phase])


def _clear_db(driver: Driver) -> None:
    _run_cypher(driver, "MATCH (n) DETACH DELETE n")


def _setup(driver: Driver, *statements: str) -> None:
    """Run setup Cypher statements, each in its own auto-commit tx."""
    for stmt in statements:
        _run_cypher(driver, stmt)


def _count_same_as(
    driver: Driver, method: str | None = None,
) -> int:
    """Count SAME_AS relationships, optionally filtered by method."""
    q = "MATCH ()-[r:SAME_AS]->() "
    if method:
        q += f"WHERE r.method = '{method}' "
    q += "RETURN count(r) AS cnt"
    with driver.session() as session:
        result = session.run(q)
        record = result.single()
        return record["cnt"] if record else 0


# ── Phase 4 tests ──────────────────────────────────────────────────


@pytest.mark.integration
def test_phase4_partial_cpf_name_match(neo4j_driver: Driver) -> None:
    """Phase 4 is disabled: partial CPF matching must not create SAME_AS."""
    _clear_db(neo4j_driver)
    _setup(
        neo4j_driver,
        "CREATE (:Person {cpf: '026.005.602-20', name: 'JOSE DIAS TOFFOLI'})",
    )
    _run_phases(neo4j_driver, [0])
    _setup(
        neo4j_driver,
        "CREATE (:Person {"
        "cpf: '005602', cpf_partial: '005602', "
        "name: 'JOSE DIAS TOFFOLI'})",
    )

    _run_phases(neo4j_driver, [4])

    assert _count_same_as(neo4j_driver) == 0


@pytest.mark.integration
def test_phase4_no_match_different_name(neo4j_driver: Driver) -> None:
    """Same cpf_partial/cpf_middle6 but different names -> no match."""
    _clear_db(neo4j_driver)
    _setup(
        neo4j_driver,
        "CREATE (:Person {cpf: '026.005.602-20', name: 'JOSE DIAS TOFFOLI'})",
    )
    _run_phases(neo4j_driver, [0])
    _setup(
        neo4j_driver,
        "CREATE (:Person {"
        "cpf: '005602', cpf_partial: '005602', "
        "name: 'MARIA DA SILVA'})",
    )

    _run_phases(neo4j_driver, [4])

    assert _count_same_as(neo4j_driver) == 0


@pytest.mark.integration
def test_phase4_no_duplicate_if_already_linked(
    neo4j_driver: Driver,
) -> None:
    """Running disabled Phase 4 twice must still create no SAME_AS."""
    _clear_db(neo4j_driver)
    _setup(
        neo4j_driver,
        "CREATE (:Person {cpf: '026.005.602-20', name: 'JOSE DIAS TOFFOLI'})",
    )
    _run_phases(neo4j_driver, [0])
    _setup(
        neo4j_driver,
        "CREATE (:Person {"
        "cpf: '005602', cpf_partial: '005602', "
        "name: 'JOSE DIAS TOFFOLI'})",
    )

    _run_phases(neo4j_driver, [4])
    _run_phases(neo4j_driver, [4])  # idempotent

    assert _count_same_as(neo4j_driver) == 0


# ── Phase 5 tests ──────────────────────────────────────────────────


@pytest.mark.integration
def test_phase5_unique_name_match(neo4j_driver: Driver) -> None:
    """Unique-name servidor (blank cpf_partial) matches unique person."""
    _clear_db(neo4j_driver)
    _setup(
        neo4j_driver,
        "CREATE (:Person {cpf: '026.005.602-20', name: 'JOSE DIAS TOFFOLI'})",
    )
    _run_phases(neo4j_driver, [0])
    _setup(
        neo4j_driver,
        "CREATE (:Person {name: 'JOSE DIAS TOFFOLI'})"
        "-[:RECEBEU_SALARIO]->(:PublicOffice {cpf: 'classified_1'})",
    )

    _run_phases(neo4j_driver, [5])

    with neo4j_driver.session() as s:
        result = s.run(
            "MATCH ()-[r:SAME_AS]->() "
            "RETURN r.confidence AS conf, r.method AS method"
        )
        records = list(result)
        assert len(records) == 1
        assert records[0]["conf"] == 0.85
        assert records[0]["method"] == "unique_name_match_servidor"


@pytest.mark.integration
def test_phase5_common_name_servidor_side_no_match(
    neo4j_driver: Driver,
) -> None:
    """Two servidores with same name and blank cpf_partial -> no match."""
    _clear_db(neo4j_driver)
    _setup(
        neo4j_driver,
        "CREATE (:Person {cpf: '111.222.333-44', name: 'JOSE DA SILVA'})",
    )
    _run_phases(neo4j_driver, [0])
    _setup(
        neo4j_driver,
        "CREATE (:Person {name: 'JOSE DA SILVA'})"
        "-[:RECEBEU_SALARIO]->(:PublicOffice {cpf: 'classified_a'})",
        "CREATE (:Person {name: 'JOSE DA SILVA'})"
        "-[:RECEBEU_SALARIO]->(:PublicOffice {cpf: 'classified_b'})",
    )

    _run_phases(neo4j_driver, [5])

    assert _count_same_as(neo4j_driver) == 0


@pytest.mark.integration
def test_phase5_common_name_person_side_no_match(
    neo4j_driver: Driver,
) -> None:
    """Unique servidor but two full-CPF persons share the name -> no match."""
    _clear_db(neo4j_driver)
    _setup(
        neo4j_driver,
        "CREATE (:Person {cpf: '111.222.333-44', name: 'MARIA OLIVEIRA'})",
        "CREATE (:Person {cpf: '555.666.777-88', name: 'MARIA OLIVEIRA'})",
    )
    _run_phases(neo4j_driver, [0])
    _setup(
        neo4j_driver,
        "CREATE (:Person {name: 'MARIA OLIVEIRA'})"
        "-[:RECEBEU_SALARIO]->(:PublicOffice {cpf: 'classified_x'})",
    )

    _run_phases(neo4j_driver, [5])

    assert _count_same_as(neo4j_driver) == 0


@pytest.mark.integration
def test_phase5_requires_recebeu_salario(neo4j_driver: Driver) -> None:
    """Person without RECEBEU_SALARIO should not match in Phase 5."""
    _clear_db(neo4j_driver)
    _setup(
        neo4j_driver,
        "CREATE (:Person {cpf: '026.005.602-20', name: 'JOSE DIAS TOFFOLI'})",
    )
    _run_phases(neo4j_driver, [0])
    _setup(
        neo4j_driver,
        # Amendment author — no RECEBEU_SALARIO relationship
        "CREATE (:Person {name: 'JOSE DIAS TOFFOLI', author_key: 'toffoli'})",
    )

    _run_phases(neo4j_driver, [5])

    assert _count_same_as(neo4j_driver, "unique_name_match_servidor") == 0


@pytest.mark.integration
def test_phase5_no_duplicate_if_already_linked(
    neo4j_driver: Driver,
) -> None:
    """Running Phase 5 twice should not create duplicate SAME_AS."""
    _clear_db(neo4j_driver)
    _setup(
        neo4j_driver,
        "CREATE (:Person {cpf: '026.005.602-20', name: 'JOSE DIAS TOFFOLI'})",
    )
    _run_phases(neo4j_driver, [0])
    _setup(
        neo4j_driver,
        "CREATE (:Person {name: 'JOSE DIAS TOFFOLI'})"
        "-[:RECEBEU_SALARIO]->(:PublicOffice {cpf: 'classified_1'})",
    )

    _run_phases(neo4j_driver, [5])
    _run_phases(neo4j_driver, [5])  # idempotent

    assert _count_same_as(neo4j_driver) == 1


@pytest.mark.integration
def test_phase5_servidor_with_cpf_partial_skipped(
    neo4j_driver: Driver,
) -> None:
    """Servidor with cpf_partial IS NOT NULL should not match in Phase 5.

    Phase 5 only handles blank-CPF servidores (cpf_partial IS NULL).
    Partial-CPF matching is intentionally disabled.
    """
    _clear_db(neo4j_driver)
    _setup(
        neo4j_driver,
        "CREATE (:Person {cpf: '026.005.602-20', name: 'JOSE DIAS TOFFOLI'})",
    )
    _run_phases(neo4j_driver, [0])
    _setup(
        neo4j_driver,
        "CREATE (:Person {name: 'JOSE DIAS TOFFOLI', cpf_partial: '005602'})"
        "-[:RECEBEU_SALARIO]->(:PublicOffice {cpf: 'partial_1'})",
    )

    _run_phases(neo4j_driver, [5])

    assert _count_same_as(neo4j_driver, "unique_name_match_servidor") == 0
