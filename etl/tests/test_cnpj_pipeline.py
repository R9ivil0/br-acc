from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from icarus_etl.pipelines.cnpj import CNPJPipeline, parse_capital_social

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline(data_dir: str | None = None, **kwargs: object) -> CNPJPipeline:
    driver = MagicMock()
    if data_dir is None:
        data_dir = str(FIXTURES)
    return CNPJPipeline(driver=driver, data_dir=data_dir, **kwargs)  # type: ignore[arg-type]


# --- parse_capital_social ---


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("7500000000,00", 7500000000.0),
        ("750000000,00", 750000000.0),
        ("0,00", 0.0),
        ("1234,56", 1234.56),
        ("7500000000", 7500000000.0),
        ("", 0.0),
        ("  ", 0.0),
    ],
)
def test_parse_capital_social(raw: str, expected: float) -> None:
    assert parse_capital_social(raw) == expected


# --- Pipeline metadata ---


def test_pipeline_metadata() -> None:
    pipeline = _make_pipeline()
    assert pipeline.name == "cnpj"
    assert pipeline.source_id == "receita_federal"


# --- RF format (extract + transform) ---


def test_extract_rf_format() -> None:
    """Extract reads RF-format files from fixtures/cnpj/."""
    pipeline = _make_pipeline()
    pipeline.extract()

    assert len(pipeline._raw_empresas) == 3
    assert len(pipeline._raw_socios) == 3
    assert len(pipeline._raw_estabelecimentos) == 3
    assert "cnpj_basico" in pipeline._raw_empresas.columns


def test_transform_rf_format_companies() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 3
    first = pipeline.companies[0]
    assert "cnpj" in first
    assert "razao_social" in first
    assert "capital_social" in first
    assert "uf" in first
    assert "municipio" in first
    assert "natureza_juridica" in first
    assert "porte_empresa" in first


def test_transform_rf_format_parses_capital_social() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    bb = next(c for c in pipeline.companies if "BANCO DO BRASIL" in c["razao_social"])
    assert bb["capital_social"] == 7500000000.0


def test_transform_rf_format_builds_full_cnpj() -> None:
    """Full CNPJ constructed from cnpj_basico + cnpj_ordem + cnpj_dv via Estabelecimentos."""
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    cnpjs = [c["cnpj"] for c in pipeline.companies]
    for cnpj in cnpjs:
        assert "/" in cnpj
        assert "-" in cnpj


def test_transform_rf_format_normalizes_names() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    names = [c["razao_social"] for c in pipeline.companies]
    for name in names:
        assert name == name.upper()
        assert "  " not in name


def test_transform_rf_format_extracts_partners() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.partners) == 3
    partner_names = [p["name"] for p in pipeline.partners]
    assert "JOAO DA SILVA" in partner_names
    assert "MARIA SANTOS" in partner_names


def test_transform_rf_format_extracts_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.relationships) == 3
    rel = pipeline.relationships[0]
    assert "source_key" in rel
    assert "target_key" in rel
    assert "tipo_socio" in rel
    assert "qualificacao" in rel
    assert "data_entrada" in rel


def test_transform_rf_format_preserves_partial_cpfs() -> None:
    """Partial CPFs from Receita Federal are stored as-is for entity resolution."""
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    cpfs = [p["cpf"] for p in pipeline.partners]
    assert any("***" in cpf or "*" in cpf for cpf in cpfs)


# --- Simple CSV format (transform only, bypass extract) ---


def test_transform_simple_format() -> None:
    pipeline = _make_pipeline()
    pipeline._raw_empresas = pd.read_csv(
        FIXTURES / "cnpj_empresas.csv", dtype=str, keep_default_na=False,
    )
    pipeline._raw_socios = pd.read_csv(
        FIXTURES / "cnpj_socios.csv", dtype=str, keep_default_na=False,
    )
    pipeline.transform()

    assert len(pipeline.companies) == 3
    assert len(pipeline.partners) == 3
    assert len(pipeline.relationships) == 3


def test_transform_simple_format_formats_cnpj() -> None:
    pipeline = _make_pipeline()
    pipeline._raw_empresas = pd.read_csv(
        FIXTURES / "cnpj_empresas.csv", dtype=str, keep_default_na=False,
    )
    pipeline._raw_socios = pd.read_csv(
        FIXTURES / "cnpj_socios.csv", dtype=str, keep_default_na=False,
    )
    pipeline.transform()

    cnpjs = [c["cnpj"] for c in pipeline.companies]
    for cnpj in cnpjs:
        assert "/" in cnpj
        assert "-" in cnpj


# --- Deduplication ---


def test_transform_deduplicates_by_cnpj() -> None:
    pipeline = _make_pipeline()
    pipeline._raw_empresas = pd.DataFrame([
        {
            "cnpj": "00000000000191",
            "razao_social": "Banco do Brasil",
            "cnae_principal": "6421200",
            "capital_social": "7500000000",
            "uf": "DF",
            "municipio": "Brasilia",
        },
        {
            "cnpj": "00000000000191",
            "razao_social": "Banco do Brasil (duplicate)",
            "cnae_principal": "6421200",
            "capital_social": "7500000000",
            "uf": "DF",
            "municipio": "Brasilia",
        },
    ])
    pipeline._raw_socios = pd.DataFrame(columns=["cnpj", "nome_socio", "cpf_socio", "tipo_socio"])
    pipeline.transform()

    assert len(pipeline.companies) == 1


# --- Limit / chunk_size ---


def test_limit_caps_rows() -> None:
    pipeline = _make_pipeline(limit=2)
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) <= 2


def test_chunk_size_parameter() -> None:
    pipeline = _make_pipeline(chunk_size=1)
    pipeline.extract()
    # Should still read all rows, just in smaller chunks
    assert len(pipeline._raw_empresas) == 3
