#!/usr/bin/env python3
"""Explore CNPJ data via Base dos Dados (BigQuery).

Requires:
    1. gcloud auth: `gcloud auth application-default login`
    2. basedosdados: `uv pip install basedosdados`

Usage:
    python etl/scripts/explore_cnpj_bd.py                  # explore all tables
    python etl/scripts/explore_cnpj_bd.py --export-state DF # export DF subset to CSV
    python etl/scripts/explore_cnpj_bd.py --limit 50        # limit sample rows
"""

from __future__ import annotations

import logging
import sys

import click

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BD_DATASET = "br_me_cnpj"
TABLES = ["empresas", "socios", "estabelecimentos"]


def _query(sql: str, billing_project: str | None = None) -> "pd.DataFrame":
    """Execute a BigQuery SQL query via basedosdados."""
    import basedosdados as bd

    logger.info("Querying: %s", sql[:120])
    return bd.read_sql(sql, billing_project_id=billing_project)


def _explore_table(table: str, limit: int, billing_project: str | None) -> None:
    """Print schema, sample values, and null counts for a table."""
    import pandas as pd

    sql = f"SELECT * FROM `basedosdados.{BD_DATASET}.{table}` LIMIT {limit}"
    df = _query(sql, billing_project)

    print(f"\n{'=' * 60}")
    print(f"  TABLE: {BD_DATASET}.{table}")
    print(f"  Rows sampled: {len(df)}")
    print(f"{'=' * 60}")

    print(f"\n  Columns ({len(df.columns)}):")
    for col in df.columns:
        dtype = df[col].dtype
        nulls = df[col].isna().sum()
        sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else "N/A"
        sample_str = str(sample)[:50]
        print(f"    {col:<40} {str(dtype):<10} nulls={nulls:<4} sample={sample_str}")

    print(f"\n  First 3 rows:")
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    print(df.head(3).to_string(index=False))


def _export_state_subset(
    state: str,
    output_dir: str,
    limit: int,
    billing_project: str | None,
) -> None:
    """Export a subset of data filtered by UF (state) to local CSV."""
    from pathlib import Path

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Estabelecimentos for state
    sql_estab = (
        f"SELECT * FROM `basedosdados.{BD_DATASET}.estabelecimentos` "
        f"WHERE sigla_uf = '{state}' LIMIT {limit}"
    )
    df_estab = _query(sql_estab, billing_project)
    estab_path = out / f"estabelecimentos_{state}.csv"
    df_estab.to_csv(estab_path, index=False)
    logger.info("Exported %d estabelecimentos to %s", len(df_estab), estab_path)

    # Get cnpj_basico values for joining
    if not df_estab.empty and "cnpj_basico" in df_estab.columns:
        basicos = df_estab["cnpj_basico"].unique().tolist()
        basicos_str = ",".join(f"'{b}'" for b in basicos[:10000])

        sql_emp = (
            f"SELECT * FROM `basedosdados.{BD_DATASET}.empresas` "
            f"WHERE cnpj_basico IN ({basicos_str})"
        )
        df_emp = _query(sql_emp, billing_project)
        emp_path = out / f"empresas_{state}.csv"
        df_emp.to_csv(emp_path, index=False)
        logger.info("Exported %d empresas to %s", len(df_emp), emp_path)

        sql_soc = (
            f"SELECT * FROM `basedosdados.{BD_DATASET}.socios` "
            f"WHERE cnpj_basico IN ({basicos_str})"
        )
        df_soc = _query(sql_soc, billing_project)
        soc_path = out / f"socios_{state}.csv"
        df_soc.to_csv(soc_path, index=False)
        logger.info("Exported %d socios to %s", len(df_soc), soc_path)

    logger.info("Export complete for state %s", state)


@click.command()
@click.option("--limit", type=int, default=100, help="Sample rows per table")
@click.option("--export-state", type=str, default=None, help="Export subset for state (e.g. DF, SP)")
@click.option("--output-dir", default="./data/cnpj/extracted", help="Output directory for exports")
@click.option("--billing-project", type=str, default=None, help="GCP billing project ID")
def main(
    limit: int,
    export_state: str | None,
    output_dir: str,
    billing_project: str | None,
) -> None:
    """Explore CNPJ data from Base dos Dados (BigQuery)."""
    try:
        import basedosdados  # noqa: F401
    except ImportError:
        logger.error("basedosdados not installed. Run: uv pip install 'basedosdados>=2.0.0'")
        sys.exit(1)

    if export_state:
        _export_state_subset(export_state, output_dir, limit, billing_project)
    else:
        for table in TABLES:
            _explore_table(table, limit, billing_project)


if __name__ == "__main__":
    main()
