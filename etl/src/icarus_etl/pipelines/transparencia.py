from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from icarus_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from icarus_etl.loader import Neo4jBatchLoader
from icarus_etl.transforms import (
    deduplicate_rows,
    format_cnpj,
    format_cpf,
    normalize_name,
    strip_document,
)


def _parse_brl(value: str | None) -> float:
    """Parse Brazilian monetary string to float.

    Handles formats like "1.234.567,89" and "1234567.89".
    """
    if not value:
        return 0.0
    cleaned = str(value).strip()
    # Remove currency symbol and whitespace
    cleaned = re.sub(r"[R$\s]", "", cleaned)
    if not cleaned:
        return 0.0
    # Brazilian format: dots as thousands sep, comma as decimal
    if "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


class TransparenciaPipeline(Pipeline):
    """ETL pipeline for Portal da Transparencia federal spending data."""

    name = "transparencia"
    source_id = "portal_transparencia"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size)
        self._raw_contratos: pd.DataFrame = pd.DataFrame()
        self._raw_servidores: pd.DataFrame = pd.DataFrame()
        self._raw_emendas: pd.DataFrame = pd.DataFrame()
        self.contracts: list[dict[str, Any]] = []
        self.offices: list[dict[str, Any]] = []
        self.amendments: list[dict[str, Any]] = []

    def extract(self) -> None:
        src_dir = Path(self.data_dir) / "transparencia"
        self._raw_contratos = pd.read_csv(
            src_dir / "contratos.csv",
            dtype=str,
            keep_default_na=False,
            encoding="utf-8",
        )
        self._raw_servidores = pd.read_csv(
            src_dir / "servidores.csv",
            dtype=str,
            keep_default_na=False,
            encoding="utf-8",
        )
        self._raw_emendas = pd.read_csv(
            src_dir / "emendas.csv",
            dtype=str,
            keep_default_na=False,
            encoding="utf-8",
        )

    def transform(self) -> None:
        contracts: list[dict[str, Any]] = []
        for _, row in self._raw_contratos.iterrows():
            cnpj = format_cnpj(str(row["cnpj_contratada"]))
            contracts.append({
                "contract_id": (
                    f"{strip_document(str(row['cnpj_contratada']))}_{row['data_inicio']}"
                ),
                "object": normalize_name(str(row["objeto"])),
                "value": _parse_brl(str(row["valor"])),
                "contracting_org": normalize_name(str(row["orgao_contratante"])),
                "date": str(row["data_inicio"]),
                "cnpj": cnpj,
                "razao_social": normalize_name(str(row["razao_social"])),
            })
        self.contracts = deduplicate_rows(contracts, ["contract_id"])

        offices: list[dict[str, Any]] = []
        for _, row in self._raw_servidores.iterrows():
            offices.append({
                "cpf": format_cpf(str(row["cpf"])),
                "name": normalize_name(str(row["nome"])),
                "org": normalize_name(str(row["orgao"])),
                "salary": _parse_brl(str(row["remuneracao"])),
            })
        self.offices = deduplicate_rows(offices, ["cpf"])

        amendments: list[dict[str, Any]] = []
        for _, row in self._raw_emendas.iterrows():
            amendments.append({
                "cpf": format_cpf(str(row["cpf_autor"])),
                "name": normalize_name(str(row["nome_autor"])),
                "object": normalize_name(str(row["objeto"])),
                "value": _parse_brl(str(row["valor"])),
            })
        self.amendments = deduplicate_rows(amendments, ["cpf", "object"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.contracts:
            loader.load_nodes(
                "Contract",
                [
                    {
                        "contract_id": c["contract_id"],
                        "object": c["object"],
                        "value": c["value"],
                        "contracting_org": c["contracting_org"],
                        "date": c["date"],
                    }
                    for c in self.contracts
                ],
                key_field="contract_id",
            )
            # Ensure Company nodes exist for contracted companies
            companies = deduplicate_rows(
                [{"cnpj": c["cnpj"], "razao_social": c["razao_social"]} for c in self.contracts],
                ["cnpj"],
            )
            loader.load_nodes("Company", companies, key_field="cnpj")

            # VENCEU: Company -> Contract
            loader.load_relationships(
                rel_type="VENCEU",
                rows=[
                    {"source_key": c["cnpj"], "target_key": c["contract_id"]}
                    for c in self.contracts
                ],
                source_label="Company",
                source_key="cnpj",
                target_label="Contract",
                target_key="contract_id",
            )

        if self.offices:
            loader.load_nodes("PublicOffice", self.offices, key_field="cpf")

            # Ensure Person nodes exist
            persons = deduplicate_rows(
                [{"name": o["name"], "cpf": o["cpf"]} for o in self.offices],
                ["cpf"],
            )
            loader.load_nodes("Person", persons, key_field="cpf")

            # RECEBEU_SALARIO: Person -> PublicOffice
            loader.load_relationships(
                rel_type="RECEBEU_SALARIO",
                rows=[
                    {"source_key": o["cpf"], "target_key": o["cpf"]}
                    for o in self.offices
                ],
                source_label="Person",
                source_key="cpf",
                target_label="PublicOffice",
                target_key="cpf",
            )

        if self.amendments:
            # Ensure Person nodes exist for amendment authors
            persons = deduplicate_rows(
                [{"name": a["name"], "cpf": a["cpf"]} for a in self.amendments],
                ["cpf"],
            )
            loader.load_nodes("Person", persons, key_field="cpf")

            # AUTOR_EMENDA: Person -> Contract (match by object)
            loader.load_relationships(
                rel_type="AUTOR_EMENDA",
                rows=[
                    {"source_key": a["cpf"], "target_key": a["object"]}
                    for a in self.amendments
                ],
                source_label="Person",
                source_key="cpf",
                target_label="Contract",
                target_key="object",
            )
