from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from icarus_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from icarus_etl.loader import Neo4jBatchLoader
from icarus_etl.transforms import deduplicate_rows, format_cnpj, normalize_name

logger = logging.getLogger(__name__)

# Receita Federal CSV column names (files have no headers)
EMPRESAS_COLS = [
    "cnpj_basico",
    "razao_social",
    "natureza_juridica",
    "qualificacao_responsavel",
    "capital_social",
    "porte_empresa",
    "ente_federativo",
]

SOCIOS_COLS = [
    "cnpj_basico",
    "identificador_socio",
    "nome_socio",
    "cpf_cnpj_socio",
    "qualificacao_socio",
    "data_entrada",
    "pais",
    "representante_legal",
    "nome_representante",
    "qualificacao_representante",
    "faixa_etaria",
]

ESTABELECIMENTOS_COLS = [
    "cnpj_basico",
    "cnpj_ordem",
    "cnpj_dv",
    "identificador_matriz_filial",
    "nome_fantasia",
    "situacao_cadastral",
    "data_situacao_cadastral",
    "motivo_situacao_cadastral",
    "nome_cidade_exterior",
    "pais",
    "data_inicio_atividade",
    "cnae_principal",
    "cnae_secundaria",
    "tipo_logradouro",
    "logradouro",
    "numero",
    "complemento",
    "bairro",
    "cep",
    "uf",
    "municipio",
    "ddd1",
    "telefone1",
    "ddd2",
    "telefone2",
    "ddd_fax",
    "fax",
    "email",
    "situacao_especial",
    "data_situacao_especial",
]

# Reference tables: 2-column CSVs (codigo, descricao)
REFERENCE_TABLES = [
    "Naturezas",
    "Qualificacoes",
    "Cnaes",
    "Municipios",
    "Paises",
    "Motivos",
]


def parse_capital_social(value: str) -> float:
    """Parse Receita Federal capital_social format.

    RF uses comma as decimal separator: '750000000,00' -> 750000000.00
    Simple format uses plain numbers: '7500000000' -> 7500000000.0
    """
    if not value or value.strip() == "":
        return 0.0
    cleaned = value.strip().replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


class CNPJPipeline(Pipeline):
    """ETL pipeline for Receita Federal CNPJ open data.

    Supports two data formats:
    - Real Receita Federal: headerless CSVs (`;` delimiter, latin-1) with multiple files
    - Simple CSV: header-based CSVs for testing/development
    """

    name = "cnpj"
    source_id = "receita_federal"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size)
        self._raw_empresas: pd.DataFrame = pd.DataFrame()
        self._raw_socios: pd.DataFrame = pd.DataFrame()
        self._raw_estabelecimentos: pd.DataFrame = pd.DataFrame()
        self._reference_tables: dict[str, dict[str, str]] = {}
        self.companies: list[dict[str, Any]] = []
        self.partners: list[dict[str, Any]] = []
        self.relationships: list[dict[str, Any]] = []

    # --- Reference tables ---

    def _load_reference_tables(self) -> None:
        """Load reference lookup tables (naturezas, qualificacoes, etc.)."""
        ref_dir = Path(self.data_dir) / "cnpj" / "reference"
        if not ref_dir.exists():
            return

        for table_name in REFERENCE_TABLES:
            files = list(ref_dir.glob(f"*{table_name}*"))
            if not files:
                continue
            try:
                df = pd.read_csv(
                    files[0],
                    sep=";",
                    encoding="latin-1",
                    header=None,
                    names=["codigo", "descricao"],
                    dtype=str,
                    keep_default_na=False,
                )
                lookup = dict(zip(df["codigo"], df["descricao"], strict=False))
                self._reference_tables[table_name.lower()] = lookup
                logger.info("Loaded reference table %s: %d entries", table_name, len(lookup))
            except Exception:
                logger.warning("Could not load reference table %s", table_name)

    def _resolve_reference(self, table: str, code: str) -> str:
        """Look up a code in a reference table. Returns code if not found."""
        lookup = self._reference_tables.get(table, {})
        return lookup.get(code.strip(), code) if code else code

    # --- Reading ---

    def _read_rf_chunks(self, pattern: str, columns: list[str]) -> pd.DataFrame:
        """Read Receita Federal headerless CSVs with chunking for memory efficiency."""
        cnpj_dir = Path(self.data_dir) / "cnpj"
        # Search both extracted/ subdirectory and cnpj/ root
        files = sorted(cnpj_dir.glob(f"extracted/{pattern}"))
        if not files:
            files = sorted(cnpj_dir.glob(pattern))
        if not files:
            return pd.DataFrame(columns=columns)

        frames: list[pd.DataFrame] = []
        total_rows = 0
        for f in files:
            logger.info("Reading %s...", f.name)
            for chunk in pd.read_csv(
                f,
                sep=";",
                encoding="latin-1",
                header=None,
                names=columns,
                dtype=str,
                keep_default_na=False,
                chunksize=self.chunk_size,
            ):
                frames.append(chunk)
                total_rows += len(chunk)
                if self.limit and total_rows >= self.limit:
                    break
            if self.limit and total_rows >= self.limit:
                break

        if not frames:
            return pd.DataFrame(columns=columns)
        result = pd.concat(frames, ignore_index=True)
        if self.limit:
            result = result.head(self.limit)
        logger.info("Read %d rows from %s", len(result), pattern)
        return result

    def extract(self) -> None:
        """Extract data from Receita Federal open data files.

        Tries real RF format first (headerless `;`-delimited files), then falls back to
        simple header-based CSVs for dev/test environments.
        """
        # Load reference tables if available
        self._load_reference_tables()

        cnpj_dir = Path(self.data_dir) / "cnpj"

        # Try real RF format: *EMPRE* or Empresas*
        rf_empresas = self._read_rf_chunks("*EMPRE*", EMPRESAS_COLS)
        if rf_empresas.empty:
            rf_empresas = self._read_rf_chunks("Empresas*", EMPRESAS_COLS)

        if not rf_empresas.empty:
            self._raw_empresas = rf_empresas
            self._raw_socios = self._read_rf_chunks("*SOCIO*", SOCIOS_COLS)
            if self._raw_socios.empty:
                self._raw_socios = self._read_rf_chunks("Socios*", SOCIOS_COLS)
            self._raw_estabelecimentos = self._read_rf_chunks(
                "*ESTABELE*", ESTABELECIMENTOS_COLS,
            )
            if self._raw_estabelecimentos.empty:
                self._raw_estabelecimentos = self._read_rf_chunks(
                    "Estabelecimentos*", ESTABELECIMENTOS_COLS,
                )
        else:
            # Simple CSV fallback (dev/test)
            empresas_path = cnpj_dir / "empresas.csv"
            socios_path = cnpj_dir / "socios.csv"
            estabelecimentos_path = cnpj_dir / "estabelecimentos.csv"
            if empresas_path.exists():
                self._raw_empresas = pd.read_csv(
                    empresas_path, dtype=str, keep_default_na=False,
                )
            if socios_path.exists():
                self._raw_socios = pd.read_csv(
                    socios_path, dtype=str, keep_default_na=False,
                )
            if estabelecimentos_path.exists():
                self._raw_estabelecimentos = pd.read_csv(
                    estabelecimentos_path, dtype=str, keep_default_na=False,
                )

        logger.info(
            "Extracted: %d empresas, %d socios, %d estabelecimentos",
            len(self._raw_empresas),
            len(self._raw_socios),
            len(self._raw_estabelecimentos),
        )

    def transform(self) -> None:
        """Transform raw data into normalized company, partner, and relationship records."""
        # Build CNPJ lookup from Estabelecimentos (if available)
        estab_cnpj: dict[str, dict[str, str]] = {}
        if not self._raw_estabelecimentos.empty:
            for _, row in self._raw_estabelecimentos.iterrows():
                basico = str(row["cnpj_basico"]).zfill(8)
                ordem = str(row["cnpj_ordem"]).zfill(4)
                dv = str(row["cnpj_dv"]).zfill(2)
                full_cnpj = f"{basico}{ordem}{dv}"
                if basico not in estab_cnpj:
                    estab_cnpj[basico] = {
                        "cnpj_full": format_cnpj(full_cnpj),
                        "cnae_principal": str(row["cnae_principal"]),
                        "uf": str(row["uf"]),
                        "municipio": str(row["municipio"]),
                    }

        # Detect format: RF format has "cnpj_basico", simple format has "cnpj"
        is_rf_format = "cnpj_basico" in self._raw_empresas.columns

        companies: list[dict[str, Any]] = []
        for _, row in self._raw_empresas.iterrows():
            if is_rf_format:
                basico = str(row["cnpj_basico"]).zfill(8)
                estab = estab_cnpj.get(basico, {})
                cnpj = estab.get("cnpj_full", format_cnpj(basico + "000100"))
                capital = parse_capital_social(str(row["capital_social"]))
                nat_juridica = self._resolve_reference(
                    "naturezas", str(row["natureza_juridica"]),
                )
                cnae = self._resolve_reference(
                    "cnaes", estab.get("cnae_principal", str(row.get("cnae_principal", ""))),
                )
                municipio_desc = self._resolve_reference(
                    "municipios", estab.get("municipio", ""),
                )
                companies.append({
                    "cnpj": cnpj,
                    "razao_social": normalize_name(str(row["razao_social"])),
                    "natureza_juridica": nat_juridica,
                    "cnae_principal": cnae,
                    "capital_social": capital,
                    "uf": estab.get("uf", ""),
                    "municipio": municipio_desc,
                    "porte_empresa": str(row["porte_empresa"]),
                })
            else:
                capital = parse_capital_social(str(row["capital_social"]))
                companies.append({
                    "cnpj": format_cnpj(str(row["cnpj"])),
                    "razao_social": normalize_name(str(row["razao_social"])),
                    "natureza_juridica": str(row.get("natureza_juridica", "")),
                    "cnae_principal": str(row["cnae_principal"]),
                    "capital_social": capital,
                    "uf": str(row["uf"]),
                    "municipio": str(row["municipio"]),
                    "porte_empresa": str(row.get("porte_empresa", "")),
                })
        self.companies = deduplicate_rows(companies, ["cnpj"])
        logger.info("Transformed %d companies", len(self.companies))

        # --- Partners ---
        is_rf_socios = "cpf_cnpj_socio" in self._raw_socios.columns

        partners: list[dict[str, Any]] = []
        relationships: list[dict[str, Any]] = []
        for _, row in self._raw_socios.iterrows():
            if is_rf_socios:
                basico = str(row["cnpj_basico"]).zfill(8)
                estab = estab_cnpj.get(basico, {})
                cnpj = estab.get("cnpj_full", format_cnpj(basico + "000100"))
                nome = normalize_name(str(row["nome_socio"]))
                cpf = str(row["cpf_cnpj_socio"])
                tipo = str(row["identificador_socio"])
                qualificacao = self._resolve_reference(
                    "qualificacoes", str(row["qualificacao_socio"]),
                )
                data_entrada = str(row["data_entrada"])
            else:
                cnpj = format_cnpj(str(row["cnpj"]))
                nome = normalize_name(str(row["nome_socio"]))
                cpf = str(row["cpf_socio"])
                tipo = str(row["tipo_socio"])
                qualificacao = str(row.get("qualificacao_socio", ""))
                data_entrada = str(row.get("data_entrada", ""))

            partners.append({
                "name": nome,
                "cpf": cpf,
                "tipo_socio": tipo,
            })
            relationships.append({
                "source_key": cpf,
                "target_key": cnpj,
                "tipo_socio": tipo,
                "qualificacao": qualificacao,
                "data_entrada": data_entrada,
            })

        self.partners = deduplicate_rows(partners, ["cpf"])
        self.relationships = relationships
        logger.info(
            "Transformed %d partners, %d relationships",
            len(self.partners),
            len(self.relationships),
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.companies:
            loader.load_nodes("Company", self.companies, key_field="cnpj")

        if self.partners:
            loader.load_nodes("Person", self.partners, key_field="cpf")

        if self.relationships:
            loader.load_relationships(
                rel_type="SOCIO_DE",
                rows=self.relationships,
                source_label="Person",
                source_key="cpf",
                target_label="Company",
                target_key="cnpj",
                properties=["tipo_socio", "qualificacao", "data_entrada"],
            )
