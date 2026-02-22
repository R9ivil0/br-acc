import logging
from abc import ABC, abstractmethod

from neo4j import Driver

logger = logging.getLogger(__name__)


class Pipeline(ABC):
    """Base class for all ETL pipelines."""

    name: str
    source_id: str

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
    ) -> None:
        self.driver = driver
        self.data_dir = data_dir
        self.limit = limit
        self.chunk_size = chunk_size

    @abstractmethod
    def extract(self) -> None:
        """Download raw data from source."""

    @abstractmethod
    def transform(self) -> None:
        """Normalize, deduplicate, and prepare data for loading."""

    @abstractmethod
    def load(self) -> None:
        """Load transformed data into Neo4j."""

    def run(self) -> None:
        """Execute the full ETL pipeline."""
        logger.info("[%s] Starting extraction...", self.name)
        self.extract()
        logger.info("[%s] Starting transformation...", self.name)
        self.transform()
        logger.info("[%s] Starting load...", self.name)
        self.load()
        logger.info("[%s] Pipeline complete.", self.name)
