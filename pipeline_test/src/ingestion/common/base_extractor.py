"""Base abstractions for ingestion extractors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path


class BaseExtractor(ABC):
    """Common interface for all extractors."""

    def __init__(self, output_dir: Path) -> None:
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    async def extract_full(self) -> Path:
        """Run a full extraction and return the output file path."""
