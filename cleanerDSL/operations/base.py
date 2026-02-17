from dataclasses import dataclass
from abc import ABC, abstractmethod
import polars as pl

@dataclass
class Operations(ABC):

    @abstractmethod
    def clean(self, result: pl.LazyFrame) -> pl.LazyFrame:
        pass
