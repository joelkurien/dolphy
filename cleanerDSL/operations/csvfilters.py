from dataclasses import dataclass
import polars as pl
from .base import Operations
from typing import Optional, List
from .filterparser import FilterParser

@dataclass
class Filter(Operations):
    expression: str

    fp = FilterParser()
    def clean(self, result: pl.LazyFrame) -> pl.LazyFrame:
        return result.filter(self.fp.parse(self.expression))

@dataclass
class Drop(Operations):
    columns: Optional[List[str]]
    strategy: str = "drop-null"

    def clean(self, result: pl.LazyFrame) -> pl.LazyFrame:
        try:
            if self.columns is not None:
                for column in self.columns:
                    if column not in result.schema:
                        raise ValueError("Column not in the table")

            match self.strategy: 
                case "drop-null":
                    if self.columns is None:
                        return result.drop_nulls()
                    else:
                        return result.drop_nulls(subset=self.columns)
                case "drop-nan":
                    if self.columns is not None and len(self.columns) > 0:
                        for col in self.columns:
                            result = result.filter(pl.col(col).is_not_nan())
                case "drop-null-row":
                    result = result.filter(~pl.all_horizontal(pl.all().is_null()))
                case "drop-null-nan":
                    if self.columns is not None and len(self.columns) > 0:
                        for col in self.columns:
                            result = result.filter(pl.col(col).is_finite())
        except Exception as e:
            print(f"Failed to drop rows: {e}")
        return result 


