from dataclasses import dataclass
import polars as pl
from typing import Optional, List
from .base import Operations

@dataclass
class Standardize(Operations):
    columns: Optional[List[str]] = None
    strategy: str = "z-score"
    inplace: bool = False

    def clean(self, result: pl.LazyFrame) -> pl.LazyFrame:
        schema = result.collect_schema()
        
        if self.columns is None:
            self.columns = []
            for col in schema.keys():
                if schema[col] != pl.String:
                    self.columns.append(col)
        try:
            for column in self.columns:
                if column not in schema:
                    raise ValueError(f"Column: {column} is not present in the CSV")

                col = pl.col(column)
                alias = column if self.inplace else f"{column}_{self.strategy}"
                match self.strategy:
                    case "z-score":
                        expression = ((col - col.mean()) / col.std()).alias(alias)
                    case "min-max":
                        rng_min, rng_max = (0,1) 
                        expression = (
                            (col - col.min()) / (col.max() - col.min()) * 
                            (rng_max - rng_min) + rng_min
                        ).alias(alias)
                    case "robust":
                        q1 = col.quantile(0.25)
                        q3 = col.quantile(0.75)

                        expression = ((col - col.median()) / (q3 - q1)).alias(alias)
                    case _:
                        raise ValueError(f"Unknown Strategy: {self.strategy}")
                result = result.with_columns(expression)
        except Exception as e:
            print(f"Failed to standardize due due to the following error: {e}")
        return result 

@dataclass
class StringNormalize(Operations):
    columns: Optional[List[str]]
    strategy: str = "lower"

    def clean(self, result: pl.LazyFrame) -> pl.LazyFrame:
        schema = result.collect_schema()
        if self.columns is None:
            self.columns = []
            for col in schema.keys():
                if schema[col] == pl.String:
                    self.columns.append(col) 

        try:
            for column in self.columns:
                if column not in schema:
                    raise ValueError(f"Column: {column} is not present in the CSV")

                col = pl.col(column)
                match self.strategy:
                    case "lower":
                        expression = col.str.to_lowercase()
                    case "upper":
                        expression = col.str.to_uppercase()
                    case "strip":
                        expression = col.str.strip_chars()
                    case "label encoding":
                        expression = col.cast(pl.Categorical).to_physical()
                    case _:
                        raise ValueError(f"Strategy {self.strategy} not found")
                result = result.with_columns(expression)
        except Exception as e:
            print(f"String Normalization failed due to : {e}")
        return result
