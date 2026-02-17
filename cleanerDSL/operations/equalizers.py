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
        if self.columns is None:
            self.columns = []
            for col in result.schema.keys():
                if result.schema[col] != pl.String:
                    self.columns.append(col)
        try:
            for column in self.columns:
                if column not in result.schema:
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
                return result.with_columns(expression)
        except Exception as e:
            print(f"Failed to standardize due due to the following error: {e}")
        return result 

@dataclass
class StringNormalize(Operations):
    columns: Optional[List[str]]
    strategy: str = "lower"
    inplace: bool = False
    delimiter: Optional[str] = None

    def clean(self, result: pl.LazyFrame) -> pl.LazyFrame:
        if self.columns is None:
            self.columns = []
            for col in result.schema.keys():
                if result.schema[col] == pl.String:
                    self.columns.append(col) 
        try:
            for column in self.columns:
                if column not in result.schema:
                    raise ValueError(f"Column: {column} is not present in the CSV")

                alias = f"{column}_{self.strategy}" if not self.inplace else column
                col = pl.col(column)
                match self.strategy:
                    case "lower":
                        expression = col.str.to_lowercase()
                    case "upper":
                        expression = col.str.to_uppercase()
                    case "strip":
                        expression = col.str.strip_chars()
                    case "one-hot encoding":
                        if self.delimiter == None:
                            raise ValueError("Delimiter needs to be specified")
                        temp_df = result.collect()
                        encode = temp_df.to_dummies(
                            columns = [column],
                            separator = self.delimiter
                        )
                        return encode.lazy()
                    case "label encoding":
                        expression = col.cast(pl.Categorical).to_physical()
                    case _:
                        raise ValueError(f"Strategy {self.strategy} not found")
                return result.with_columns(expression.alias(alias))
        except Exception as e:
            print(f"String Normalization failed due to : {e}")
        return result

