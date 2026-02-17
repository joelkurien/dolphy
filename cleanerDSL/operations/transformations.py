from dataclasses import dataclass
import polars as pl
from typing import Optional, Any, List
from scipy import stats
from .base import Operations

@dataclass
class Transformation(Operations):
    columns: List[str]
    strategy: str = 'log-transform'
    replace_columns: bool = False

    def clean(self, result: pl.LazyFrame) -> pl.LazyFrame:
        try:
            for column in self.columns:
                if result.schema[column] == pl.String:
                    raise TypeError("Transformation strategy is not compatible with a String column => cast it or do something else")
                match self.strategy:
                    case 'log-transform':
                        if self.replace_columns:
                            result = result.with_columns(pl.col(column).log1p())
                        else:
                            result = result.with_columns(pl.col(column)
                                                     .log()
                                                     .alias(f"{column}_sqrt"))
                    case 'sqrt-transform':
                        if self.replace_columns:
                            result = result.with_columns(pl.col(column).sqrt())
                        else:
                            result = result.with_columns(pl.col(column)
                                                     .sqrt()
                                                     .alias(f"{column}_sqrt"))
                    case 'reciprocal-transform':
                        if self.replace_columns:
                            result = result.with_columns(1/pl.col(column))
                        else:
                            result = result.with_columns((1/pl.col(column))
                                                     .alias(f"{column}_sqrt"))
                    case 'yeojohnson-transform':
                        if self.replace_columns:
                            result = result.with_columns(
                                    pl.col(column).map_batches(lambda x: pl.Series(stats.yeojohnson(x.to_numpy())[0]))
                            )
                    case 'square-transform':
                        if self.replace_columns:
                            result = result.with_columns(pl.col(column)**2)
                        else:
                            result = result.with_columns((pl.col(column)**2)
                                                     .alias(f"{column}_sqrt"))
        except Exception as e:
            print(f"Failed to perform transformation due to the following error: {e}")
        return result 

@dataclass
class ImputeNa(Operations):
    columns: Optional[List[str]] = None
    value: Any = None
    strategy: str = 'default'

    def clean(self, result: pl.LazyFrame) -> pl.LazyFrame:
        if not self.columns:
            self.columns = list(result.schema.keys())
        try:
            for column in self.columns:
                match self.strategy:
                    case 'default':
                        result = result.with_columns(
                            pl.col(column).fill_null(self.value)        
                        )
                    case 'forward':
                        result = result.with_columns(
                            pl.col(column).fill_null(strategy = self.strategy)  
                        )
                    case 'backward':
                        result = result.with_columns(
                            pl.col(column).fill_null(strategy = self.strategy)
                        )
                    case 'mean':
                        result = result.with_columns(
                            pl.col(column).fill_null(pl.col(column).mean())
                        )
                    case 'median':
                        result = result.with_columns(
                            pl.col(column).fill_null(pl.col(column).median())
                        )
                    case 'mode':
                        result = result.with_columns(
                            pl.col(column).fill_null(pl.col(column).mode().first())
                        )
                    case _:
                        raise ValueError("Strategy not found")
        except Exception as e:
            print(f"Failure in null value imputation: {e}")
        return result

@dataclass
class OutlierHandling(Operations):
    column: str
    strategy: str = 'remove'

    def clean(self, result: pl.LazyFrame) -> pl.LazyFrame:
        try:
            if self.column not in result.schema:
                raise ValueError("Column is not present in the dataframe")

            q1 = pl.col(self.column).quantile(0.25)
            q3 = pl.col(self.column).quantile(0.75)
            iqr = q3 - q1

            upper_bound = q3 + 1.5*iqr
            lower_bound = q1 - 1.5*iqr
            
            column = pl.col(self.column)
            match self.strategy: 
                case "remove":
                    return result.filter((column >= lower_bound) & 
                                         (column <= upper_bound))
                case "cap":
                    return result.with_columns(
                            column.clip(lower_bound, upper_bound).alias(self.column)
                    )
                case "mean replace":
                    mean_val = column.mean()
                    return result.with_columns(
                        pl.when((column < lower_bound) | (column > upper_bound))
                        .then(mean_val)
                        .otherwise(column)
                        .alias(self.column)
                    )
                case "median replace":
                    median_val = column.median()
                    return result.with_columns(
                        pl.when((column < lower_bound) | (column > upper_bound))
                        .then(median_val)
                        .otherwise(column)
                        .alias(self.column)
                    )
                case "null replace":
                    return result.with_columns(
                        pl.when((column < lower_bound) | (column > upper_bound))
                        .then(None)
                        .otherwise(column)
                        .alias(self.column)
                    )
                case _:
                    raise ValueError(f"Unknown Strategy: {self.strategy}")
        except Exception as e:
            print(f"Outlier Handling failed due to the following issue: {e}")
        
        return result

@dataclass
class Rename(Operations):
    mapping: Any

    def clean(self, result: pl.LazyFrame) -> pl.LazyFrame:
        return result.rename(self.mapping)

