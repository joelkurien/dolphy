import polars as pl
from typing import Optional, List, Any
from operations import (
    Drop, 
    ImputeNa,
    OutlierHandling,
    Rename,
    Transformation, 
    Filter,
    Standardize,
    StringNormalize
) 

class CleanerPipeline:
    def __init__(self, df:pl.DataFrame):
        self.df = df
        self.operations = []

    def drop_na(self, columns: Optional[List[str]] = None, 
                strategy: str = 'null'):
        self.operations.append(Drop(columns, strategy))
        return self
    
    def impute_na(self, columns: Optional[List[str]] = None, 
                  value: Any = None, 
                  strategy: str = 'default'):

        self.operations.append(ImputeNa(columns, value, strategy))
        return self

    def handle_outlier(self, column: str, strategy: str = 'remove'):

        self.operations.append(OutlierHandling(column = column, 
                                               strategy = strategy))
        return self
    
    def rename(self, **mapping):
        self.operations.append(Rename(mapping))
        return self

    def transform(self, columns: List[str], 
                  strategy: str = 'log', 
                  replace: bool = False):
        self.operations.append(Transformation(columns, strategy, replace))
        return self

    def filter(self, expression: str):
        self.operations.append(Filter(expression))
        return self

    def standardization(self, columns: Optional[List[str]] = None, strategy: str = 'z-score'):
        self.operations.append(Standardize(columns, strategy))
        return self

    def normalize(self, columns: Optional[List[str]] = None, strategy: str = 'lower'):
        self.operations.append(StringNormalize(columns, strategy))
        return self
    
    def execute(self) -> pl.DataFrame:
        result = self.df.lazy()
        for op in self.operations:
            op.clean()
        return result.collect()

