import polars as pl
import re
import yaml
from cleanerpl import CleanerPipeline
from operations import ValidateCommand

class DSLInterpreter:
    def __init__(self, df: pl.DataFrame):
        self.dsl_engine = CleanerPipeline(df)
        ValidateCommand.set_schema(df.schema)

        self.rules = [
            (r"DROP ALL NULL ROWS", self._handle_drop_all_na),
            (r"DROP ANY NULL ROWS", self._handle_drop_any_na),
            (r"DROP ROWS WHERE (.+) IS (NULL|NAN)?", self._handle_drop_na_for_cols),
            (r"DROP NULL AND NAN ROWS", self._handle_drop_nan_na),
            (r"DROP ANY NAN ROWS", self._handle_drop_nan),
            (r'FILTER WHERE\s+(.+)', self._handle_filter),
            (r"NORMALISE COLUMNS (.+) USING ([\w-]+)", self._handle_normalise),
            (r"TRANSFORM COLUMNS (.+) USING (\w+)( INPLACE)?", self._handle_transform), 
            (r"FILL NULL IN COLUMN (.+) USING (\w+)", self._handle_col_impute),
            (r"FILL NULL USING (\w+)", self._handle_all_col_impute),
            (r"RENAME (.+) TO (.+)", self._handle_rename)
        ]

    def _handle_drop_all_na(self, match):
        self.dsl_engine.drop_na(strategy = "drop-null-row") 
    
    def _handle_drop_any_na(self, match):
        self.dsl_engine.drop_na()

    def _handle_drop_na_for_cols(self, match):
        cols = [c.strip() for c in match.group(1).split(",")]
        ValidateCommand(columns = cols)
        isNull = match.group(2)
        if isNull not in {'NULL', 'NAN'}:
            raise RuntimeError(f"{isNull}: This drop condition is not defined")
        self.dsl_engine.drop_na(columns = cols, strategy = f"drop-{isNull.lower()}")

    def _handle_drop_nan_na(self, match):
        self.dsl_engine.drop_na(strategy = "drop-null-nan")

    def _handle_drop_nan(self, match):
        self.dsl_engine.drop_na(strategy = "drop-nan")

    def _handle_filter(self, match):
        expr = match.group(1).strip()
        self.dsl_engine.filter(expression = expr)

    def _handle_normalise(self, match):
        std_strats = {'z-score', 'min-max', 'robust'}
        norm_strats = {'lower', 'upper', 'strip', 'label encoding'}
        
        cols = [c.strip() for c in match.group(1).split(",")]
        ValidateCommand(columns = cols)
        strategy = match.group(2).lower()
        if strategy in std_strats:
            self.dsl_engine.standardize(cols, strategy)
        elif strategy in norm_strats:
            self.dsl_engine.string_normalize(cols, strategy)

    def _handle_transform(self, match):
        groups = match.groups()
        cols = [c.strip() for c in match.group(1).split(",")]
        ValidateCommand(columns = cols)
        strategy = match.group(2).lower()
        inplace = groups[2] is not None
        self.dsl_engine.transform(cols, f"{strategy}-transform", inplace)

    def _handle_all_col_impute(self, match):
        strats = {'forward', 'backward', 'mean', 'median', 'mode'}
        group_2 = match.group(1)
        strategy = group_2.lower() if group_2.lower() in strats else "default"
        self.dsl_engine.impute_na(value = group_2, strategy = strategy) 

    def _handle_col_impute(self, match):
        cols = [c.strip() for c in match.group(1).split(",")]
        ValidateCommand(columns = cols)
        strats = {'forward', 'backward', 'mean', 'median', 'mode'}
        group_2 = match.group(2)
        strategy = group_2.lower() if group_2.lower() in strats else "default"
        self.dsl_engine.impute_na(columns = cols, value = group_2, strategy = strategy)

    def _handle_rename(self, match):
        old_cols = [c.strip() for c in match.group(1).split(",")]
        ValidateCommand(columns = old_cols)
        new_cols = [c.strip() for c in match.group(2).split(",")]
        self.dsl_engine.rename(**dict(zip(old_cols, new_cols)))

    def run(self, yml):
        result = pl.DataFrame() 
        commands = yaml.safe_load(yml)
        for cmd in commands:
            found = False
            for pattern, handler in self.rules:
                match = re.match(pattern, cmd, re.IGNORECASE)
                if match:
                    handler(match)
                    found = True
                    break
            if not found:
                raise ValueError(f"{cmd} is invalid")
        result = self.dsl_engine.execute()
        return result

