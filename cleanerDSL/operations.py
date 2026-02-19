"""
Tests for DSLInterpreter.

Assumes the project structure:
    your_package/
        __init__.py
        cleanerpl.py
        DSLInterpreter.py
        operations.py

Run with:
    pytest test_dsl_interpreter.py -v
"""


import math
import pytest
import polars as pl
from unittest.mock import MagicMock, patch, call
import yaml

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_df(**kwargs):
    """Convenience wrapper to build a small Polars DataFrame."""
    return pl.DataFrame(kwargs)



# ---------------------------------------------------------------------------
# Fixtures

# ---------------------------------------------------------------------------

@pytest.fixture
def basic_df():
    return make_df(
        age=[25, None, 35, 1000, 28],
        salary=[50_000, 60_000, None, 75_000, 55_000],
        name=["Alice", "BOB", "  Charlie", None, "dave"],
        city=["London", "paris", "BERLIN", "Madrid", None],
    )


@pytest.fixture
def interpreter(basic_df):
    """Return a fresh DSLInterpreter backed by basic_df with mocked pipeline."""
    from .DSLInterpreter import DSLInterpreter
    interp = DSLInterpreter(basic_df)
    interp.dsl_engine = MagicMock()
    interp.dsl_engine.execute.return_value = basic_df  # default return
    return interp


def run_yml(interp, *commands):
    """Serialise commands to YAML and call interp.run()."""
    return interp.run(yaml.dump(list(commands)))


# ---------------------------------------------------------------------------
# DROP commands
# ---------------------------------------------------------------------------

class TestDropCommands:

    def test_drop_all_null_rows(self, interpreter):
        run_yml(interpreter, "DROP ALL NULL ROWS")
        interpreter.dsl_engine.drop_na.assert_called_once_with(strategy="drop-null-row")

    def test_drop_any_null_rows(self, interpreter):
        run_yml(interpreter, "DROP ANY NULL ROWS")
        interpreter.dsl_engine.drop_na.assert_called_once_with()

    def test_drop_null_and_nan_rows(self, interpreter):
        run_yml(interpreter, "DROP NULL AND NAN ROWS")
        interpreter.dsl_engine.drop_na.assert_called_once_with(strategy="drop-null-nan")

    def test_drop_any_nan_rows(self, interpreter):
        run_yml(interpreter, "DROP ANY NAN ROWS")
        interpreter.dsl_engine.drop_na.assert_called_once_with(strategy="drop-nan")

    def test_drop_rows_where_column_is_null(self, interpreter):
        run_yml(interpreter, "DROP ROWS WHERE age IS NULL")
        interpreter.dsl_engine.drop_na.assert_called_once_with(
            columns=["age"], strategy="drop-null"
        )

    def test_drop_rows_where_column_is_nan(self, interpreter):

        run_yml(interpreter, "DROP ROWS WHERE salary IS NAN")
        interpreter.dsl_engine.drop_na.assert_called_once_with(
            columns=["salary"], strategy="drop-nan"
        )

    def test_drop_rows_where_multiple_columns(self, interpreter):
        run_yml(interpreter, "DROP ROWS WHERE age, salary IS NULL")
        interpreter.dsl_engine.drop_na.assert_called_once_with(
            columns=["age", "salary"], strategy="drop-null"
        )

    def test_drop_rows_invalid_condition_raises(self, interpreter):
        """An unsupported condition should raise RuntimeError internally.
        run() catches it and prints; the return value will be None."""
        result = run_yml(interpreter, "DROP ROWS WHERE age IS ZERO")
        assert result is None  # run() swallows and returns None on error

    def test_drop_commands_case_insensitive(self, interpreter):
        run_yml(interpreter, "drop all null rows")
        interpreter.dsl_engine.drop_na.assert_called_once_with(strategy="drop-null-row")



# ---------------------------------------------------------------------------
# FILTER command
# ---------------------------------------------------------------------------

class TestFilterCommand:

    def test_filter_simple_expression(self, interpreter):
        run_yml(interpreter, "FILTER WHERE age > 30")
        interpreter.dsl_engine.filter.assert_called_once_with(expression="age > 30")

    def test_filter_complex_expression(self, interpreter):

        expr = "age > 20 AND salary < 80000"
        run_yml(interpreter, f"FILTER WHERE {expr}")
        interpreter.dsl_engine.filter.assert_called_once_with(expression=expr)


# ---------------------------------------------------------------------------
# NORMALISE / STANDARDIZE commands
# ---------------------------------------------------------------------------


class TestNormaliseCommand:

    @pytest.mark.parametrize("strategy", ["z-score", "min-max", "robust"])

    def test_standardize_strategies(self, interpreter, strategy):
        run_yml(interpreter, f"NORMALISE COLUMNS age, salary USING {strategy}")
        interpreter.dsl_engine.standardize.assert_called_once_with(
            ["age", "salary"], strategy

        )


    @pytest.mark.parametrize("strategy", ["lower", "upper", "strip"])
    def test_string_normalize_strategies(self, interpreter, strategy):
        run_yml(interpreter, f"NORMALISE COLUMNS name, city USING {strategy}")
        interpreter.dsl_engine.string_normalize.assert_called_once_with(
            ["name", "city"], strategy
        )

    def test_normalise_single_column(self, interpreter):
        run_yml(interpreter, "NORMALISE COLUMNS age USING z-score")
        interpreter.dsl_engine.standardize.assert_called_once_with(["age"], "z-score")

    def test_normalise_case_insensitive(self, interpreter):
        run_yml(interpreter, "normalise columns age USING Z-SCORE")
        interpreter.dsl_engine.standardize.assert_called_once_with(["age"], "z-score")


# ---------------------------------------------------------------------------

# TRANSFORM command
# ---------------------------------------------------------------------------

class TestTransformCommand:

    def test_transform_basic(self, interpreter):
        run_yml(interpreter, "TRANSFORM COLUMNS age USING log-transform")
        interpreter.dsl_engine.transform.assert_called_once_with(

            ["age"], "log-transform", False
        )


    def test_transform_inplace(self, interpreter):
        run_yml(interpreter, "TRANSFORM COLUMNS salary USING log-transform INPLACE")
        interpreter.dsl_engine.transform.assert_called_once_with(
            ["salary"], "log-transform", True
        )


    def test_transform_multiple_columns(self, interpreter):
        run_yml(interpreter, "TRANSFORM COLUMNS age, salary USING sqrt")
        interpreter.dsl_engine.transform.assert_called_once_with(
            ["age", "salary"], "sqrt", False
        )


    def test_transform_inplace_false_by_default(self, interpreter):
        run_yml(interpreter, "TRANSFORM COLUMNS age USING log-transform")
        _, _, inplace = interpreter.dsl_engine.transform.call_args[0]
        assert inplace is False


# ---------------------------------------------------------------------------
# FILL NULL (impute) commands
# ---------------------------------------------------------------------------

class TestImputeCommands:

    @pytest.mark.parametrize("strategy", ["forward", "backward", "mean", "median", "mode"])
    def test_fill_null_all_columns_known_strategy(self, interpreter, strategy):
        run_yml(interpreter, f"FILL NULL USING {strategy}")
        interpreter.dsl_engine.impute_na.assert_called_once_with(
            value=strategy, strategy=strategy
        )

    def test_fill_null_all_columns_literal_value(self, interpreter):
        run_yml(interpreter, "FILL NULL USING 0")
        interpreter.dsl_engine.impute_na.assert_called_once_with(
            value="0", strategy="default"
        )


    def test_fill_null_specific_column_with_strategy(self, interpreter):
        run_yml(interpreter, "FILL NULL IN COLUMN age USING mean")
        interpreter.dsl_engine.impute_na.assert_called_once_with(

            columns=["age"], value="mean", strategy="mean"
        )


    def test_fill_null_specific_columns_multiple(self, interpreter):
        run_yml(interpreter, "FILL NULL IN COLUMN age, salary USING median")
        interpreter.dsl_engine.impute_na.assert_called_once_with(
            columns=["age", "salary"], value="median", strategy="median"

        )

    def test_fill_null_specific_column_literal_value(self, interpreter):
        run_yml(interpreter, "FILL NULL IN COLUMN age USING 99")
        interpreter.dsl_engine.impute_na.assert_called_once_with(
            columns=["age"], value="99", strategy="default"
        )


# ---------------------------------------------------------------------------
# RENAME command
# ---------------------------------------------------------------------------

class TestRenameCommand:


    def test_rename_single_column(self, interpreter):
        run_yml(interpreter, "RENAME age TO years")
        interpreter.dsl_engine.rename.assert_called_once_with(age="years")

    def test_rename_multiple_columns(self, interpreter):
        run_yml(interpreter, "RENAME age, salary TO years, income")

        interpreter.dsl_engine.rename.assert_called_once_with(age="years", salary="income")



# ---------------------------------------------------------------------------
# Multi-command / pipeline tests
# ---------------------------------------------------------------------------


class TestMultiCommandPipeline:

    def test_multiple_commands_called_in_order(self, interpreter):
        run_yml(
            interpreter,
            "DROP ANY NULL ROWS",
            "NORMALISE COLUMNS age USING z-score",

            "RENAME age TO years",
        )
        assert interpreter.dsl_engine.drop_na.call_count == 1
        assert interpreter.dsl_engine.standardize.call_count == 1
        assert interpreter.dsl_engine.rename.call_count == 1

    def test_execute_called_once(self, interpreter):
        run_yml(interpreter, "DROP ANY NULL ROWS")
        interpreter.dsl_engine.execute.assert_called_once()



# ---------------------------------------------------------------------------
# Invalid / unknown command
# ---------------------------------------------------------------------------

class TestInvalidCommand:


    def test_unknown_command_returns_none(self, interpreter):
        result = run_yml(interpreter, "UNKNOWN COMMAND HERE")
        assert result is None

    def test_partial_match_not_accepted(self, interpreter):
        """A command that partially looks like a rule but doesn't fully match."""
        result = run_yml(interpreter, "DROP")
        assert result is None


# ---------------------------------------------------------------------------
# Integration tests (no mocking — uses real CleanerPipeline + operations)
# ---------------------------------------------------------------------------

class TestIntegration:
    """
    These tests require a working `operations` module.
    Skip them gracefully if dependencies are unavailable.
    """

    @pytest.fixture(autouse=True)
    def _import_guard(self):
        pytest.importorskip("polars")
        try:
            from .DSLInterpreter import DSLInterpreter  # noqa: F401
        except ImportError:
            pytest.skip("DSLInterpreter not importable")

    @pytest.fixture
    def real_interp(self):
        from .DSLInterpreter import DSLInterpreter
        df = make_df(

            age=[25, None, 35, 28],
            salary=[50_000, 60_000, None, 55_000],

            name=["Alice", "BOB", "charlie", "Dave"],
        )
        return DSLInterpreter(df)

    def test_drop_any_null_rows_integration(self, real_interp):
        result = run_yml(real_interp, "DROP ANY NULL ROWS")
        assert result is not None
        assert result.shape[0] == 2  # rows with any null dropped

    def test_rename_integration(self, real_interp):
        result = run_yml(real_interp, "RENAME age TO years")
        assert result is not None

        assert "years" in result.columns
        assert "age" not in result.columns

    def test_filter_integration(self, real_interp):
        result = run_yml(real_interp, "FILTER WHERE age > 26")
        assert result is not None
        # Only rows where age > 26 (and age is not null)
        valid = result.filter(pl.col("age").is_not_null())
        assert all(v > 26 for v in valid["age"].to_list())

    def test_pipeline_chain_integration(self, real_interp):
        result = run_yml(
            real_interp,
            "DROP ANY NULL ROWS",
            "RENAME age TO years",
        )
        assert result is not None
        assert "years" in result.columns
        assert result["years"].null_count() == 0
