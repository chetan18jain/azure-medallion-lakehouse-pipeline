"""Lightweight data-quality expectations for DataFrames.

Each check returns a result dict; assert_all() raises DataQualityError
when any check breaches, so the calling job fails fast.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class DataQualityError(Exception):
    pass

def check_min_rows(df: DataFrame, minimum: int) -> dict:
    count = df.count()
    return {"check": "min_rows", "passed": count >= minimum,
            "detail": f"rows={count}, minimum={minimum}"}

def check_no_nulls(df: DataFrame, columns: list) -> dict:
    bad = {}
    for col in columns:
        n = df.filter(F.col(col).isNull()).count()
        if n:
            bad[col] = n
    return {"check": "no_nulls", "passed": not bad,
            "detail": f"null counts: {bad or 'none'}"}

def check_no_duplicates(df: DataFrame, keys: list) -> dict:
    total = df.count()
    distinct = df.dropDuplicates(keys).count()
    return {"check": "no_duplicates", "passed": total == distinct,
            "detail": f"rows={total}, distinct keys={distinct}"}

def check_values_in(df: DataFrame, column: str, allowed: list) -> dict:
    bad = (df.filter(~F.col(column).isin(allowed))
             .select(column).distinct().count())
    return {"check": f"values_in:{column}", "passed": bad == 0,
            "detail": f"unexpected distinct values={bad}"}

def assert_all(results: list) -> None:
    for result in results:
        status = "PASS" if result["passed"] else "FAIL"
        print(f"[DQ] {status} {result['check']} :: {result['detail']}")
    failed = [r for r in results if not r["passed"]]
    if failed:
        names = ", ".join(r["check"] for r in failed)
        raise DataQualityError(f"Data quality checks failed: {names}")
