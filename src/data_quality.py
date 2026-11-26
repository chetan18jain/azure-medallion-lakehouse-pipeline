"""Lightweight data-quality expectations for DataFrames.

Two ways to run checks:

* The single-check helpers (``check_min_rows`` etc.) each run their own
  Spark action — fine for a one-off check, wasteful as a gate.
* ``run_checks(df, spec)`` — the gate to use in jobs: it computes every
  requested metric in ONE aggregation and one ``collect()``, then decides
  pass/fail locally in Python. A four-check gate used to scan the
  DataFrame ~7 times (one filtered count per null column, two counts for
  the duplicate check, one per values-in column, one for the row count);
  now it scans it once.

Each check returns a result dict; ``assert_all()`` prints every result
and raises DataQualityError listing ALL failures, so the calling job
fails fast with the full picture instead of one breach at a time.
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

def run_checks(df: DataFrame, spec: dict) -> list:
    """Run every check in ``spec`` in a single Spark action.

    ``spec`` keys (all optional):
        "min_rows": int — minimum row count
        "no_nulls": list[str] — columns that must have no nulls
        "no_duplicates": list[str] — key columns that must be unique
        "values_in": dict[str, list] — column -> allowed values

    Returns a list of result dicts in the same shape as the single-check
    helpers, in spec order: min_rows, no_nulls, no_duplicates, then one
    values_in result per column. One aggregation computes the row count,
    one null count per no_nulls column, the distinct key count, and the
    distinct unexpected-value count per values_in column; a single
    ``collect()`` brings the numbers back and the verdicts are decided
    locally, so no check can trigger a second scan.
    """
    agg_exprs = [F.count("*").alias("__rows")]
    null_cols = spec.get("no_nulls") or []
    for i, col in enumerate(null_cols):
        agg_exprs.append(
            F.sum(F.col(col).isNull().cast("int")).alias(f"__null_{i}"))
    dup_keys = spec.get("no_duplicates") or []
    if dup_keys:
        # struct() keeps composite keys together; a struct is only null
        # when every field is null, so null keys still count as a key.
        agg_exprs.append(
            F.countDistinct(F.struct(*dup_keys)).alias("__distinct_keys"))
    values_in = spec.get("values_in") or {}
    for i, col in enumerate(values_in):
        allowed = values_in[col]
        agg_exprs.append(
            F.countDistinct(
                F.when(~F.col(col).isin(allowed), F.col(col))
            ).alias(f"__bad_{i}"))

    row = df.agg(*agg_exprs).collect()[0]

    results = []
    minimum = spec.get("min_rows")
    if minimum is not None:
        results.append({"check": "min_rows",
                        "passed": row["__rows"] >= minimum,
                        "detail": f"rows={row['__rows']}, minimum={minimum}"})
    if null_cols:
        bad = {c: row[f"__null_{i}"] for i, c in enumerate(null_cols)
               if row[f"__null_{i}"]}
        results.append({"check": "no_nulls", "passed": not bad,
                        "detail": f"null counts: {bad or 'none'}"})
    if dup_keys:
        results.append({"check": "no_duplicates",
                        "passed": row["__rows"] == row["__distinct_keys"],
                        "detail": f"rows={row['__rows']}, "
                                  f"distinct keys={row['__distinct_keys']}"})
    for i, col in enumerate(values_in):
        bad = row[f"__bad_{i}"]
        results.append({"check": f"values_in:{col}", "passed": bad == 0,
                        "detail": f"unexpected distinct values={bad}"})
    return results

def assert_all(results: list) -> None:
    for result in results:
        status = "PASS" if result["passed"] else "FAIL"
        print(f"[DQ] {status} {result['check']} :: {result['detail']}")
    failed = [r for r in results if not r["passed"]]
    if failed:
        names = ", ".join(r["check"] for r in failed)
        raise DataQualityError(f"Data quality checks failed: {names}")
