"""Delta table maintenance: OPTIMIZE compaction + Z-ORDER clustering.

Run periodically (or from an ADF maintenance activity) to compact
small files and co-locate rows that are filtered together, e.g.
Z-ORDER BY (customer_id) makes per-customer lookups skip most files.

Usage: python src/maintenance.py --gold data/gold
"""
import argparse, sys, os
from delta.tables import DeltaTable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bronze_ingest import get_spark

def optimise(spark, path: str, zorder_cols: list = None) -> None:
    table = DeltaTable.forPath(spark, path)
    if zorder_cols:
        table.optimize().executeZOrderBy(zorder_cols)
    else:
        table.optimize().executeCompaction()
    print(f"OPTIMIZE done: {path} zorder={zorder_cols}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gold", default="data/gold")
    args = parser.parse_args()
    spark = get_spark("delta-maintenance")
    optimise(spark, f"{args.gold}/daily_sales")
    optimise(spark, f"{args.gold}/customer_summary", ["customer_id"])
    spark.stop()

if __name__ == "__main__":
    main()
