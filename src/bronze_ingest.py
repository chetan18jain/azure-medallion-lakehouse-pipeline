"""Bronze: land raw CSV extracts into Delta, unchanged except metadata.

Bronze keeps source fidelity: no business rules, just typed storage,
an ingestion timestamp and partitioning by event_date for orders.
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def get_spark(app_name: str) -> SparkSession:
    return (SparkSession.builder.appName(app_name)
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())

def ingest_csv(spark, source_path: str, target_path: str,
               partition_col: str = None) -> None:
    df = (spark.read.option("header", "true")
          .option("inferSchema", "true").csv(source_path))
    df = df.withColumn("ingested_at", F.current_timestamp())
    writer = df.write.format("delta").mode("overwrite")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.save(target_path)
    print(f"Bronze wrote {df.count()} rows -> {target_path}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", default="data/raw")
    parser.add_argument("--bronze", default="data/bronze")
    args = parser.parse_args()
    spark = get_spark("bronze-ingest")
    ingest_csv(spark, f"{args.raw}/orders.csv",
               f"{args.bronze}/orders", partition_col="event_date")
    ingest_csv(spark, f"{args.raw}/customers.csv", f"{args.bronze}/customers")
    ingest_csv(spark, f"{args.raw}/products.csv", f"{args.bronze}/products")
    spark.stop()

if __name__ == "__main__":
    main()
