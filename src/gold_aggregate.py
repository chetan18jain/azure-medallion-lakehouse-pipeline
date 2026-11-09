"""Gold: business-ready aggregates for reporting.

Outputs:
  gold/daily_sales        revenue and order counts per day/category
  gold/customer_summary   per-customer lifetime orders and spend

Only COMPLETED orders count toward revenue. Tables are partitioned
by event_date (daily_sales) and written with a bounded file count;
see src/maintenance.py for OPTIMIZE / Z-ORDER upkeep.
"""
import argparse, sys, os
from pyspark.sql import functions as F
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bronze_ingest import get_spark
from data_quality import assert_all, check_min_rows

def daily_sales(orders, products):
    completed = orders.filter(F.col("status") == "COMPLETED")
    return (completed.join(products, "product_id", "left")
            .groupBy("event_date", "category")
            .agg(F.countDistinct("order_id").alias("orders"),
                 F.sum("quantity").alias("units_sold"),
                 F.sum("amount").alias("revenue"))
            .orderBy("event_date", "category"))

def customer_summary(orders, customers):
    completed = orders.filter(F.col("status") == "COMPLETED")
    return (completed.groupBy("customer_id")
            .agg(F.countDistinct("order_id").alias("total_orders"),
                 F.sum("amount").alias("lifetime_spend"),
                 F.max("event_date").alias("last_order_date"))
            .join(customers.select("customer_id", "customer_name", "city"),
                  "customer_id", "left"))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver", default="data/silver")
    parser.add_argument("--gold", default="data/gold")
    args = parser.parse_args()
    spark = get_spark("gold-aggregate")

    orders = spark.read.format("delta").load(f"{args.silver}/orders")
    customers = spark.read.format("delta").load(f"{args.silver}/customers")
    products = spark.read.format("delta").load(f"{args.silver}/products")

    sales = daily_sales(orders, products)
    assert_all([check_min_rows(sales, 1)])
    # Coalesce before write: small daily partitions otherwise produce
    # thousands of tiny files that slow every downstream scan.
    (sales.coalesce(1).write.format("delta").mode("overwrite")
     .partitionBy("event_date").save(f"{args.gold}/daily_sales"))

    summary = customer_summary(orders, customers)
    (summary.write.format("delta").mode("overwrite")
     .save(f"{args.gold}/customer_summary"))
    print("Gold tables written.")
    spark.stop()

if __name__ == "__main__":
    main()
