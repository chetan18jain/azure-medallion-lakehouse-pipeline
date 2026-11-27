# Learning notes — Azure Medallion Lakehouse Pipeline

What this project taught me, file by file, in my own words. These notes
are the thinking behind the code — the design decisions, the traps, and
the one-line answers I want to be able to give in an interview.

## The one-sentence architecture

Data gets better in stages, and each stage has ONE job: Bronze lands it
exactly as the source sent it, Silver is the only place cleansing lives,
Gold answers business questions. If Silver or Gold breaks, I can rebuild
both from Bronze alone, because Bronze never throws anything away.

## Medallion: why the layers exist

| Layer | Job | Rule |
|---|---|---|
| Bronze | Land data exactly as source sent it | NO business rules. Only add `ingested_at` |
| Silver | Clean it — the ONLY place cleansing lives | Dedupe, fix types, trim, validate status |
| Gold | Answer business questions | Aggregates only. Only COMPLETED orders = revenue |

Why not clean in Bronze? If you clean at landing and your rule was
wrong, the original is gone. Bronze = insurance. That is also why the
demo uses `mode("overwrite")` in Bronze but production would use append
or Delta MERGE — overwrite every run is a demo convenience, not a
pattern to carry into prod.

## The tech pieces and what each one actually does

- **PySpark** — processes data in parallel across partitions.
  `SparkSession` is the entry point to everything.
- **Delta Lake** — storage format on top of Parquet. Gives you ACID
  writes, time travel, `MERGE`, `OPTIMIZE`, Z-ORDER. That's why every
  write is `.format("delta")`.
- **ADLS Gen2** — in production, folders are
  `abfss://bronze@<account>.dfs.core.windows.net/...`. Locally, plain
  folders stand in. **The job code does not change** — only the path
  changes. That's the design trick of this project.
- **ADF (Azure Data Factory)** — does NOT transform data. It only
  orchestrates: run Bronze, if it succeeds run Silver, etc.
  (`adf/pipeline.json`)

Every Spark job needs Delta configured — see `get_spark()` in
`bronze_ingest.py`. That function is defined ONCE there and imported by
silver/gold/maintenance. Reuse, don't copy-paste.

## The generator: a stand-in, not a source

`src/generate_sample_data.py` is plain Python stdlib — no Spark.
Generating 156 rows doesn't need a cluster; Spark enters at Bronze.

- `random.Random(42)` — a dedicated instance, not the global module,
  so nothing else can disturb the sequence. Same seed + same args =
  identical data every run: repeatable demos, tests, debugging.
- 30 customers, 6 products, 120 orders (Jan 2026 dates).
- Weighted status trick: `choice(["COMPLETED", "COMPLETED",
  "COMPLETED", "CANCELLED"])` = ~75% completed / ~25% cancelled.
  Deliberate: if everything were COMPLETED, Gold's COMPLETED-only rule
  would filter nothing and prove nothing.
- In real life this file doesn't exist — CSVs land in ADLS from source
  systems. This is only a local stand-in.

## Bronze: raw fidelity, plus one timestamp

`src/bronze_ingest.py` — learn this pattern, it repeats everywhere.

```python
df = spark.read.option("header","true").option("inferSchema","true").csv(source_path)
df = df.withColumn("ingested_at", F.current_timestamp())
df.write.format("delta").mode("overwrite").partitionBy("event_date").save(target_path)
```

1. `header + inferSchema` — CSV has no types, Spark guesses them. OK
   for Bronze; production pins an explicit `StructType`.
2. `ingested_at` is the ONLY column added. No filtering, no dedupe.
3. `partitionBy("event_date")` for orders only — because reporting
   always filters by date, so Spark can skip whole folders (partition
   pruning). Customers/products are small — no partition.
4. Source and partition-column checks run up front: the CSV must exist
   and the partition column must be in `df.columns`, so a bad path
   fails early with a clear error instead of halfway through a write.
5. `try/finally` in `main`: `spark.stop()` runs even if an ingest
   fails.

### Don't count what you just wrote

The success print used to call `df.count()` after the write — a Spark
action that re-runs the whole DataFrame computation just to log a
number. The row count now comes from the Delta transaction log instead:
`DeltaTable.forPath(spark, target_path).history(1)` gives
`numOutputRows` from the just-committed transaction — a metadata-only
read, no re-scan. The general lesson: the log already knows what the
write did; don't recompute it.

## Data quality: collect-then-assert

`src/data_quality.py` is a tiny test-set for a DataFrame. Every check
returns the SAME shape — `{check, passed, detail}` — and none of them
raises:

- `check_min_rows(df, min)` — the table isn't empty
- `check_no_nulls(df, [cols])` — key columns have no nulls
- `check_no_duplicates(df, [keys])` — `count ==
  dropDuplicates(keys).count`
- `check_values_in(df, col, allowed)` — e.g. status ∈ {COMPLETED,
  CANCELLED, REFUNDED}

Then ONE function decides:

```python
assert_all(results)  # prints PASS/FAIL for each, raises DataQualityError if any FAIL
```

**Pattern name:** collect-then-assert. You see ALL failures in one run,
not one-at-a-time. That matters operationally: a one-failure-per-run
gate turns a three-problem dataset into three pipeline runs.

### One aggregation, not one action per check

The original `check_no_nulls` ran one `.count()` per column, and the
original Silver usage (min_rows + 3 null columns + duplicates + values)
was 7 Spark actions on the orders DataFrame before writing. Every
`.count()` is a full scan/job. The checks are now combined in
`run_checks`: one aggregation computes the row count, the null count
per column, the distinct-key count and the out-of-range count, and one
`collect()` returns all of it. The gate scans the DataFrame once
instead of once per check. Correctness first, optimisation second — but
at production scale, uncached repeated actions are the first thing
that shows up in the bill.

Two traps the checks close:

- **NULL slips past `check_values_in`.** Spark's 3-valued logic means
  NULL is neither "in" nor "not in" the allowed list, so a null status
  used to pass the values check. The condition is now explicitly
  `(col IS NULL) OR (col NOT IN allowed)`.
- **Empty inputs must not silently pass.** A negative minimum fails,
  an empty keys list for duplicates fails (`dropDuplicates([])` would
  silently pass), an empty columns list passes cleanly.

## Silver: the only place cleansing lives

`src/silver_transform.py` — where ALL cleansing lives.

Orders:
```python
.dropDuplicates(["order_id"])                      # dedupe on natural key
.withColumn("event_date", F.to_date("event_date")) # string → real date
.withColumn("amount", F.col("amount").cast("decimal(12,2)"))  # money type
.filter(F.col("quantity") > 0)                     # nonsense rows out
```
Customers: dedupe on `customer_id`, `trim()` names,
`initcap(trim(city))` → " hyderabad " becomes "Hyderabad".
Products: dedupe, cast `list_price` to decimal.

Money = decimal, never float (rounding). Dates = real `DateType` for
filtering/partitioning.

**Order matters:** clean FIRST, then run the quality checks, then
write. If a check fails, the job dies and NOTHING is written to
Silver. Gold never sees bad data.

### Fail-fast, not filter — a deliberate choice

The docstring once said "keep only valid statuses", but the code does
NOT filter statuses: `check_values_in` FAILS THE JOB if an invalid
status appears. That is a fail-fast gate, not a filter. The two
philosophies are different:

- **Fail (this project):** bad data is loud, the pipeline stops,
  someone investigates. Nothing is silently lost.
- **Filter:** bad rows are silently dropped, the pipeline stays green,
  and revenue could quietly shrink. And if you filter in cleaning, the
  later `check_values_in` can never fail — it becomes dead code.

Filtering is defensible only with a quarantine/dead-letter table
counting what was dropped. For an interview: "I chose fail-fast in
Silver so Gold never sees bad data and nothing is silently dropped."

Two honest mismatches found while reviewing the file, worth knowing:

- There are NO joins in Silver — tables are cleaned independently.
  Joins happen in Gold.
- `dropDuplicates` on a key keeps an ARBITRARY row if two rows share
  the key but differ elsewhere. Production needs a winner rule (latest
  timestamp, etc.) — see Project 4's loader, which dedupes by key
  keeping the latest row per key.

## Gold: join order is the design lesson

`src/gold_aggregate.py` — business answers, no cleansing here.

Business rule, applied in BOTH functions first: **only `status ==
"COMPLETED"` counts as revenue.**

- Table 1 — `daily_sales`: completed orders JOIN products on
  `product_id`, group by `event_date + category`, aggregate
  `countDistinct(order_id)`, `sum(quantity)`, `sum(amount)`.
- Table 2 — `customer_summary`: completed orders group by
  `customer_id` → total_orders, lifetime_spend, last_order_date, then
  JOIN customers for name/city.

Join order is the key lesson:

- `daily_sales`: JOIN products BEFORE groupBy — because category (the
  grouping key) only exists in products.
- `customer_summary`: groupBy FIRST, JOIN customers AFTER — name/city
  are labels needed once per customer, not per order. Aggregating first
  joins ~30 rows instead of ~120.

Rule: join before grouping only when the joined column is a grouping
key or is aggregated. Otherwise aggregate first, join small.

### LEFT joins and what countDistinct does NOT protect

- Both joins are LEFT: a missing lookup must not delete a completed
  order's revenue. An unmatched `product_id` → category = NULL →
  groupBy creates a real NULL-category group per date. Revenue is
  preserved and visible — a signal to fix the product mapping in
  Silver, not silent loss.
- `countDistinct(order_id)` counts unique orders even if a join
  duplicated a row. It does NOT protect `sum(amount)` /
  `sum(quantity)`: if products had duplicate `product_id` rows, the
  join multiplies order rows and revenue is inflated while the order
  count still looks right. Defence: `product_id` must be unique in
  Silver (Silver's dedupe + DQ `no_duplicates` check is what protects
  Gold). Layers protect each other.

### coalesce(1)

Before the partitioned `daily_sales` write: collapses output to 1
partition → ~1 file per date partition instead of many tiny files.
Downside: ALL data funnels through 1 task — no parallelism, bottleneck
+ memory pressure at scale. Portfolio scale: fine. Production:
size-based partition count, and rely on OPTIMIZE (maintenance) for
compaction.

## Maintenance: physical layout, not data

`src/maintenance.py` — no data changes. This file only makes existing
Gold Delta tables cheaper/faster to read. It runs separately and
periodically (or as an ADF activity), NOT inside the daily transform —
OPTIMIZE is expensive, and the daily job should stay fast and
predictable.

### The small-files problem

Each Spark write leaves many small Parquet files. Every scan pays
per-file overhead: open file, read metadata/footer, schedule a task.
Thousands of tiny files = mostly overhead, little data. Same data,
slower queries.

### OPTIMIZE (compaction)

Reads existing files, rewrites the SAME rows into fewer, larger files
(~1GB target in Delta). Old files are removed from the current
snapshot in the transaction log but NOT physically deleted yet — that's
why time travel still works after OPTIMIZE. Data and columns
unchanged; only physical layout changes.

### Z-ORDER

A space-filling curve (multi-dimensional clustering) applied during
the rewrite: rows with similar values of the chosen column(s) land in
the same files. Delta keeps per-file min/max stats in the transaction
log. A filter like `customer_id = 101` lets the engine check stats and
SKIP files that cannot contain the value — data skipping. It improves
the probability of skipping; it does not guarantee only one file is
read.

### Why the split

- `daily_sales`: plain compaction. Already partitioned by `event_date`,
  and date-range queries prune whole partitions — Z-ORDER on top adds
  little.
- `customer_summary`: Z-ORDER BY (`customer_id`). Not partitioned (one
  row per customer), and the natural query is "give me this customer"
  — clustering by `customer_id` is exactly what makes that lookup skip
  most files.

Rule: partition on the low-cardinality column you always filter by
(date); Z-ORDER on high-cardinality columns you filter by but can't
sensibly partition on (`customer_id`).

Honest gaps: no VACUUM — obsolete files are never physically deleted
here. VACUUM is deliberately a separate, careful step; its default
retention is 7 days, and a shorter retention can delete files still
needed by time travel or in-flight readers. And no existence check:
`DeltaTable.forPath` raises if the path is missing, so a bad path
fails the job loudly but with no friendly message. Also, if
`daily_sales` fails, `customer_summary` never runs (no per-table
isolation) — production would wrap each table separately.

## Orchestration in ADF

`adf/pipeline.json` — pipeline `pl_medallion_lakehouse_daily`.

Chain (each step runs ONLY if previous = Succeeded):

```
BronzeIngest → SilverTransform → GoldAggregate → DeltaMaintenance
```

- Each activity is type `DatabricksSparkPython` — ADF tells Databricks
  to run that .py file.
- Paths are pipeline **parameters** (`rawFolder`, `bronzeFolder`...) —
  locally they are `data/...`, in prod they become `abfss://...`
  paths. Code unchanged.
- Parameter → script mapping: `@pipeline().parameters.rawFolder` is
  passed as the script's `--raw` argparse value. ADF supplies the
  values; argparse is how each script receives them. That contract is
  why every script takes folder flags instead of hardcoding paths.

**Failure behaviour:** Silver DQ fails → Gold never starts → bad data
never reaches reporting. By design.

What ADF adds beyond the scripts: scheduling/triggers, run
monitoring, per-activity retry, failure alerting, managed ordering.
The scripts alone know nothing about when or in what order to run.

Honest gap: this is a simplified export. A real ADF pipeline also
carries `linkedServiceName` (the Databricks linked service) on each
activity, and triggers are defined separately — there is no trigger in
this file, so as-is nothing schedules it.

## Design decisions to quote in interviews

1. **Partition by `event_date`, NOT by status/category** — date is the
   dominant filter + high-enough cardinality. Low-cardinality columns
   make bad partitions.
2. **Bronze is rebuild-everything insurance** — append-only in prod,
   source fidelity always.
3. **DQ gates in Silver, before write** — fail fast, fail before Gold.
4. **Incremental in prod = Delta MERGE + watermark** on natural key /
   `event_date`. Demo uses overwrite only for clarity.
5. **OPTIMIZE + Z-ORDER** is not optional at scale — small files kill
   scan performance.
6. **Collect-then-assert** — see every failure in one run, not
   one-at-a-time.
7. **Fail-fast in Silver, not silent filtering** — bad data stops the
   pipeline; nothing is quietly dropped.
8. **The Delta log already knows what the write did** — read row counts
   from `history()`, don't re-scan with `count()`.

## Interview lines

- "If Silver or Gold breaks, I can rebuild both from Bronze alone,
  because Bronze never throws anything away."
- "Cleansing lives in exactly one place — Silver — and the quality
  gates run before anything is written, so Gold never sees bad data."
- "Every `.count()` is a Spark action. The gate combines all checks
  into one aggregation, and the write count comes from the Delta
  transaction log, not a re-scan."
- "Join before grouping only when the joined column is a grouping key;
  otherwise aggregate first and join small."
- "Partition on the date you always filter by; Z-ORDER on the
  high-cardinality column you look up by."
