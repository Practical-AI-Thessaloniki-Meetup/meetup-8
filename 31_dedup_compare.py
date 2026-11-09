# Databricks notebook source
# MAGIC %md
# MAGIC # 31 Dedup compare (stage N, task N3): Message-ID against the subject + body key
# MAGIC
# MAGIC A **report**. It reads `silver_messages` and `silver_dedup`, puts every definition of "the same message" on the **same base**
# MAGIC (the parsed rows of the snapshot), and writes one small table, `silver_dedup_compare`, one row per metric.
# MAGIC No existing table is changed and no row is deleted or merged. The Message-ID rate and the dup_key rate are reported side by side, never mixed.
# MAGIC
# MAGIC - **Message-ID** (`mid_v1`): trim, then remove one pair of surrounding angle brackets. No lowercasing. A NULL or empty result belongs to no Message-ID group.
# MAGIC - **A** Message-ID view | **B** subject + body view (from `silver_dedup`) | **C** agreement between the two on groups of 2+ rows
# MAGIC - **D** row-level difference | **E** sender as a key (the rejected option) | **F** body-only key: skipped, see the note in that cell.
# MAGIC
# MAGIC **Check 3b:** the dup_key extra copies recomputed here equal the duplicate rows in `silver_dedup`. Every group and bucket count must sum to its parent.
# MAGIC Any failure raises, so the Job fails and nothing is written. This notebook prints counts only.
# MAGIC
# MAGIC **Parameter:** `snapshot_id` (empty means the newest snapshot in Bronze). A re-run replaces that snapshot's rows only.

# COMMAND ----------

# MAGIC %run ./00_setup_and_config

# COMMAND ----------

# MAGIC %run ./01_normalise

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


def get_param(name, default):
    """Widget value when running as a notebook or Job task, else the default."""
    try:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name).strip()
    except NameError:
        return default


# Same default as 30_dedup: the config holds settings, not run state, so empty means newest in Bronze.
LATEST_SNAPSHOT_ID = spark.sql(f"SELECT MAX(snapshot_id) FROM {BRONZE_TABLE}").first()[0]
SNAPSHOT_ID = get_param("snapshot_id", "") or LATEST_SNAPSHOT_ID
assert SNAPSHOT_ID, f"No rows in {BRONZE_TABLE}"

GRAIN = ["snapshot_id", "source_file", "msg_index"]
MID_VERSION = "mid_v1"   # bump when the Message-ID normalisation changes

parsed = spark.table(SILVER_TABLE).where(F.col("snapshot_id") == SNAPSHOT_ID)
dedup = spark.table(DEDUP_TABLE).where(F.col("snapshot_id") == SNAPSHOT_ID)
parsed_rows = parsed.count()
# Without this, every check below could pass on 0 = 0.
if parsed_rows == 0:
    raise AssertionError(f"No rows in {SILVER_TABLE} for snapshot {SNAPSHOT_ID}. Run 20_silver first.")
if dedup.count() == 0:
    raise AssertionError(f"No rows in {DEDUP_TABLE} for snapshot {SNAPSHOT_ID}. Run 30_dedup first.")

# ONE base for every metric: parsed rows joined to their dedup flags. An inner join that loses rows must fail loudly.
base = (parsed
        .select(*GRAIN, "list_name", "month", "date_utc", "message_id", "sender_claimed_value")
        .join(dedup.select(*GRAIN, "dup_key", "short_body", "is_duplicate", "normalisation_version"), GRAIN, "inner")
        .withColumn("mid", F.regexp_replace(F.trim("message_id"), "^<(.*)>$", "$1"))   # one pair of brackets only
        .withColumn("mid", F.when(F.col("mid") == "", None).otherwise(F.col("mid"))))
base.cache()   # every metric below reads the same rows
BASE_ROWS = base.count()
if BASE_ROWS != parsed_rows:
    raise AssertionError(f"Join lost rows: silver_messages {parsed_rows}, joined base {BASE_ROWS}")

# Take the version from the data, so this report can never label a norm_v2 run as norm_v1.
versions = [r[0] for r in base.select("normalisation_version").distinct().collect()]
if len(versions) != 1:
    raise AssertionError(f"silver_dedup has {len(versions)} normalisation versions for this snapshot")
NORM_VERSION = versions[0]

print("Snapshot:", SNAPSHOT_ID, "| newest in Bronze:", LATEST_SNAPSHOT_ID)
print("Config SHA-256:", CONFIG_SHA256)
print("norm_version:", NORM_VERSION, "| mid_version:", MID_VERSION)
print("Base rows (parsed):", BASE_ROWS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## A and B: the two views

# COMMAND ----------

metrics = {}   # metric name -> value, in the order it is printed and stored

# A. Message-ID view
a = base.agg(
    F.count(F.when(F.col("mid").isNotNull(), 1)).alias("with_id"),
    F.countDistinct("mid").alias("distinct_ids")).first()
rows_with_id, distinct_ids = a["with_id"], a["distinct_ids"]
rows_without_id = BASE_ROWS - rows_with_id
mid_groups = (base.where(F.col("mid").isNotNull()).groupBy("mid").agg(F.count("*").alias("n"))
              .where("n > 1").count())

metrics["A rows with a usable Message-ID"] = rows_with_id
metrics["A rows without a usable Message-ID"] = rows_without_id
metrics["A distinct Message-IDs"] = distinct_ids
metrics["A Message-ID groups with more than one row"] = mid_groups
metrics["A extra copies by Message-ID"] = rows_with_id - distinct_ids
metrics["A unique messages by Message-ID"] = distinct_ids + rows_without_id

# B. Subject + body view, straight from silver_dedup
dup_rows = base.where("is_duplicate").count()
metrics["B extra copies by dup_key (silver_dedup)"] = dup_rows
metrics["B unique messages by dup_key (silver_dedup)"] = BASE_ROWS - dup_rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## C: agreement on groups of 2+ rows
# MAGIC
# MAGIC Short-body rows (no dup_key) and rows without a Message-ID each get their own bucket, so they never inflate agree or differ.

# COMMAND ----------

# C1. Message-ID groups, looked at through the dup_key
mid_g = (base.where(F.col("mid").isNotNull()).groupBy("mid")
         .agg(F.count("*").alias("n"),
              F.count(F.when(F.col("dup_key").isNull(), 1)).alias("n_short"),
              F.countDistinct("dup_key").alias("n_keys"))
         .where("n > 1"))
c1 = mid_g.agg(
    F.count("*").alias("groups"),
    F.count(F.when(F.col("n_short") > 0, 1)).alias("with_short"),
    F.count(F.when((F.col("n_short") == 0) & (F.col("n_keys") == 1), 1)).alias("agree"),
    F.count(F.when((F.col("n_short") == 0) & (F.col("n_keys") > 1), 1)).alias("differ")).first()

metrics["C Message-ID groups, 2+ rows (total)"] = c1["groups"]
metrics["C Message-ID groups: all rows share one dup_key (agree)"] = c1["agree"]
metrics["C Message-ID groups: more than one dup_key (same ID, different content)"] = c1["differ"]
metrics["C Message-ID groups: contain a short_body row (separate)"] = c1["with_short"]

# C2. dup_key groups, looked at through the Message-ID
key_g = (base.where(F.col("dup_key").isNotNull()).groupBy("dup_key")
         .agg(F.count("*").alias("n"),
              F.count(F.when(F.col("mid").isNull(), 1)).alias("n_no_id"),
              F.countDistinct("mid").alias("n_ids"))
         .where("n > 1"))
c2 = key_g.agg(
    F.count("*").alias("groups"),
    F.count(F.when(F.col("n_no_id") > 0, 1)).alias("with_no_id"),
    F.count(F.when((F.col("n_no_id") == 0) & (F.col("n_ids") == 1), 1)).alias("agree"),
    F.count(F.when((F.col("n_no_id") == 0) & (F.col("n_ids") > 1), 1)).alias("differ")).first()

metrics["C dup_key groups, 2+ rows (total)"] = c2["groups"]
metrics["C dup_key groups: all rows share one Message-ID (agree)"] = c2["agree"]
metrics["C dup_key groups: more than one Message-ID (same content, different IDs)"] = c2["differ"]
metrics["C dup_key groups: contain a row without a Message-ID (separate)"] = c2["with_no_id"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## D: row-level difference
# MAGIC
# MAGIC For Message-ID the first row is primary, with the same ordering as `silver_dedup`: parsed date (NULLs last), list, month, `msg_index`.

# COMMAND ----------

by_mid = Window.partitionBy("mid").orderBy(F.col("date_utc").asc_nulls_last(), "list_name", "month", "msg_index")
flagged = base.withColumn(
    "mid_duplicate", F.when(F.col("mid").isNotNull(), F.row_number().over(by_mid) > 1).otherwise(False))

d = flagged.agg(
    F.count(F.when(F.col("is_duplicate") & F.col("mid_duplicate"), 1)).alias("both"),
    F.count(F.when(F.col("is_duplicate") & ~F.col("mid_duplicate"), 1)).alias("key_only"),
    F.count(F.when(~F.col("is_duplicate") & F.col("mid_duplicate"), 1)).alias("mid_only"),
    F.count(F.when(~F.col("is_duplicate") & ~F.col("mid_duplicate"), 1)).alias("neither"),
    F.count(F.when(F.col("mid_duplicate"), 1)).alias("mid_dups")).first()

metrics["D rows duplicate by both"] = d["both"]
metrics["D rows duplicate by dup_key but not by Message-ID"] = d["key_only"]
metrics["D rows duplicate by Message-ID but not by dup_key"] = d["mid_only"]
metrics["D rows duplicate by neither"] = d["neither"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## E: sender as a key (rejected, shown for the slide)
# MAGIC
# MAGIC A NULL sender counts as one value. The sender is claimed, not verified. No sender value is printed.

# COMMAND ----------

distinct_senders = base.select(F.coalesce("sender_claimed_value", F.lit("")).alias("s")).distinct().count()
sender_removed = BASE_ROWS - distinct_senders
metrics["E distinct sender_claimed_value"] = distinct_senders
metrics["E rows removed if sender were the key"] = sender_removed
metrics["E percent of base removed if sender were the key"] = round(100 * sender_removed / BASE_ROWS, 2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## F: body-only key, skipped
# MAGIC
# MAGIC `normalise()` now lives in `01_normalise.py`, shared with `30_dedup.py`, so F could be computed without writing anything.
# MAGIC It is still not computed: F was not part of the plan, so it needs a decision first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks (before anything is written)

# COMMAND ----------

failures = []

# Check 3b: recompute the dup_key extra copies from the keys alone, not from the is_duplicate flag.
k = base.where(F.col("dup_key").isNotNull()).agg(
    F.count("*").alias("rows"), F.countDistinct("dup_key").alias("keys")).first()
recomputed_extra = k["rows"] - k["keys"]
check3b_ok = recomputed_extra == dup_rows
if not check3b_ok:
    failures.append(f"CHECK 3b: dup_key extra copies recomputed {recomputed_extra}, duplicate rows in silver_dedup {dup_rows}")

sum_checks = [
    ("A with + without a Message-ID = base", rows_with_id + rows_without_id, BASE_ROWS),
    ("A unique + extra by Message-ID = base", metrics["A unique messages by Message-ID"] + metrics["A extra copies by Message-ID"], BASE_ROWS),
    ("B unique + extra by dup_key = base", metrics["B unique messages by dup_key (silver_dedup)"] + dup_rows, BASE_ROWS),
    ("C Message-ID buckets = groups", c1["agree"] + c1["differ"] + c1["with_short"], c1["groups"]),
    ("C Message-ID groups = A groups", c1["groups"], mid_groups),
    ("C dup_key buckets = groups", c2["agree"] + c2["differ"] + c2["with_no_id"], c2["groups"]),
    ("D four buckets = base", d["both"] + d["key_only"] + d["mid_only"] + d["neither"], BASE_ROWS),
    ("D Message-ID duplicates = A extra copies", d["mid_dups"], metrics["A extra copies by Message-ID"]),
]
for name, got, parent in sum_checks:
    if got != parent:
        failures.append(f"SUM CHECK '{name}': {got} != {parent}")

if failures:
    raise AssertionError("; ".join(failures))
print(f"CHECK 3b PASSED: dup_key extra copies recomputed {recomputed_extra} = duplicate rows in silver_dedup {dup_rows}")
print(f"SUM CHECKS PASSED: {len(sum_checks)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write
# MAGIC
# MAGIC `replaceWhere` replaces only this snapshot's rows, so a re-run never duplicates them.

# COMMAND ----------

SCHEMA_OUT = StructType([
    StructField("metric", StringType()),
    StructField("value", DoubleType()),   # counts and one percentage in one column
    StructField("base_rows", StringType()),
    StructField("snapshot_id", StringType()),
    StructField("config_hash", StringType()),
    StructField("norm_version", StringType()),
    StructField("mid_version", StringType()),
])
out = spark.createDataFrame(
    [(name, float(value), str(BASE_ROWS), SNAPSHOT_ID, CONFIG_SHA256, NORM_VERSION, MID_VERSION)
     for name, value in metrics.items()], SCHEMA_OUT)

(out.write.mode("overwrite")
    .option("replaceWhere", f"snapshot_id = '{SNAPSHOT_ID}'")
    .saveAsTable(DEDUP_COMPARE_TABLE))
print("Written:", DEDUP_COMPARE_TABLE, "|", len(metrics), "metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final printout (counts only)

# COMMAND ----------

print("Snapshot:", SNAPSHOT_ID)
print("Config SHA-256:", CONFIG_SHA256)
print("norm_version:", NORM_VERSION, "| mid_version:", MID_VERSION, "| base rows:", BASE_ROWS)
print("Check 3b: PASSED | sum checks: PASSED")
for name, value in metrics.items():
    print(f"  {name}: {value}")
print("F body-only key: not computed (not in the task list)")

# Diagnostic figures were measured on 21,634 rows, so a small difference is expected. They are shown, not matched.
print("\nDifferences from the diagnostic (base there: 21,634):")
print(f"  Message-ID extra copies: about 28 expected, got {metrics['A extra copies by Message-ID']}")
print(f"  Message-ID groups with 2+ rows: 21 expected, got {mid_groups}")
print(f"  of those, same ID different content: 3 expected, got {c1['differ']}")
print(f"  sender as key rows removed: about 19,570 (90%) expected, got {sender_removed} ({metrics['E percent of base removed if sender were the key']}%)")
