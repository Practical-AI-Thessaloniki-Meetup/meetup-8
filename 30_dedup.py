# Databricks notebook source
# MAGIC %md
# MAGIC # 30 Dedup (stage N, tasks N1 and N2): normalise, build the dup_key, flag duplicates
# MAGIC
# MAGIC Reads `silver_messages` for one snapshot and writes **one new table**, `silver_dedup`, with one row per parsed row.
# MAGIC `silver_messages`, `silver_quarantine` and Bronze are only read, never changed. Nothing is deleted: duplicates are **flagged**.
# MAGIC
# MAGIC - **dup_key** = SHA-256 of normalised subject + `\x1f` + normalised body (ADR 0003).
# MAGIC   Quoted lines are not stripped, and the subject keeps "Re:" and list tags.
# MAGIC - **short_body**: a normalised body shorter than `dedup.min_body_chars` (config) gets `dup_key = NULL`, is never grouped and is always primary.
# MAGIC - **Primary** = row 1 of `ROW_NUMBER` per dup_key, ordered by parsed date (NULLs last), list, month, `msg_index`.
# MAGIC   Gold joins on the grain key and filters `is_duplicate = false` for unique volume.
# MAGIC - Only hashes and flags are stored, not the normalised text. This notebook prints counts only.
# MAGIC
# MAGIC **Check 3:** primary + duplicates = parsed rows, and every grain key of `silver_messages` appears exactly once in `silver_dedup`.
# MAGIC If not, the notebook raises and the Job fails.
# MAGIC
# MAGIC **Parameter:** `snapshot_id` (empty means the newest snapshot in Bronze). A re-run replaces that snapshot's rows only.

# COMMAND ----------

# MAGIC %run ./00_setup_and_config

# COMMAND ----------

# MAGIC %run ./01_normalise

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType


def get_param(name, default):
    """Widget value when running as a notebook or Job task, else the default."""
    try:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name).strip()
    except NameError:
        return default


# Same default as 20_silver: the config holds settings, not run state, so empty means newest in Bronze.
LATEST_SNAPSHOT_ID = spark.sql(f"SELECT MAX(snapshot_id) FROM {BRONZE_TABLE}").first()[0]
REQUESTED_SNAPSHOT_ID = get_param("snapshot_id", "")
# In a Job no task picks its own default, so a missing parameter stops here (ADR 0012, point 5).
if RUNNING_IN_JOB and not REQUESTED_SNAPSHOT_ID:
    raise RuntimeError("snapshot_id is empty inside a Job. Set it from the Bronze printout.")
SNAPSHOT_ID = REQUESTED_SNAPSHOT_ID or LATEST_SNAPSHOT_ID
assert SNAPSHOT_ID, f"No rows in {BRONZE_TABLE}"

GRAIN = ["snapshot_id", "source_file", "msg_index"]

parsed = spark.table(SILVER_TABLE).where(F.col("snapshot_id") == SNAPSHOT_ID)
parsed_rows = parsed.count()
# Without this, Check 3 would pass on 0 = 0.
if parsed_rows == 0:
    raise AssertionError(f"No rows in {SILVER_TABLE} for snapshot {SNAPSHOT_ID}. Run 20_silver first.")

print("Snapshot:", SNAPSHOT_ID, "| newest in Bronze:", LATEST_SNAPSHOT_ID)
print("Config SHA-256:", CONFIG_SHA256)
print("Normalisation version:", NORMALISATION_VERSION)
print("Minimum normalised body length:", DEDUP_MIN_BODY_CHARS)
print("Parsed rows:", parsed_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## N1: normalise and build the dup_key (pure Python, one message in, one result out)

# COMMAND ----------

KEY_SCHEMA = StructType([
    StructField("dup_key", StringType()),
    StructField("short_body", BooleanType()),
])
key_udf = F.udf(make_key, KEY_SCHEMA)

keyed = (parsed
         .withColumn("k", key_udf("subject", "body"))
         .select(*GRAIN, "list_name", "month", "date_utc", "k.dup_key", "k.short_body"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## N2: primary vs duplicate
# MAGIC
# MAGIC Short-body rows (NULL key) skip the window: they are always primary, in a group of one.

# COMMAND ----------

by_key = Window.partitionBy("dup_key")
ranked = Window.partitionBy("dup_key").orderBy(
    F.col("date_utc").asc_nulls_last(), "list_name", "month", "msg_index")

with_key = (keyed.where(F.col("dup_key").isNotNull())
            .withColumn("is_duplicate", F.row_number().over(ranked) > 1)
            .withColumn("dup_group_size", F.count("*").over(by_key)))
no_key = (keyed.where(F.col("dup_key").isNull())
          .withColumn("is_duplicate", F.lit(False))
          .withColumn("dup_group_size", F.lit(1).cast("long")))

dedup_df = (with_key.unionByName(no_key)
            .select(*GRAIN, "list_name", "month", "dup_key", "short_body", "is_duplicate", "dup_group_size",
                    F.lit(NORMALISATION_VERSION).alias("normalisation_version"),
                    F.lit(CONFIG_SHA256).alias("config_sha256")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write
# MAGIC
# MAGIC `replaceWhere` replaces only this snapshot's rows, so a re-run never duplicates them.

# COMMAND ----------

(dedup_df.write.mode("overwrite")
    .option("replaceWhere", f"snapshot_id = '{SNAPSHOT_ID}'")
    .saveAsTable(DEDUP_TABLE))
print("Written:", DEDUP_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 3: primary + duplicates = parsed, and every grain key exactly once
# MAGIC
# MAGIC Counted from what is stored, not from the in-memory result.

# COMMAND ----------

stored = spark.table(DEDUP_TABLE).where(F.col("snapshot_id") == SNAPSHOT_ID)
stored.cache()   # one read for all the counts below

primary_rows = stored.where(~F.col("is_duplicate")).count()
duplicate_rows = stored.where(F.col("is_duplicate")).count()
distinct_keys = stored.select(*GRAIN).distinct().count()
missing_in_dedup = parsed.select(*GRAIN).join(stored.select(*GRAIN), GRAIN, "left_anti").count()
unknown_in_dedup = stored.select(*GRAIN).join(parsed.select(*GRAIN), GRAIN, "left_anti").count()

check3_ok = (primary_rows + duplicate_rows == parsed_rows and distinct_keys == parsed_rows
             and missing_in_dedup == 0 and unknown_in_dedup == 0)
if not check3_ok:
    raise AssertionError(
        f"CHECK 3 FAILED: parsed {parsed_rows}, primary {primary_rows}, duplicates {duplicate_rows}, "
        f"distinct grain keys in silver_dedup {distinct_keys}, parsed keys missing from silver_dedup {missing_in_dedup}, "
        f"silver_dedup keys not in silver_messages {unknown_in_dedup}")
print(f"CHECK 3 PASSED: primary {primary_rows} + duplicates {duplicate_rows} = parsed {parsed_rows}; every grain key once")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final printout (counts only)

# COMMAND ----------

EXPECTED_EXTRA_COPIES = 66   # from the earlier diagnostic at N = 50; a comparison, not a target

keyed_rows = stored.where(F.col("dup_key").isNotNull()).count()
short_rows = stored.where("short_body").count()

groups = (stored.where("dup_group_size > 1")
          .groupBy("dup_key")
          .agg(F.max("dup_group_size").alias("size"), F.countDistinct("list_name").alias("lists")))
group_rows = groups.collect()   # one small row per group, hash never printed
sizes = [r["size"] for r in group_rows]
same_list = sum(1 for r in group_rows if r["lists"] == 1)
cross_list = len(group_rows) - same_list

print("Snapshot:", SNAPSHOT_ID)
print("Config SHA-256 (this run):", CONFIG_SHA256)
print("Config SHA-256 (Bronze and Silver ran under): 5006efb69c7c26c57b90d172d0d0dd43f766fdf0c477f4a1fb420e0df2db02ab")
print("Normalisation version:", NORMALISATION_VERSION, "| minimum body length:", DEDUP_MIN_BODY_CHARS)
print(f"\nParsed rows {parsed_rows} | with a dup_key {keyed_rows} | short_body {short_rows}")
print(f"Primary rows {primary_rows} | duplicate rows {duplicate_rows} | Check 3: PASSED")
print(f"\nUnique messages (= primary rows): {primary_rows}")
print(f"Extra copies (= duplicate rows): {duplicate_rows}")
print(f"Duplicate groups: {len(sizes)} | largest group size: {max(sizes) if sizes else 0}")
print("Group size distribution:",
      f"2: {sum(s == 2 for s in sizes)} | 3: {sum(s == 3 for s in sizes)} | 4+: {sum(s >= 4 for s in sizes)}")
print(f"Groups with all copies in the same list: {same_list} | copies across lists: {cross_list}")

print("\nDifferences from the diagnostic expectation:")
if duplicate_rows == EXPECTED_EXTRA_COPIES:
    print("  none")
else:
    print(f"  extra copies: expected about {EXPECTED_EXTRA_COPIES}, got {duplicate_rows} "
          f"(difference {duplicate_rows - EXPECTED_EXTRA_COPIES:+d}). Not tuned to match.")
