# Databricks notebook source
# MAGIC %md
# MAGIC # 50 Checks (task C1): six reconciliations, the last task of Job 2
# MAGIC
# MAGIC Re-checks one snapshot from what is **stored**, not from what an earlier notebook had in memory.
# MAGIC Every check runs and is recorded in `check_results`; if any failed, the notebook raises at the end, so the Job fails
# MAGIC and nothing downstream may read Gold.
# MAGIC
# MAGIC 1. Bronze rows = source count per file (ADR 0009 exceptions apply), repeated here so a snapshot that never passed Job 1 is caught (ADR 0012)
# MAGIC 2. parsed + quarantined = Bronze
# MAGIC 3. primary + duplicates = parsed, every grain key once
# MAGIC 4. funnel is non-increasing
# MAGIC 5. required fields present (a NULL date is accepted only when `date_parse_flag` = 1)
# MAGIC 6. no address-shaped text in Gold (a guard rail, not a guarantee: ADR 0011)
# MAGIC
# MAGIC Each check also refuses to pass on 0 = 0. Prints counts only: no text, addresses, pseudonyms, Message-IDs or the salt.
# MAGIC
# MAGIC **Parameter:** `snapshot_id`. Inside a Job it must be set (ADR 0012); by hand, empty means the newest snapshot in Bronze.

# COMMAND ----------

# MAGIC %run ./00_setup_and_config

# COMMAND ----------

import json
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType, TimestampType


def get_param(name, default):
    """Widget value when running as a notebook or Job task, else the default."""
    try:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name).strip()
    except NameError:
        return default


REQUESTED_SNAPSHOT_ID = get_param("snapshot_id", "")
# In a Job no task picks its own default, so a missing parameter stops here (ADR 0012, point 5).
if RUNNING_IN_JOB and not REQUESTED_SNAPSHOT_ID:
    raise RuntimeError("snapshot_id is empty inside a Job. Set it from the Bronze printout.")

SNAPSHOT_ID = REQUESTED_SNAPSHOT_ID or spark.sql(f"SELECT MAX(snapshot_id) FROM {BRONZE_TABLE}").first()[0]
assert SNAPSHOT_ID, f"No rows in {BRONZE_TABLE}"
print("Snapshot:", SNAPSHOT_ID, "| given as a parameter:", bool(REQUESTED_SNAPSHOT_ID))
print("Config SHA-256:", CONFIG_SHA256)

GRAIN = ["snapshot_id", "source_file", "msg_index"]
GOLD_TABLES = [GOLD_FUNNEL_TABLE, GOLD_REVIEW_SET_TABLE, GOLD_CELL_SUMMARY_TABLE,
               GOLD_LIST_SUMMARY_TABLE, GOLD_PRIORITY_CURVE_TABLE, GOLD_KPIS_TABLE]
# Which Silver columns must always have a value. Message-ID and Cc may legitimately be missing, so they are not here.
REQUIRED_SILVER_COLUMNS = GRAIN + ["list_name", "month", "message_sha256", "sender_claimed_value", "body"]
# The funnel steps Gold must have written, in order. A missing step must fail, not shorten the check.
FUNNEL_STEPS = ["raw", "parsed", "unique", "in_scope", "keyword_hits", "review_set", "strong_hits"]


def stored(table):
    return spark.table(table).where(F.col("snapshot_id") == SNAPSHOT_ID)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Pure logic (no Spark), so it can be tested on small synthetic input

# COMMAND ----------

def short_files(usable, rows_by_file, exceptions):
    """Files with fewer Bronze rows than the source count, unless a recorded exception covers the shortfall.
    A file with no source count fails too: missing is not zero."""
    problems = []
    for m in usable:
        rows, source = rows_by_file.get(m["file"], 0), m["stats_count"]
        if source is None:
            problems.append(f"{m['file']}: no source count")
            continue
        shortfall = source - rows
        if shortfall <= 0:
            continue   # surplus is not a failure here; a truncated file has fewer messages, not more
        exception = exceptions.get(m["file"])
        if not (exception and shortfall <= exception["accepted_shortfall"]):
            problems.append(f"{m['file']}: {rows} rows against source count {source} ({shortfall} short)")
    return problems


def funnel_problem(counts_by_step):
    """None if the funnel has every step and never increases, else a short description."""
    missing = [s for s in FUNNEL_STEPS if s not in counts_by_step]
    if missing:
        return f"missing steps {missing}"
    counts = [counts_by_step[s] for s in FUNNEL_STEPS]
    if counts[0] <= 0:
        return "raw count is not positive"
    if any(later > earlier for earlier, later in zip(counts, counts[1:])):
        return "increases: " + ", ".join(f"{s} {counts_by_step[s]}" for s in FUNNEL_STEPS)
    return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the six checks (a check that errors counts as failed, with the error type only)

# COMMAND ----------

RESULTS = []   # (check name, passed, detail); detail holds counts only


def run_check(name, function):
    try:
        passed, detail = function()
    except Exception as e:
        passed, detail = False, f"could not run: {type(e).__name__}"
    RESULTS.append((name, bool(passed), detail))
    print(("PASSED" if passed else "FAILED"), name, "|", detail)


def check1_bronze_vs_source():
    with open(f"{VOLUME_ROOT}/raw/{SNAPSHOT_ID}/manifest.json", encoding="utf-8") as f:
        manifest = json.load(f)
    usable = [m for m in manifest if m["status"] in ("landed", "landed_empty", "skipped_exists") and m["bytes"] > 0]
    rows_by_file = {r[0]: r[1] for r in spark.sql(
        f"SELECT source_file, COUNT(*) FROM {BRONZE_TABLE} WHERE snapshot_id = '{SNAPSHOT_ID}' GROUP BY source_file").collect()}
    problems = short_files(usable, rows_by_file, BRONZE_EXCEPTIONS)
    bronze_rows = sum(rows_by_file.values())
    if not usable or bronze_rows == 0:
        return False, f"usable files {len(usable)}, Bronze rows {bronze_rows}"
    ok = not problems and len(rows_by_file) == len(usable)
    return ok, (f"Bronze rows {bronze_rows}, files in Bronze {len(rows_by_file)} of {len(usable)} usable, "
                f"files short {len(problems)}, exceptions in config {len(BRONZE_EXCEPTIONS)}")


def check2_parsed_plus_quarantined():
    bronze_rows = stored(BRONZE_TABLE).count()
    parsed, quarantined = stored(SILVER_TABLE), stored(QUARANTINE_TABLE)
    parsed_rows, quarantined_rows = parsed.count(), quarantined.count()
    # Equal sums could still hide a row in both tables and another in neither, so also count distinct keys.
    distinct_keys = parsed.select(*GRAIN).union(quarantined.select(*GRAIN)).distinct().count()
    ok = bronze_rows > 0 and parsed_rows + quarantined_rows == bronze_rows and distinct_keys == bronze_rows
    return ok, f"Bronze {bronze_rows}, parsed {parsed_rows}, quarantined {quarantined_rows}, distinct keys {distinct_keys}"


def check3_primary_plus_duplicates():
    parsed, dedup = stored(SILVER_TABLE), stored(DEDUP_TABLE)
    parsed_rows = parsed.count()
    primary_rows = dedup.where(~F.col("is_duplicate")).count()
    duplicate_rows = dedup.where(F.col("is_duplicate")).count()
    distinct_keys = dedup.select(*GRAIN).distinct().count()
    missing = parsed.select(*GRAIN).join(dedup.select(*GRAIN), GRAIN, "left_anti").count()
    unknown = dedup.select(*GRAIN).join(parsed.select(*GRAIN), GRAIN, "left_anti").count()
    ok = (parsed_rows > 0 and primary_rows + duplicate_rows == parsed_rows and distinct_keys == parsed_rows
          and missing == 0 and unknown == 0)
    return ok, (f"parsed {parsed_rows}, primary {primary_rows}, duplicates {duplicate_rows}, distinct keys {distinct_keys}, "
                f"missing from dedup {missing}, not in parsed {unknown}")


def check4_funnel_non_increasing():
    counts = {r["step"]: r["count"] for r in stored(GOLD_FUNNEL_TABLE).collect()}
    problem = funnel_problem(counts)
    return problem is None, ("funnel " + " >= ".join(f"{s} {counts[s]}" for s in FUNNEL_STEPS) if problem is None else problem)


def check5_required_fields():
    silver = stored(SILVER_TABLE)
    parsed_rows = silver.count()
    empty = lambda c: F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")
    # A NULL date is allowed only when the flag says why (missing or unparseable); the flag itself must be 0 or 1.
    bad_date = (F.col("date_utc").isNull() & (F.col("date_parse_flag") != 1)) | ~F.col("date_parse_flag").isin(0, 1) | F.col("date_parse_flag").isNull()
    not_claimed = F.col("sender_claimed").isNull() | ~F.col("sender_claimed")
    counts_row = silver.agg(
        *[F.sum(F.when(empty(c), 1).otherwise(0)).alias(c) for c in REQUIRED_SILVER_COLUMNS],
        F.sum(F.when(bad_date, 1).otherwise(0)).alias("date_without_flag"),
        F.sum(F.when(not_claimed, 1).otherwise(0)).alias("sender_not_marked_claimed"),
    ).first().asDict()
    problems = {k: int(v or 0) for k, v in counts_row.items() if int(v or 0) > 0}
    return parsed_rows > 0 and not problems, f"parsed rows {parsed_rows}, columns with problems {problems or 'none'}"


def check6_no_addresses_in_gold():
    # Only the address pattern is checked; names are out of scope (ADR 0011). Public list ids are address-shaped, so mask them first.
    forbidden = {"subject", "body", "sender_claimed_value", "message_id", "to_raw", "cc_raw", "date_raw"}
    address_hits, forbidden_found, gold_rows = 0, [], 0
    for table in GOLD_TABLES:
        df = stored(table)
        gold_rows += df.count()
        forbidden_found += [f"{table.split('.')[-1]}.{c}" for c in df.columns if c.lower() in forbidden]
        string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
        if string_cols:
            masked = lambda c: F.regexp_replace(F.col(c), LIST_ID_MASK, "")
            row = df.agg(*[F.sum(F.when(masked(c).rlike(EMAIL_PATTERN), 1).otherwise(0)).alias(c) for c in string_cols]).first()
            address_hits += sum(int(v or 0) for v in row)
    ok = gold_rows > 0 and address_hits == 0 and not forbidden_found
    return ok, f"Gold rows {gold_rows} in {len(GOLD_TABLES)} tables, address-shaped values {address_hits}, forbidden columns {len(forbidden_found)}"


run_check("1 Bronze rows vs source count", check1_bronze_vs_source)
run_check("2 parsed + quarantined = Bronze", check2_parsed_plus_quarantined)
run_check("3 primary + duplicates = parsed", check3_primary_plus_duplicates)
run_check("4 funnel non-increasing", check4_funnel_non_increasing)
run_check("5 required fields present", check5_required_fields)
run_check("6 no raw addresses in Gold", check6_no_addresses_in_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Record the results, then fail the Job if any check failed

# COMMAND ----------

RESULT_SCHEMA = StructType([
    StructField("snapshot_id", StringType()), StructField("check_name", StringType()),
    StructField("passed", BooleanType()), StructField("detail", StringType()),
    StructField("config_sha256", StringType()), StructField("checked_at", TimestampType()),
])
now = datetime.now(timezone.utc)
rows = [(SNAPSHOT_ID, name, passed, detail, CONFIG_SHA256, now) for name, passed, detail in RESULTS]
# Written before raising, so a failed run still leaves its evidence. A re-run replaces this snapshot's rows only.
(spark.createDataFrame(rows, RESULT_SCHEMA).write.mode("overwrite")
      .option("replaceWhere", f"snapshot_id = '{SNAPSHOT_ID}'").saveAsTable(CHECKS_TABLE))
print("Written:", CHECKS_TABLE)

failed = [name for name, passed, _ in RESULTS if not passed]
if failed:
    raise AssertionError(f"CHECKS FAILED ({len(failed)} of {len(RESULTS)}): " + "; ".join(failed))
print(f"ALL {len(RESULTS)} CHECKS PASSED for snapshot {SNAPSHOT_ID}. Gold may be read.")
