# Databricks notebook source
# MAGIC %md
# MAGIC # 20 Silver (stage P): parse headers and body, quarantine what cannot be used
# MAGIC
# MAGIC Reads the Bronze rows of one snapshot, parses each message with Python's `email` library and writes two tables.
# MAGIC Every Bronze row lands in **exactly one** of them:
# MAGIC - `silver_messages`: parsed, with a usable `text/plain` body.
# MAGIC - `silver_quarantine`: not usable, with a reason code (`no_text_plain`, `empty_text_plain`, `parse_error`).
# MAGIC
# MAGIC The body is stored as is. Lowercasing, whitespace and quote handling belong to stage N.
# MAGIC The sender is **claimed, not verified**: it is stored as `sender_claimed_value` with `sender_claimed = true` on every row.
# MAGIC Bronze is only read, never changed. This notebook prints counts only: no text, subjects, addresses or Message-IDs.
# MAGIC
# MAGIC **Check 2:** parsed + quarantined = Bronze rows of the snapshot. If not, the notebook raises and the Job fails.
# MAGIC
# MAGIC **Parameters** (Job task parameters or widgets):
# MAGIC - `snapshot_id`: empty means the newest snapshot in Bronze.
# MAGIC - `dry_run`: `true` (default) parses, counts and runs Check 2 but writes nothing, so the date numbers can be reviewed first.
# MAGIC   Set `false` to write. A re-run of the same snapshot replaces that snapshot's rows.

# COMMAND ----------

# MAGIC %run ./00_setup_and_config

# COMMAND ----------

from datetime import datetime, timezone
from email import policy
from email.parser import BytesParser
from email.utils import parsedate_to_datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, IntegerType, StringType, StructField,
                               StructType, TimestampType)


def get_param(name, default):
    """Widget value when running as a notebook or Job task, else the default."""
    try:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name).strip()
    except NameError:
        return default


# No snapshot key in the config (it holds settings, not run state), so empty means newest in Bronze.
LATEST_SNAPSHOT_ID = spark.sql(f"SELECT MAX(snapshot_id) FROM {BRONZE_TABLE}").first()[0]
REQUESTED_SNAPSHOT_ID = get_param("snapshot_id", "")
# In a Job no task picks its own default, so a missing parameter stops here (ADR 0012, point 5).
if RUNNING_IN_JOB and not REQUESTED_SNAPSHOT_ID:
    raise RuntimeError("snapshot_id is empty inside a Job. Set it from the Bronze printout.")
SNAPSHOT_ID = REQUESTED_SNAPSHOT_ID or LATEST_SNAPSHOT_ID
DRY_RUN = get_param("dry_run", "true").lower() != "false"

assert SNAPSHOT_ID, f"No rows in {BRONZE_TABLE}"
print("Snapshot:", SNAPSHOT_ID, "| newest in Bronze:", LATEST_SNAPSHOT_ID)
print("Config SHA-256:", CONFIG_SHA256)
print("Dry run (nothing is written):", DRY_RUN)
# A dry run in a Job would write nothing and the next task would fail on empty input, far from the cause.
if DRY_RUN and RUNNING_IN_JOB:
    raise RuntimeError("dry_run is true inside a Job. Set the task parameter dry_run to false, or run this notebook by hand.")

# The expected date window, from config: first instant of start_month up to (not including) the month after end_month.
def month_start(yyyy_mm, add_months=0):
    year, month = int(yyyy_mm[:4]), int(yyyy_mm[5:7]) - 1 + add_months
    return datetime(year + month // 12, month % 12 + 1, 1, tzinfo=timezone.utc)

WINDOW_START = month_start(START_MONTH)
WINDOW_END = month_start(END_MONTH, add_months=1)
print("Date window (UTC):", WINDOW_START.date(), "to before", WINDOW_END.date())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parsing (pure Python, one message in, one tuple out)

# COMMAND ----------

def parse_date(raw_date):
    """Return (UTC datetime or None, reason or None). A missing or unparseable date gives None, not a guess."""
    if raw_date is None or not raw_date.strip():
        return None, "missing"
    try:
        moment = parsedate_to_datetime(raw_date)
        if moment.tzinfo is None:   # "-0000" style dates carry no zone: treat as UTC
            moment = moment.replace(tzinfo=timezone.utc)
        moment = moment.astimezone(timezone.utc)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None, "unparseable"
    if not (WINDOW_START <= moment < WINDOW_END):
        return moment, "out_of_window"   # the date is real, so keep it and only flag it
    return moment, None


def leaf_types(msg):
    """Content types of the non-container parts, for the quarantine detail (types only, never content)."""
    types = sorted({p.get_content_type() for p in msg.walk() if not p.is_multipart()})
    return ", ".join(types)[:200] or msg.get_content_type()


def decode_part(part):
    """Decode with the declared charset; replace bad bytes. An unknown charset falls back to UTF-8."""
    payload = part.get_payload(decode=True) or b""
    try:
        return payload.decode(part.get_content_charset() or "utf-8", errors="replace")
    except LookupError:
        return payload.decode("utf-8", errors="replace")


def header_text(msg, name):
    value = msg.get(name)
    return None if value is None else str(value)


def parse_message(raw_bytes):
    """Returns (sender, to, cc, subject, date_raw, date_utc, date_flag, date_reason, message_id, body,
    reason_code, reason_detail). A failure becomes a reason_code; no row is ever skipped."""
    try:
        msg = BytesParser(policy=policy.default).parsebytes(bytes(raw_bytes))
        date_raw = header_text(msg, "Date")
        date_utc, date_reason = parse_date(date_raw)
        message_id = header_text(msg, "Message-ID")
        headers = (header_text(msg, "From"), header_text(msg, "To"), header_text(msg, "Cc"),
                   header_text(msg, "Subject"), date_raw, date_utc,
                   1 if date_reason else 0, date_reason,
                   message_id.strip() if message_id else None)   # not lowercased: the local part is case-sensitive

        part = msg.get_body(preferencelist=("plain",))   # first text/plain that is not an attachment
        if part is None:
            return headers + (None, "no_text_plain", "content types: " + leaf_types(msg))
        body = decode_part(part)
        if not body.strip():
            return headers + (body, "empty_text_plain", "content types: " + leaf_types(msg))
        return headers + (body, None, None)
    except Exception as e:   # safety net: any failure still becomes a quarantine row, with the class only
        return (None,) * 7 + (0, None, None, "parse_error", "exception: " + type(e).__name__)

# COMMAND ----------

PARSED_SCHEMA = StructType([
    StructField("sender_claimed_value", StringType()),
    StructField("to_raw", StringType()),
    StructField("cc_raw", StringType()),
    StructField("subject", StringType()),
    StructField("date_raw", StringType()),
    StructField("date_utc", TimestampType()),
    StructField("date_parse_flag", IntegerType()),
    StructField("date_parse_reason", StringType()),
    StructField("message_id", StringType()),
    StructField("body", StringType()),
    StructField("reason_code", StringType()),
    StructField("reason_detail", StringType()),
])

parse_udf = F.udf(parse_message, PARSED_SCHEMA)

GRAIN = ["snapshot_id", "source_file", "msg_index"]

bronze = spark.table(BRONZE_TABLE).where(F.col("snapshot_id") == SNAPSHOT_ID)
bronze_rows = bronze.count()
assert bronze_rows > 0, f"No Bronze rows for snapshot {SNAPSHOT_ID}"

# One row per Bronze row, parsed or not. Splitting on reason_code below guarantees "exactly one table".
result = (bronze
          .withColumn("p", parse_udf("raw_bytes"))
          .select(*GRAIN, "list_name", "month", "message_sha256", "p.*"))

SILVER_COLUMNS = GRAIN + [
    "list_name", "month", "message_sha256",
    F.lit(SNAPSHOT_ID == LATEST_SNAPSHOT_ID).alias("latest_snapshot"),
    "sender_claimed_value", F.lit(True).alias("sender_claimed"),
    "to_raw", "cc_raw", "subject", "date_raw", "date_utc", "date_parse_flag", "date_parse_reason",
    "message_id", "body",
]
QUARANTINE_COLUMNS = GRAIN + ["list_name", "month", "reason_code", "reason_detail", "message_sha256"]

parsed_df = result.where(F.col("reason_code").isNull()).select(*SILVER_COLUMNS)
quarantine_df = result.where(F.col("reason_code").isNotNull()).select(*QUARANTINE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date problems, counted before anything is written
# MAGIC
# MAGIC These rows stay in the parsed table with `date_parse_flag = 1`. Missing and unparseable dates have a NULL `date_utc`,
# MAGIC so Check 5 (required fields) must accept a NULL date when `date_parse_flag = 1`.

# COMMAND ----------

date_counts = {r["date_parse_reason"]: r["n"] for r in
               parsed_df.where("date_parse_flag = 1").groupBy("date_parse_reason").agg(F.count("*").alias("n")).collect()}
print("Parsed rows that would have date_parse_flag = 1:", sum(date_counts.values()))
for reason in ("missing", "unparseable", "out_of_window"):
    print(f"  {reason}: {date_counts.get(reason, 0)}")
null_dates = date_counts.get("missing", 0) + date_counts.get("unparseable", 0)
if null_dates:
    print(f"\n{null_dates} parsed rows have a NULL date. Check 5 must accept a NULL date when date_parse_flag = 1.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write (skipped in a dry run)
# MAGIC
# MAGIC `replaceWhere` replaces only this snapshot's rows, so a re-run never duplicates them and other snapshots stay untouched.

# COMMAND ----------

def write_snapshot(df, table):
    (df.write.mode("overwrite")
       .option("replaceWhere", f"snapshot_id = '{SNAPSHOT_ID}'")
       .saveAsTable(table))


if DRY_RUN:
    print("Dry run: nothing written. Set the dry_run parameter to false to write.")
else:
    write_snapshot(parsed_df, SILVER_TABLE)
    write_snapshot(quarantine_df, QUARANTINE_TABLE)
    # From here on, report from what is stored, not from the in-memory result.
    parsed_df = spark.table(SILVER_TABLE).where(F.col("snapshot_id") == SNAPSHOT_ID)
    quarantine_df = spark.table(QUARANTINE_TABLE).where(F.col("snapshot_id") == SNAPSHOT_ID)
    print("Written:", SILVER_TABLE, "and", QUARANTINE_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 2: parsed + quarantined = Bronze

# COMMAND ----------

parsed_rows = parsed_df.count()
quarantined_rows = quarantine_df.count()
# Equal counts could still hide a row in both tables and another in neither, so also count distinct keys.
distinct_keys = parsed_df.select(*GRAIN).union(quarantine_df.select(*GRAIN)).distinct().count()

if parsed_rows + quarantined_rows != bronze_rows or distinct_keys != bronze_rows:
    raise AssertionError(
        f"CHECK 2 FAILED: Bronze {bronze_rows}, parsed {parsed_rows}, quarantined {quarantined_rows}, "
        f"distinct keys across both {distinct_keys}")
print(f"CHECK 2 PASSED: Bronze {bronze_rows} = parsed {parsed_rows} + quarantined {quarantined_rows}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final printout (counts only)

# COMMAND ----------

EXPECTED = {"bronze": 21634, "parsed": 21604, "quarantined": 30, "no_text_plain": 25, "empty_text_plain": 5}

by_reason = {r["reason_code"]: r["n"] for r in
             quarantine_df.groupBy("reason_code").agg(F.count("*").alias("n")).collect()}
blank_subject = parsed_df.where("subject IS NULL OR trim(subject) = ''").count()
blank_sender = parsed_df.where("sender_claimed_value IS NULL OR trim(sender_claimed_value) = ''").count()

print("Snapshot:", SNAPSHOT_ID, "| config SHA-256:", CONFIG_SHA256, "| dry run:", DRY_RUN)
print(f"Bronze {bronze_rows} | parsed {parsed_rows} | quarantined {quarantined_rows} | Check 2: PASSED")

print("\nQuarantine by reason_code:")
for reason, n in sorted(by_reason.items()):
    print(f"  {reason}: {n}")
print("Quarantine by list and reason_code:")
for r in quarantine_df.groupBy("list_name", "reason_code").agg(F.count("*").alias("n")).orderBy("list_name", "reason_code").collect():
    print(f"  {r['list_name']} | {r['reason_code']}: {r['n']}")

print("\nParsed rows with date_parse_flag = 1:", sum(date_counts.values()))
for reason in ("missing", "unparseable", "out_of_window"):
    print(f"  {reason}: {date_counts.get(reason, 0)}")
print("\nParsed rows with null or empty subject:", blank_subject)
print("Parsed rows with null or empty sender_claimed_value:", blank_sender)

actual = {"bronze": bronze_rows, "parsed": parsed_rows, "quarantined": quarantined_rows,
          "no_text_plain": by_reason.get("no_text_plain", 0), "empty_text_plain": by_reason.get("empty_text_plain", 0)}
print("\nDifferences from expectations (from the earlier diagnostic):")
differences = [f"  {k}: expected {EXPECTED[k]}, got {actual[k]}" for k in EXPECTED if EXPECTED[k] != actual[k]]
if by_reason.get("parse_error", 0):
    differences.append(f"  parse_error: expected 0, got {by_reason['parse_error']}")
print("\n".join(differences) if differences else "  none")
