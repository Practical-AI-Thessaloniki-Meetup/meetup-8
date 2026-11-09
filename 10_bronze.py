# Databricks notebook source
# MAGIC %md
# MAGIC # 10 Bronze: one row per message, raw bytes, hashed
# MAGIC
# MAGIC Reads the mbox files of one snapshot, splits each file into messages, and appends them to the Bronze table.
# MAGIC Nothing is parsed, cleaned or deduplicated here. Bronze is the evidence.
# MAGIC
# MAGIC **Check 1:** for every file, putting its messages back together must give the original file hash.
# MAGIC If it does, no byte was lost or changed in the split.

# COMMAND ----------

# MAGIC %run ./00_setup_and_config

# COMMAND ----------

import hashlib
import json
import os
from datetime import datetime, timezone

from pyspark.sql.types import (BinaryType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

def get_param(name, default):
    """Widget value when running as a notebook or Job task, else the default."""
    try:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name).strip()
    except NameError:
        return default


def newest_snapshot():
    """Newest snapshot folder that has a manifest. Folder names are UTC timestamps, so the largest name is the newest."""
    root = f"{VOLUME_ROOT}/raw"
    folders = [d for d in os.listdir(root) if os.path.exists(f"{root}/{d}/manifest.json")]
    return max(folders) if folders else None


# Resolved once for Job 1 (ADR 0014). Job 2 tasks take the parameter; none picks its own default.
def snapshot_from_pull():
    """The id the pull task of this Job run made or resumed. Empty outside a Job or if it cannot be read."""
    try:
        return dbutils.jobs.taskValues.get(taskKey="pull", key="snapshot_id", default="", debugValue="")
    except Exception as e:
        print("Pull task value not read:", type(e).__name__)
        return ""


# Order: parameter (pins a snapshot), then what the pull task made, then newest (by hand only).
REQUESTED_SNAPSHOT_ID = get_param("snapshot_id", "") or snapshot_from_pull()
if RUNNING_IN_JOB and not REQUESTED_SNAPSHOT_ID:
    raise RuntimeError("No snapshot_id: not given as a parameter and not read from the pull task. Check the task is named pull.")
SNAPSHOT_ID = REQUESTED_SNAPSHOT_ID or newest_snapshot()
assert SNAPSHOT_ID, f"No snapshot with a manifest under {VOLUME_ROOT}/raw. Run the pull first."

SNAP_ROOT = f"{VOLUME_ROOT}/raw/{SNAPSHOT_ID}"
assert os.path.exists(f"{SNAP_ROOT}/manifest.json"), f"No manifest for snapshot {SNAPSHOT_ID}"
print("Snapshot:", SNAPSHOT_ID, "| given as a parameter or by the pull task:", bool(REQUESTED_SNAPSHOT_ID))
print("Config SHA-256:", CONFIG_SHA256)

# Hand the id to the next tasks. Outside a Job there is nothing to hand it to, so only the error type is printed.
try:
    dbutils.jobs.taskValues.set(key="snapshot_id", value=SNAPSHOT_ID)
except Exception as e:
    print("Task value not set (normal outside a Job):", type(e).__name__)

# COMMAND ----------

def split_mbox(data: bytes):
    """Split mbox bytes into messages. A new message starts at a line beginning with b'From '
    when it is the first line of the file or follows a blank line (the mbox convention).
    Every byte stays in exactly one part, so b''.join(parts) == data."""
    parts = []
    start = 0
    pos = 0
    prev_blank = True          # the start of the file counts as "after a blank line"
    n = len(data)
    while pos < n:
        end = data.find(b"\n", pos)
        end = n if end == -1 else end + 1
        line = data[pos:end]
        if line.startswith(b"From ") and prev_blank and pos > start:
            parts.append(data[start:pos])
            start = pos
        prev_blank = line.strip() == b""
        pos = end
    if start < n:
        parts.append(data[start:n])
    return parts

# COMMAND ----------

SCHEMA_BRONZE = StructType([
    StructField("snapshot_id", StringType(), False),
    StructField("list_name", StringType(), False),
    StructField("month", StringType(), False),
    StructField("source_file", StringType(), False),
    StructField("source_sha256", StringType(), False),
    StructField("msg_index", IntegerType(), False),
    StructField("raw_bytes", BinaryType(), False),
    StructField("message_sha256", StringType(), False),
    StructField("ingested_at", TimestampType(), False),
])

with open(f"{SNAP_ROOT}/manifest.json", encoding="utf-8") as f:
    manifest = json.load(f)

usable = [m for m in manifest if m["status"] in ("landed", "landed_empty", "skipped_exists") and m["bytes"] > 0]
print("Manifest files:", len(manifest), "| usable (landed, non-empty):", len(usable))

already = set()
if spark.catalog.tableExists(BRONZE_TABLE):
    already = {r[0] for r in spark.sql(
        f"SELECT DISTINCT source_file FROM {BRONZE_TABLE} WHERE snapshot_id = '{SNAPSHOT_ID}'").collect()}
    print("Files already in Bronze for this snapshot:", len(already))

# COMMAND ----------

report = []   # one row per file, for the check and the printout
now = datetime.now(timezone.utc)

for m in usable:
    fname = m["file"]
    if fname in already:
        report.append({"file": fname, "status": "skipped_already_loaded"})
        continue

    with open(f"{SNAP_ROOT}/mbox/{fname}", "rb") as f:
        data = f.read()

    file_sha = hashlib.sha256(data).hexdigest()
    parts = split_mbox(data)
    reassembled_ok = hashlib.sha256(b"".join(parts)).hexdigest() == file_sha
    matches_manifest_hash = file_sha == m["sha256"]

    report.append({
        "file": fname,
        "status": "loaded",
        "rows": len(parts),
        "from_lines": m["from_line_count"],
        "stats_count": m["stats_count"],
        "reassembled_ok": reassembled_ok,
        "file_hash_matches_manifest": matches_manifest_hash,
    })

    # Only write when the file is intact; a bad file is reported, not half-loaded.
    if not (reassembled_ok and matches_manifest_hash):
        report[-1]["status"] = "NOT_LOADED"
        continue

    rows = [
        (SNAPSHOT_ID, m["list"], m["month"], fname, file_sha, i,
         bytearray(p), hashlib.sha256(p).hexdigest(), now)
        for i, p in enumerate(parts)
    ]
    spark.createDataFrame(rows, SCHEMA_BRONZE).write.mode("append").saveAsTable(BRONZE_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 1 and the counts to compare

# COMMAND ----------

loaded = [r for r in report if r["status"] == "loaded"]
not_loaded = [r for r in report if r["status"] == "NOT_LOADED"]
skipped = [r for r in report if r["status"] == "skipped_already_loaded"]

print(f"Loaded: {len(loaded)} | not loaded (hash problem): {len(not_loaded)} | skipped (already in Bronze): {len(skipped)}")
for r in not_loaded:
    print("  NOT LOADED:", r["file"], "reassembled_ok:", r["reassembled_ok"], "matches_manifest:", r["file_hash_matches_manifest"])

# Counts from the Bronze table itself, for this snapshot
bronze_rows = spark.sql(f"SELECT COUNT(*) FROM {BRONZE_TABLE} WHERE snapshot_id = '{SNAPSHOT_ID}'").first()[0]
total_from = sum(m["from_line_count"] or 0 for m in usable)
total_stats = sum(m["stats_count"] or 0 for m in manifest)
print()
print("Bronze rows for this snapshot:", bronze_rows)
print("Manifest From-line count (approximate):", total_from)
print("Source-side count from stats (the naive number):", total_stats)

# Files where the three numbers disagree, counts only
diff = [r for r in loaded if r["rows"] != r["from_lines"] or r["rows"] != r["stats_count"]]
print(f"Files where Bronze rows differ from From-lines or stats: {len(diff)} of {len(loaded)}")
for r in diff[:15]:
    print(f"  {r['file']}: bronze {r['rows']}, from-lines {r['from_lines']}, stats {r['stats_count']}")

# The check itself: nothing lost in the split, and every usable file is in Bronze.
in_bronze_files = spark.sql(
    f"SELECT COUNT(DISTINCT source_file) FROM {BRONZE_TABLE} WHERE snapshot_id = '{SNAPSHOT_ID}'").first()[0]
# Without these two, an empty manifest or a wrong snapshot would pass every comparison on 0 = 0.
assert usable, f"Check 1 FAILED: no usable files in the manifest of snapshot {SNAPSHOT_ID}"
assert bronze_rows > 0, f"Check 1 FAILED: no Bronze rows for snapshot {SNAPSHOT_ID}"
assert not not_loaded, "Check 1 FAILED: at least one file did not reassemble to its original hash"
assert in_bronze_files == len(usable), f"Check 1 FAILED: {in_bronze_files} files in Bronze vs {len(usable)} usable in manifest"
# Check 1, second condition (ADR 0009): a file cut short during download still reassembles,
# so compare Bronze with the source's own count. Rows come from the table, so a re-run that skipped files still checks them.
rows_by_file = {r[0]: r[1] for r in spark.sql(
    f"SELECT source_file, COUNT(*) FROM {BRONZE_TABLE} WHERE snapshot_id = '{SNAPSHOT_ID}' GROUP BY source_file").collect()}

too_short = []
for m in usable:
    rows, source = rows_by_file.get(m["file"], 0), m["stats_count"]
    if source is None:
        too_short.append(f"{m['file']}: no source count in the manifest")   # missing is not zero
        continue
    shortfall = source - rows
    if shortfall <= 0:
        continue   # surplus is printed above, not failed: a truncated file has fewer messages, not more
    exception = BRONZE_EXCEPTIONS.get(m["file"])
    if exception and shortfall <= exception["accepted_shortfall"]:
        print(f"EXCEPTION APPLIED: {m['file']} is {shortfall} short (accepted {exception['accepted_shortfall']}). Reason: {exception['reason']}")
    else:
        too_short.append(f"{m['file']}: {rows} rows against source count {source} ({shortfall} short)")

if too_short:
    print("Completeness cannot be proven for these files (a short download, a split bug, or a source count that includes non-mail):")
    for line in too_short:
        print("  ", line)
    raise RuntimeError(f"CHECK 1 FAILED: {len(too_short)} file(s) have fewer rows than the source count")

print()
print("CHECK 1 PASSED: every usable file is in Bronze, reassembles to its original hash, and is not below the source count (exceptions printed above).")