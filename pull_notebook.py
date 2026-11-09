# Databricks notebook source
# MAGIC %md
# MAGIC # Pull notebook: public mailing list archives to raw mbox files
# MAGIC
# MAGIC Lands one mbox file per list and month in a Unity Catalog volume, under a `snapshot_id` folder.
# MAGIC Nothing is parsed here. Bronze reads these files later.
# MAGIC
# MAGIC Each run writes:
# MAGIC - `pull_params.json`: what was asked for: the full config and its hash (the same params give the same pull)
# MAGIC - `stats/`: the raw stats JSON per list (source-side counts per month, the naive number)
# MAGIC - `mbox/`: one file per list and month, untouched
# MAGIC - `manifest.json` and `baseline.md`: what actually landed, with SHA-256 per file

# COMMAND ----------

# MAGIC %run ./00_setup_and_config

# COMMAND ----------

import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone

import requests

def get_param(name, default):
    """Widget value when running as a notebook or Job task, else the default."""
    try:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name).strip()
    except NameError:
        return default


# Empty = new snapshot. An existing id resumes it (ADR 0014); files already there are skipped.
REQUESTED_SNAPSHOT_ID = get_param("snapshot_id", "")

HEADERS = {"User-Agent": USER_AGENT}   # BASE, USER_AGENT, PAUSE_SECONDS, LISTS come from 00_setup_and_config


def check_same_config(params_path, current_sha256):
    """Resuming with different settings would mix two pulls in one snapshot, so stop."""
    if not os.path.exists(params_path):
        return   # nothing landed yet, so nothing to compare
    with open(params_path, encoding="utf-8") as f:
        old_sha256 = json.load(f).get("config_sha256")
    if old_sha256 != current_sha256:
        raise ValueError(
            "This snapshot was made with different settings (config_sha256 differs). "
            "Start a new snapshot: leave snapshot_id empty."
        )


SNAPSHOT_ID = REQUESTED_SNAPSHOT_ID or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

ROOT = f"{VOLUME_ROOT}/raw/{SNAPSHOT_ID}"
check_same_config(f"{ROOT}/pull_params.json", CONFIG_SHA256)
for sub in ("stats", "mbox"):
    os.makedirs(f"{ROOT}/{sub}", exist_ok=True)


def month_range(start, end):
    y, m = int(start[:4]), int(start[5:7])
    ey, em = int(end[:4]), int(end[5:7])
    out = []
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


MONTHS = month_range(START_MONTH, END_MONTH)


def get(url, params, tries=3):
    """GET with a few retries on rate limits and server errors. Returns the response or None."""
    for attempt in range(1, tries + 1):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=90)
            if r.status_code == 200:
                return r
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(5 * attempt)
                continue
            return r
        except requests.RequestException:
            time.sleep(5 * attempt)
    return None


with open(f"{ROOT}/pull_params.json", "w", encoding="utf-8") as f:
    json.dump(
        {
            "snapshot_id": SNAPSHOT_ID,
            "config_sha256": CONFIG_SHA256,
            "months": MONTHS,
            "config": CONFIG,
        },
        f,
        indent=2,
    )

print("Snapshot:", SNAPSHOT_ID, "| given as a parameter:", bool(REQUESTED_SNAPSHOT_ID))

# Bronze, the next task of Job 1, reads this so it loads the snapshot this run made (ADR 0014).
try:
    dbutils.jobs.taskValues.set(key="snapshot_id", value=SNAPSHOT_ID)
except Exception as e:
    print("Task value not set (normal outside a Job):", type(e).__name__)
print("Root:", ROOT)
print("Lists:", len(LISTS), "| Months:", len(MONTHS), "| Planned mbox calls:", len(LISTS) * len(MONTHS))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preflight: source-side counts, no mail downloaded
# MAGIC One stats call per list. `active_months` gives a message count per month: the naive number.

# COMMAND ----------

stats_counts = {}   # (list, domain) -> {month: count}

for lst, domain in LISTS:
    r = get(f"{BASE}/stats.lua", {"list": lst, "domain": domain, "d": START_MONTH, "emailsOnly": "true"})
    key = f"{lst}@{domain}"
    if r is None or r.status_code != 200:
        print(f"{key}: FAILED (status {None if r is None else r.status_code})")
        stats_counts[(lst, domain)] = {}
        continue
    data = r.json()
    with open(f"{ROOT}/stats/{key}.json", "w", encoding="utf-8") as f:
        f.write(r.text)
    active = data.get("active_months", {})
    stats_counts[(lst, domain)] = active
    in_window = {m: active.get(m) for m in MONTHS}
    total = sum(v for v in in_window.values() if v)
    print(f"{key}: {total} messages in window, months with data: {sum(1 for v in in_window.values() if v)}/{len(MONTHS)}")
    time.sleep(PAUSE_SECONDS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pull: one mbox file per list and month

# COMMAND ----------

manifest = []
FROM_LINE = re.compile(rb"^From ", re.M)

for lst, domain in LISTS:
    key = f"{lst}@{domain}"
    for month in MONTHS:
        fname = f"{key}_{month}.mbox"
        path = f"{ROOT}/mbox/{fname}"
        row = {
            "list": key,
            "month": month,
            "file": fname,
            "stats_count": stats_counts.get((lst, domain), {}).get(month),
            "status": None,
            "http_status": None,
            "bytes": 0,
            "sha256": None,
            "from_line_count": None,   # approximate: lines starting with "From ". Bronze does the real split.
        }

        if os.path.exists(path):
            content = open(path, "rb").read()
            row.update(status="skipped_exists", http_status=200)
        else:
            r = get(f"{BASE}/mbox.lua", {"list": key, "date": month})
            time.sleep(PAUSE_SECONDS)
            if r is None:
                row.update(status="failed_no_response")
                manifest.append(row)
                continue
            row["http_status"] = r.status_code
            if r.status_code != 200:
                row.update(status="failed_http")
                manifest.append(row)
                continue
            content = r.content
            with open(path, "wb") as f:
                f.write(content)
            row["status"] = "landed" if len(content) > 0 else "landed_empty"

        row["bytes"] = len(content)
        row["sha256"] = hashlib.sha256(content).hexdigest()
        row["from_line_count"] = len(FROM_LINE.findall(content))
        manifest.append(row)
        print(f"{fname}: {row['status']}, {row['bytes']} bytes, source count {row['stats_count']}, From-lines {row['from_line_count']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manifest and baseline

# COMMAND ----------

with open(f"{ROOT}/manifest.json", "w", encoding="utf-8") as f:
    json.dump(manifest, f, indent=2)

landed = [m for m in manifest if m["status"] in ("landed", "landed_empty", "skipped_exists")]
failed = [m for m in manifest if m["status"] not in ("landed", "landed_empty", "skipped_exists")]
total_stats = sum(m["stats_count"] or 0 for m in manifest)
total_from = sum(m["from_line_count"] or 0 for m in landed)
total_bytes = sum(m["bytes"] for m in landed)

lines = [
    f"# Baseline for snapshot {SNAPSHOT_ID}",
    "",
    f"- Lists: {len(LISTS)}, months: {len(MONTHS)}, files landed: {len(landed)}, failed: {len(failed)}",
    f"- Source-side message count (stats, the naive number): {total_stats}",
    f"- From-lines in landed mbox files (approximate message count): {total_from}",
    f"- Total size: {total_bytes / 1e6:.1f} MB",
    "",
    "| List | Month | Source count | From-lines | Bytes | Status |",
    "| --- | --- | --- | --- | --- | --- |",
]
for m in manifest:
    lines.append(
        f"| {m['list']} | {m['month']} | {m['stats_count']} | {m['from_line_count']} | {m['bytes']} | {m['status']} |"
    )

with open(f"{ROOT}/baseline.md", "w", encoding="utf-8") as f:
    f.write("\n".join(lines))

print("\n".join(lines[:8]))
if failed:
    print("\nFailed files (re-run with snapshot_id set to resume):")
    for m in failed:
        print(" ", m["file"], m["status"], m["http_status"])