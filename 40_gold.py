# Databricks notebook source
# MAGIC %md
# MAGIC # 40 Gold (stage G): funnel, review set, priority ranking, hours, pseudonymised view
# MAGIC
# MAGIC **Scenario (fictional, for the talk):** a consultancy advises a client on committing to a data-platform stack or approving
# MAGIC a major upgrade. An engineer must read the public community discussion for breaking changes, deprecations and regressions first.
# MAGIC Reading all of it is not affordable, so Gold says **which lists and months to read first, how many unique messages that is,
# MAGIC and what it costs in engineer hours** at assumed rates.
# MAGIC
# MAGIC Reads `silver_messages` joined to `silver_dedup` for one snapshot and writes six Gold tables (names in `00_setup_and_config`).
# MAGIC Bronze and Silver are only read. A re-run replaces that snapshot's rows only (`replaceWhere`).
# MAGIC
# MAGIC - **Unique** = `is_duplicate = false`. **Cell month** = year-month of the *parsed* date (UTC), not the source file month.
# MAGIC - **In-scope** = unique, list in `gold.scope_lists`, parsed date present, cell month in `gold.scope_months`.
# MAGIC   A row with no parsed date is never in-scope; it stays in unique and is reported separately.
# MAGIC - **Keyword match** = case-insensitive whole word, in subject or body; each keyword counts once per message.
# MAGIC - **Review set** = in-scope unique messages with at least one keyword. **Strong hit** = review-set message with at least
# MAGIC   `gold.strong_hit_min_keywords` distinct keywords. Keywords are a relevance **proxy**, not a judgement.
# MAGIC - **Priority score** = lower bound of the Wilson interval for the strong-hit share of a cell. It is for robustness: it stops a
# MAGIC   tiny cell from topping the ranking by luck. It is a ranking device, not a literal 95% statement (ADR 0004).
# MAGIC - **Sender pseudonym** = HMAC-SHA256 of the claimed address with a salt read from a secret (ADR 0005). Pseudonymised, not anonymised.
# MAGIC - Review rates and the cost per hour are the author's **assumptions**, not benchmarks.
# MAGIC
# MAGIC **Smoke checks** here: Check 4 (funnel non-increasing), Check 6 (no raw addresses in Gold) and row-count reconciliations.
# MAGIC Any failure raises, so the Job fails. The full checks notebook is task C1.
# MAGIC This notebook prints counts and aggregates only: no text, subjects, addresses, pseudonyms, Message-IDs or the salt.
# MAGIC
# MAGIC **Parameter:** `snapshot_id` (empty means the newest snapshot in Bronze).

# COMMAND ----------

# MAGIC %run ./00_setup_and_config

# COMMAND ----------

import hashlib
import hmac
import math
import re
from email.utils import parseaddr

from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)


def get_param(name, default):
    """Widget value when running as a notebook or Job task, else the default."""
    try:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name).strip()
    except NameError:
        return default


# Months are cut in UTC, the same zone Silver parsed the dates into.
spark.conf.set("spark.sql.session.timeZone", "UTC")

GRAIN = ["snapshot_id", "source_file", "msg_index"]

LATEST_SNAPSHOT_ID = spark.sql(f"SELECT MAX(snapshot_id) FROM {BRONZE_TABLE}").first()[0]
REQUESTED_SNAPSHOT_ID = get_param("snapshot_id", "")
# In a Job no task picks its own default, so a missing parameter stops here (ADR 0012, point 5).
if RUNNING_IN_JOB and not REQUESTED_SNAPSHOT_ID:
    raise RuntimeError("snapshot_id is empty inside a Job. Set it from the Bronze printout.")
SNAPSHOT_ID = REQUESTED_SNAPSHOT_ID or LATEST_SNAPSHOT_ID
assert SNAPSHOT_ID, f"No rows in {BRONZE_TABLE}"

K = STRONG_HIT_MIN_KEYWORDS
RATE_LABELS = ["r1", "r2"]   # the two rates, lowest first
print("Snapshot:", SNAPSHOT_ID, "| newest in Bronze:", LATEST_SNAPSHOT_ID)
print("Config SHA-256:", CONFIG_SHA256)
print("Scope lists:", len(GOLD_SCOPE_LISTS), "| scope months:", GOLD_SCOPE_MONTHS)
print("Keywords:", list(GOLD_KEYWORDS), "| strong hit at", K, "or more distinct keywords")
print("Review rates (documents per hour, assumptions):", REVIEW_RATES)
print("Cost per review hour:", COST_PER_REVIEW_HOUR if COST_PER_REVIEW_HOUR is not None else "not set")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pure Python helpers (no Spark, so they can be tested on small numbers)

# COMMAND ----------

def wilson_lower(strong, n, z):
    """Lower bound of the Wilson interval for strong out of n. None when n = 0 (no evidence, no score)."""
    if n == 0:
        return None
    p = strong / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (centre - margin) / (1 + z * z / n)


def rank_cells(cells, strong_key):
    """Cells sorted best first: score high, then fewer review hours (smaller n), then list, then month.
    A cell with n = 0 has no score and goes last. Returns new dicts with score and rank added."""
    scored = []
    for c in cells:
        score = wilson_lower(c[strong_key], c["review_set"], WILSON_Z)
        scored.append({**c, "score": score})
    scored.sort(key=lambda c: (c["score"] is None, -(c["score"] or 0), c["review_set"], c["list_name"], c["cell_month"]))
    for i, c in enumerate(scored, start=1):
        c["rank"] = i
    return scored


def curve_steps(ordered_cells, strong_key):
    """Cumulative review set and strong hits after each cell, for cells that have a review set."""
    steps, cum_n, cum_s = [], 0, 0
    for c in ordered_cells:
        if c["review_set"] == 0:
            continue
        cum_n += c["review_set"]
        cum_s += c[strong_key]
        steps.append({"step": len(steps) + 1, "list_name": c["list_name"], "cell_month": c["cell_month"],
                      "cum_review_set": cum_n, "cum_strong": cum_s})
    return steps


def hours_to_capture(steps, target, rate):
    """Review hours until the cumulative strong hits reach target x all strong hits. None if there are none.
    Whole cells are reviewed, so this is the hours at the end of the cell where the target is crossed."""
    if not steps or steps[-1]["cum_strong"] == 0:
        return None
    needed = target * steps[-1]["cum_strong"]
    for s in steps:
        if s["cum_strong"] >= needed - 1e-9:
            return s["cum_review_set"] / rate


def spearman(rank_a, rank_b):
    """Spearman correlation of two rankings of the same cells (ranks 1..m, no ties). None if fewer than 2 cells."""
    keys = list(rank_a)
    m = len(keys)
    if m < 2:
        return None
    d2 = sum((rank_a[k] - rank_b[k]) ** 2 for k in keys)
    return 1 - 6 * d2 / (m * (m * m - 1))


def pseudonym_of(sender, salt):
    """HMAC-SHA256 of the lowercased address in the claimed sender. None for a blank sender."""
    value = (sender or "").strip()
    if not value:
        return None
    try:
        address = parseaddr(value)[1].strip().lower()
    except Exception:   # a header the parser cannot read still gets a pseudonym from the whole value
        address = ""
    return hmac.new(salt, (address or value.lower()).encode("utf-8"), hashlib.sha256).hexdigest()


def keyword_pattern(variants):
    """Whole-word, case-insensitive regex for one keyword (Java regex, used by Spark rlike)."""
    return r"(?i)\b(" + "|".join(re.escape(v) for v in variants) + r")\b"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the base: silver_messages joined to silver_dedup

# COMMAND ----------

bronze_rows = spark.table(BRONZE_TABLE).where(F.col("snapshot_id") == SNAPSHOT_ID).count()
silver = spark.table(SILVER_TABLE).where(F.col("snapshot_id") == SNAPSHOT_ID)
dedup = spark.table(DEDUP_TABLE).where(F.col("snapshot_id") == SNAPSHOT_ID)
parsed_rows = silver.count()
# Without this, every check below would pass on 0 = 0.
if bronze_rows == 0 or parsed_rows == 0:
    raise AssertionError(f"No rows for snapshot {SNAPSHOT_ID}: Bronze {bronze_rows}, {SILVER_TABLE} {parsed_rows}. Run 20_silver and 30_dedup first.")

norm_versions = [r[0] for r in dedup.select("normalisation_version").distinct().collect()]
if len(norm_versions) != 1:
    raise AssertionError(f"Expected one normalisation_version in {DEDUP_TABLE}, found {len(norm_versions)}")
NORM_VERSION = norm_versions[0]

rows = (silver.join(dedup.select(*GRAIN, "is_duplicate"), GRAIN, "inner")
        .select(*GRAIN, "list_name", "month", "date_utc", "date_parse_flag", "subject", "body",
                "sender_claimed_value", "is_duplicate")
        .withColumn("cell_month", F.date_format("date_utc", "yyyy-MM")))
rows.cache()
joined_rows = rows.count()
if joined_rows != parsed_rows:
    raise AssertionError(f"silver_messages {parsed_rows} rows but the join with silver_dedup has {joined_rows}")
print(f"Bronze {bronze_rows} | parsed {parsed_rows} | normalisation {NORM_VERSION}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scope: counts per list and cell month (so the scope settings can be reviewed)

# COMMAND ----------

unique = rows.where(~F.col("is_duplicate"))
unique_rows = unique.count()

print("Unique messages by list and cell month (counts only; 'NO DATE' = no parsed date):")
by_cell = (unique.groupBy("list_name", F.coalesce("cell_month", F.lit("NO DATE")).alias("m"))
           .agg(F.count("*").alias("n")).orderBy("list_name", "m").collect())
for r in by_cell:
    print(f"  {r['list_name']} | {r['m']}: {r['n']}")

present_lists = {r["list_name"] for r in by_cell}
present_months = {r["m"] for r in by_cell}
bad_lists = [x for x in GOLD_SCOPE_LISTS if x not in present_lists]
bad_months = [x for x in GOLD_SCOPE_MONTHS if x not in present_months]
if bad_lists or bad_months:
    raise AssertionError(f"Scope settings not found in the data. Lists: {bad_lists} | months: {bad_months}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Keyword matching (on in-scope rows, duplicates included, so hits before dedup can be reported)

# COMMAND ----------

in_scope_cond = (F.col("list_name").isin(GOLD_SCOPE_LISTS) & F.col("date_utc").isNotNull()
                 & F.col("cell_month").isin(GOLD_SCOPE_MONTHS))

text = F.concat_ws(" ", "subject", "body")   # concat_ws skips NULLs
kw_cols = {name: F.when(text.rlike(keyword_pattern(variants)), 1).otherwise(0) for name, variants in GOLD_KEYWORDS.items()}
KW_NAMES = list(GOLD_KEYWORDS)

scoped = rows.where(in_scope_cond)
for name in KW_NAMES:
    scoped = scoped.withColumn("kw_" + name, kw_cols[name])
scoped = scoped.withColumn("keyword_count", sum(F.col("kw_" + n) for n in KW_NAMES))
scoped.cache()

scoped_unique = scoped.where(~F.col("is_duplicate"))
in_scope_rows = scoped_unique.count()
hits_before_dedup = scoped.where("keyword_count >= 1").count()
review = scoped_unique.where("keyword_count >= 1").withColumn("is_strong_hit", F.col("keyword_count") >= K)
review.cache()
review_size = review.count()
strong_total = review.where("is_strong_hit").count()

# Why unique rows are not in scope (three disjoint reasons)
no_date = unique.where(F.col("date_utc").isNull()).count()
list_out = unique.where(F.col("date_utc").isNotNull() & ~F.col("list_name").isin(GOLD_SCOPE_LISTS)).count()
month_out = unique.where(F.col("date_utc").isNotNull() & F.col("list_name").isin(GOLD_SCOPE_LISTS)
                         & ~F.col("cell_month").isin(GOLD_SCOPE_MONTHS)).count()
if no_date + list_out + month_out + in_scope_rows != unique_rows:
    raise AssertionError(f"Scope split does not add up: unique {unique_rows} = in scope {in_scope_rows} + no date {no_date} "
                         f"+ list {list_out} + month {month_out}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## G1: funnel (and Check 4, smoke version)

# COMMAND ----------

FUNNEL = [("raw", bronze_rows), ("parsed", parsed_rows), ("unique", unique_rows), ("in_scope", in_scope_rows),
          ("keyword_hits", review_size), ("review_set", review_size), ("strong_hits", strong_total)]

counts = [n for _, n in FUNNEL]
if any(later > earlier for earlier, later in zip(counts, counts[1:])):
    raise AssertionError("CHECK 4 FAILED: funnel increases: " + ", ".join(f"{s} {n}" for s, n in FUNNEL))
print("CHECK 4 PASSED (smoke): " + " >= ".join(f"{s} {n}" for s, n in FUNNEL))
print("Note: keyword_hits and review_set are the same number by definition (in-scope unique messages with a hit).")
print(f"Unique -> in scope drop: {unique_rows - in_scope_rows} = no date {no_date} + list not in scope {list_out} "
      f"+ month not in scope {month_out}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## G2: keyword statistics (in-scope unique messages)

# COMMAND ----------

kw_counts = scoped_unique.agg(*[F.sum("kw_" + n).alias(n) for n in KW_NAMES]).first().asDict()
kw_counts = {n: int(kw_counts[n] or 0) for n in KW_NAMES}
print("Messages matching each keyword (a message can match several):")
for n in KW_NAMES:
    print(f"  {n}: {kw_counts[n]}")

warnings = []
if review_size and max(kw_counts.values()) > review_size / 2:
    top = max(kw_counts, key=kw_counts.get)
    warnings.append(f"keyword '{top}' accounts for {kw_counts[top]} of {review_size} hits (more than half)")
if in_scope_rows and review_size > in_scope_rows / 2:
    warnings.append(f"the review set is {review_size} of {in_scope_rows} in-scope unique messages (more than half)")
for w in warnings:
    print("WARNING:", w, "-> the keyword set may not be selective enough; tighten or drop a keyword in config and rerun.")
print("Keyword warnings:", len(warnings))
print(f"Hit rate: {review_size / in_scope_rows:.4f}" if in_scope_rows else "Hit rate: no in-scope messages")

# COMMAND ----------

# MAGIC %md
# MAGIC ## G3: review hours and cost (the rates and the cost per hour are assumptions)

# COMMAND ----------

hours = [review_size / r for r in REVIEW_RATES]
hours_avoided = [(hits_before_dedup - review_size) / r for r in REVIEW_RATES]
cost = [h * COST_PER_REVIEW_HOUR if COST_PER_REVIEW_HOUR is not None else None for h in hours]

print(f"Hits before dedup: {hits_before_dedup} | review set: {review_size} | duplicates among hits: {hits_before_dedup - review_size}")
for r, h, a, c in zip(REVIEW_RATES, hours, hours_avoided, cost):
    cost_text = f"{c:.2f}" if c is not None else "cost per hour not set"
    print(f"  at {r} documents/hour: review hours {h:.2f} | hours avoided by dedup {a:.2f} | estimated cost {cost_text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## G5: priority ranking of list-and-month cells

# COMMAND ----------

thresholds = sorted({K, K + 1} | ({K - 1} if K - 1 >= 2 else set()))
agg = [F.count("*").alias("unique_in_scope"), F.sum(F.when(F.col("keyword_count") >= 1, 1).otherwise(0)).alias("review_set")]
agg += [F.sum(F.when(F.col("keyword_count") >= t, 1).otherwise(0)).alias(f"strong_{t}") for t in thresholds]
cell_rows = scoped_unique.groupBy("list_name", "cell_month").agg(*agg).collect()
cells = [{"list_name": r["list_name"], "cell_month": r["cell_month"], "unique_in_scope": int(r["unique_in_scope"]),
          "review_set": int(r["review_set"]), **{f"strong_{t}": int(r[f"strong_{t}"]) for t in thresholds}}
         for r in cell_rows]
for c in cells:
    c["strong"] = c[f"strong_{K}"]   # the headline threshold

ranked = rank_cells(cells, "strong")
chronological = sorted(cells, key=lambda c: (c["cell_month"], c["list_name"]))
steps = {"priority": curve_steps(ranked, "strong"), "chronological": curve_steps(chronological, "strong")}

cells_with_review = sum(1 for c in cells if c["review_set"] > 0)
print(f"Cells (list x month) in scope: {len(cells)} | with a review set: {cells_with_review}")
print("Top 5 cells by priority (list | month | review set n | strong hits s | score):")
for c in ranked[:5]:
    score = f"{c['score']:.4f}" if c["score"] is not None else "none"
    print(f"  {c['rank']}. {c['list_name']} | {c['cell_month']} | n={c['review_set']} | s={c['strong']} | {score}")

headline = {}   # (ordering, rate, target) -> hours
for ordering in steps:
    for r in REVIEW_RATES:
        for t in CAPTURE_TARGETS:
            headline[(ordering, r, t)] = hours_to_capture(steps[ordering], t, r)

print(f"\nReview hours to capture a share of all {strong_total} strong hits (whole cells), priority vs chronological:")
for r in REVIEW_RATES:
    for t in CAPTURE_TARGETS:
        p, ch = headline[("priority", r, t)], headline[("chronological", r, t)]
        if p is None:
            print(f"  {r}/h, {t:.0%}: no strong hits")
            continue
        saving = (ch - p) / ch if ch else 0
        print(f"  {r}/h, {t:.0%}: priority {p:.2f} h | chronological {ch:.2f} h | saving {saving:.1%}")
        if saving <= 0:
            print("    Prioritising gains nothing here: the curves are the same or priority is worse. That result is valid.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stability: move the strong-hit threshold by one keyword and compare the ranking

# COMMAND ----------

base_ranks = {(c["list_name"], c["cell_month"]): c["rank"] for c in ranked if c["review_set"] > 0}
stability = {}   # threshold -> (spearman, top5 overlap, top5 size)
for t in thresholds:
    if t == K:
        continue
    alt = rank_cells(cells, f"strong_{t}")
    alt_ranks = {(c["list_name"], c["cell_month"]): c["rank"] for c in alt if c["review_set"] > 0}
    top_n = min(5, len(base_ranks))
    top_base = {k for k, v in base_ranks.items() if v <= top_n}
    top_alt = {k for k, v in alt_ranks.items() if v <= top_n}
    stability[t] = (spearman(base_ranks, alt_ranks), len(top_base & top_alt), top_n)
    rho = stability[t][0]
    print(f"  threshold {t} (base {K}): Spearman {rho:.3f}" if rho is not None else f"  threshold {t}: Spearman not defined (fewer than 2 cells)",
          f"| top-{top_n} cells kept: {stability[t][1]} of {top_n}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## G4: pseudonymise senders (salt from a secret, never printed or stored)

# COMMAND ----------

def read_salt():
    """The salt lives in a secret scope, named in config. Only the error type is reported, never a value."""
    try:
        salt = dbutils.secrets.get(SALT_SECRET_SCOPE, SALT_SECRET_KEY)
    except Exception as e:
        raise RuntimeError(f"Could not read the salt secret (config gold.salt_secret_scope / salt_secret_key): {type(e).__name__}")
    if len(salt) < 16:
        raise RuntimeError("The salt secret is shorter than 16 characters")
    return salt.encode("utf-8")


SALT = read_salt()
pseudonym_udf = F.udf(lambda s: pseudonym_of(s, SALT), StringType())   # the salt travels with the task, not into any table

review_out = review.withColumn("sender_pseudonym", pseudonym_udf("sender_claimed_value"))
print("Salt read: OK (length not printed)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the Gold tables

# COMMAND ----------

STAMP = [F.lit(SNAPSHOT_ID).alias("snapshot_id"), F.lit(CONFIG_SHA256).alias("config_sha256"),
         F.lit(NORM_VERSION).alias("normalisation_version")]
STAMP_SCHEMA = [StructField("snapshot_id", StringType()), StructField("config_sha256", StringType()),
                StructField("normalisation_version", StringType())]
STAMP_VALUES = (SNAPSHOT_ID, CONFIG_SHA256, NORM_VERSION)


def small_table(records, fields):
    """Driver-side results (a few dozen rows) to a DataFrame, with the stamp columns appended."""
    schema = StructType(fields + STAMP_SCHEMA)
    return spark.createDataFrame([tuple(r) + STAMP_VALUES for r in records], schema)


def hours_pair(n):
    return tuple(n / r for r in REVIEW_RATES)


# 1. funnel
funnel_df = small_table([(s, i + 1, n) for i, (s, n) in enumerate(FUNNEL)],
                        [StructField("step", StringType()), StructField("step_order", IntegerType()),
                         StructField("count", LongType())])

# 2. review set: one row per review-set message, no text and no address
# GRAIN already holds snapshot_id, so only the other two stamp columns are added
review_set_df = review_out.select(*GRAIN, "list_name", "cell_month", "keyword_count", "is_strong_hit", "sender_pseudonym", *STAMP[1:])

# 3. cell summary (all in-scope cells; cells with no review set have no score and rank last)
cell_records = []
for c in ranked:
    n = c["review_set"]
    h1, h2 = hours_pair(n)
    cell_records.append((c["list_name"], c["cell_month"], c["unique_in_scope"], n, c["strong"],
                         c["strong"] / n if n else None, c["score"], c["rank"], h1, h2))
cell_df = small_table(cell_records, [
    StructField("list_name", StringType()), StructField("cell_month", StringType()),
    StructField("unique_in_scope", LongType()), StructField("review_set", LongType()), StructField("strong_hits", LongType()),
    StructField("share", DoubleType()), StructField("priority_score", DoubleType()), StructField("priority_rank", IntegerType()),
    StructField("review_hours_r1", DoubleType()), StructField("review_hours_r2", DoubleType())])

# 4. list summary
senders = {r["list_name"]: r["n"] for r in
           review_out.groupBy("list_name").agg(F.countDistinct("sender_pseudonym").alias("n")).collect()}
list_records = []
for name in GOLD_SCOPE_LISTS:
    mine = [c for c in cells if c["list_name"] == name]
    n = sum(c["review_set"] for c in mine)
    h1, h2 = hours_pair(n)
    list_records.append((name, sum(c["unique_in_scope"] for c in mine), n, sum(c["strong"] for c in mine),
                         int(senders.get(name, 0)), h1, h2))
list_df = small_table(list_records, [
    StructField("list_name", StringType()), StructField("unique_in_scope", LongType()), StructField("review_set", LongType()),
    StructField("strong_hits", LongType()), StructField("distinct_senders", LongType()),
    StructField("review_hours_r1", DoubleType()), StructField("review_hours_r2", DoubleType())])

# 5. priority curve
curve_records = []
for ordering in ("priority", "chronological"):
    for s in steps[ordering]:
        h1, h2 = hours_pair(s["cum_review_set"])
        curve_records.append((ordering, s["step"], s["list_name"], s["cell_month"], s["cum_review_set"], s["cum_strong"], h1, h2))
curve_df = small_table(curve_records, [
    StructField("ordering", StringType()), StructField("step", IntegerType()), StructField("list_name", StringType()),
    StructField("cell_month", StringType()), StructField("cum_review_set", LongType()), StructField("cum_strong", LongType()),
    StructField("cum_hours_r1", DoubleType()), StructField("cum_hours_r2", DoubleType())])

# 6. KPIs (value is numeric; rate and note are text)
quarantine_by_reason = {r["reason_code"]: r["n"] for r in
                        spark.table(QUARANTINE_TABLE).where(F.col("snapshot_id") == SNAPSHOT_ID)
                        .groupBy("reason_code").agg(F.count("*").alias("n")).collect()}
month_mismatch = rows.where(F.col("list_name").isin(GOLD_SCOPE_LISTS) & F.col("date_utc").isNotNull()
                            & (F.col("cell_month") != F.col("month"))).count()

kpi = []   # (metric, value, rate, note)
kpi += [("parse_success_rate", parsed_rows / bronze_rows, None, "parsed / Bronze rows"),
        ("unique_without_date", no_date, None, "unique, no parsed date: counted in unique, never in scope"),
        ("unique_out_of_scope_list", list_out, None, "unique, dated, list not in scope"),
        ("unique_out_of_scope_month", month_out, None, "unique, dated, list in scope, cell month not in scope"),
        ("hits_before_dedup", hits_before_dedup, None, "in-scope rows with a keyword hit, duplicates included"),
        ("review_set_size", review_size, None, "in-scope unique messages with a keyword hit"),
        ("strong_hits", strong_total, None, f"review-set messages with {K} or more distinct keywords"),
        ("month_differs_from_file", month_mismatch, None, "scope-list rows whose parsed-date month differs from the source file month (sanity number)")]
for reason, n in sorted(quarantine_by_reason.items()):
    kpi.append((f"quarantine_{reason}", n, None, "quarantined rows with this reason code"))
for name in KW_NAMES:
    kpi.append((f"keyword_{name}", kw_counts[name], None, "in-scope unique messages matching this keyword"))
for r, h, a, c in zip(REVIEW_RATES, hours, hours_avoided, cost):
    kpi += [("review_hours", h, f"{r} docs/hour", "assumed rate, not a benchmark"),
            ("hours_avoided_by_dedup", a, f"{r} docs/hour", "(hits before dedup - review set) / rate"),
            ("estimated_cost", c, f"{r} docs/hour", "hours x cost per review hour (assumption)" if c is not None else "cost per hour not set")]
for (ordering, r, t), h in headline.items():
    kpi.append(("capture_hours", h, f"{r} docs/hour", f"ordering={ordering}; target={t}; whole cells; NULL = no strong hits"))
for t, (rho, kept, top_n) in stability.items():
    kpi.append((f"stability_spearman_threshold_{t}", rho, None, f"ranking vs base threshold {K}"))
    kpi.append((f"stability_top{top_n}_kept_threshold_{t}", kept, None, f"of the top {top_n} cells, vs base threshold {K}"))
kpi_df = small_table([(m, None if v is None else float(v), rate, note) for m, v, rate, note in kpi],
                     [StructField("metric", StringType()), StructField("value", DoubleType()),
                      StructField("rate", StringType()), StructField("note", StringType())])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write (replaceWhere: only this snapshot's rows are replaced)

# COMMAND ----------

GOLD_TABLES = {
    GOLD_FUNNEL_TABLE: funnel_df, GOLD_REVIEW_SET_TABLE: review_set_df, GOLD_CELL_SUMMARY_TABLE: cell_df,
    GOLD_LIST_SUMMARY_TABLE: list_df, GOLD_PRIORITY_CURVE_TABLE: curve_df, GOLD_KPIS_TABLE: kpi_df,
}
for table, df in GOLD_TABLES.items():
    (df.write.mode("overwrite").option("replaceWhere", f"snapshot_id = '{SNAPSHOT_ID}'").saveAsTable(table))
    print("Written:", table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Smoke checks, from what is stored

# COMMAND ----------

def stored(table):
    return spark.table(table).where(F.col("snapshot_id") == SNAPSHOT_ID)


def fail(message):
    raise AssertionError("GOLD CHECK FAILED: " + message)


# Reconciliations against the definitions
rs_rows = stored(GOLD_REVIEW_SET_TABLE).count()
cell_sums = stored(GOLD_CELL_SUMMARY_TABLE).agg(F.sum("review_set"), F.sum("strong_hits"), F.sum("unique_in_scope")).first()
list_sums = stored(GOLD_LIST_SUMMARY_TABLE).agg(F.sum("review_set"), F.sum("strong_hits"), F.sum("unique_in_scope")).first()
funnel_stored = {r["step"]: r["count"] for r in stored(GOLD_FUNNEL_TABLE).collect()}
curve_end = {r["ordering"]: r["n"] for r in
             stored(GOLD_PRIORITY_CURVE_TABLE).groupBy("ordering").agg(F.max("cum_review_set").alias("n")).collect()}
distinct_review_keys = stored(GOLD_REVIEW_SET_TABLE).select(*GRAIN).distinct().count()

expected = (review_size, strong_total, in_scope_rows)
if rs_rows != review_size or distinct_review_keys != review_size:
    fail(f"review_set rows {rs_rows} (distinct {distinct_review_keys}) vs review set {review_size}")
if tuple(cell_sums) != expected:
    fail(f"cell_summary sums (review, strong, in scope) {tuple(cell_sums)} vs {expected}")
if tuple(list_sums) != expected:
    fail(f"list_summary sums (review, strong, in scope) {tuple(list_sums)} vs {expected}")
if funnel_stored != dict(FUNNEL):
    fail(f"stored funnel {funnel_stored} vs computed {dict(FUNNEL)}")
if review_size and any(curve_end.get(o) != review_size for o in ("priority", "chronological")):
    fail(f"curve ends at {curve_end} vs review set {review_size}")
print(f"RECONCILED: review_set rows {rs_rows} | cell and list sums match (review {review_size}, strong {strong_total}, in scope {in_scope_rows}) | funnel and curves match")

# Check 6 (smoke): no raw address in any Gold string column, and no forbidden column
FORBIDDEN = {"subject", "body", "sender_claimed_value", "message_id", "to_raw", "cc_raw", "date_raw"}
# The address pattern and the list-id mask are defined once in 00_setup_and_config (ADR 0011).

address_hits, forbidden_found = 0, []
for table in GOLD_TABLES:
    df = stored(table)
    forbidden_found += [f"{table.split('.')[-1]}.{c}" for c in df.columns if c.lower() in FORBIDDEN]
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    if string_cols:
        masked = lambda c: F.regexp_replace(F.col(c), LIST_ID_MASK, "")
        counts_row = df.agg(*[F.sum(F.when(masked(c).rlike(EMAIL_PATTERN), 1).otherwise(0)).alias(c) for c in string_cols]).first()
        address_hits += sum(int(v or 0) for v in counts_row)
if address_hits or forbidden_found:
    fail(f"CHECK 6: {address_hits} values look like an address; forbidden columns: {forbidden_found}")
print(f"CHECK 6 PASSED (smoke): 0 address-shaped values in {len(GOLD_TABLES)} Gold tables, 0 forbidden columns (list ids masked first)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final printout (counts only)

# COMMAND ----------

print("Snapshot:", SNAPSHOT_ID, "| config SHA-256 (this run):", CONFIG_SHA256, "| norm:", NORM_VERSION)
print("Scope months:", GOLD_SCOPE_MONTHS)
print("\nFunnel:", ", ".join(f"{s} {n}" for s, n in FUNNEL))
print(f"Unique -> in scope drop: no date {no_date} | list {list_out} | month {month_out}")
print("\nTable row counts:")
for table in GOLD_TABLES:
    print(f"  {table.split('.')[-1]}: {stored(table).count()}")
print("\nUnverified until the first run on Databricks: this whole notebook, the salt read, and the UTC session setting.")
