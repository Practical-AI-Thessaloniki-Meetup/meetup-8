# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00 Setup and config (single source of truth)
# MAGIC
# MAGIC Run once to create the schema and volume. Every other notebook starts with `%run ./00_setup_and_config`
# MAGIC and reads its names from here, so a rename happens in one place.
# MAGIC Settings live in `config/pipeline.json` (validated here, hashed so a snapshot records which settings made it).
# MAGIC Safe to re-run: it only creates what is missing and never drops anything.

# COMMAND ----------

import hashlib
import json
import os
import re

CONFIG_RELATIVE_PATH = "config/pipeline.json"
MONTH_PATTERN = re.compile(r"\d{4}-(0[1-9]|1[0-2])")   # YYYY-MM


def find_config_path():
    """Look next to this notebook in the workspace first, then in the current folder."""
    candidates = []
    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        candidates.append("/Workspace" + os.path.dirname(notebook_path) + "/" + CONFIG_RELATIVE_PATH)
    except Exception:
        pass   # not on Databricks (or no dbutils): use the fallback only
    candidates.append(os.path.join(os.getcwd(), CONFIG_RELATIVE_PATH))
    for path in candidates:
        if os.path.exists(path):
            return path
    raise FileNotFoundError(f"{CONFIG_RELATIVE_PATH} not found. Looked in: {candidates}")


def canonical_sha256(config):
    """Hash of the settings, not of the file: spacing and key order do not change it."""
    text = json.dumps(config, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def need(section, key, expected_type, where):
    """Return section[key] or raise a ValueError that names the exact setting."""
    if not isinstance(section, dict) or key not in section:
        raise ValueError(f"config: missing '{where}{key}'")
    value = section[key]
    # bool is a subclass of int in Python, so reject it explicitly for numbers
    if not isinstance(value, expected_type) or isinstance(value, bool):
        raise ValueError(f"config: '{where}{key}' has the wrong type ({type(value).__name__})")
    return value


def validate_config(config):
    for key in ("catalog", "schema", "volume"):
        if not need(config, key, str, ""):
            raise ValueError(f"config: '{key}' is empty")

    source = need(config, "source", dict, "")
    if not need(source, "base_url", str, "source.").startswith("https://"):
        raise ValueError("config: 'source.base_url' must start with https://")
    if not need(source, "user_agent", str, "source.").strip():
        raise ValueError("config: 'source.user_agent' is empty")
    if need(source, "pause_seconds", (int, float), "source.") < 0.5:
        raise ValueError("config: 'source.pause_seconds' must be at least 0.5 (be polite to the source)")

    lists = need(config, "lists", list, "")
    if not lists:
        raise ValueError("config: 'lists' is empty")
    for i, item in enumerate(lists):
        for key in ("prefix", "domain"):
            if not need(item, key, str, f"lists[{i}].").strip():
                raise ValueError(f"config: 'lists[{i}].{key}' is empty")

    window = need(config, "window", dict, "")
    start = need(window, "start_month", str, "window.")
    end = need(window, "end_month", str, "window.")
    for name, month in (("start_month", start), ("end_month", end)):
        if not MONTH_PATTERN.fullmatch(month):
            raise ValueError(f"config: 'window.{name}' must look like YYYY-MM, got '{month}'")
    if start > end:   # YYYY-MM strings sort in date order
        raise ValueError(f"config: 'window.start_month' ({start}) is after 'window.end_month' ({end})")

    dedup = need(config, "dedup", dict, "")
    if need(dedup, "min_body_chars", int, "dedup.") < 1:
        raise ValueError("config: 'dedup.min_body_chars' must be at least 1")
    if not need(dedup, "normalisation_version", str, "dedup.").strip():   # checked against the code in 01_normalise (ADR 0007)
        raise ValueError("config: 'dedup.normalisation_version' is empty")

    validate_bronze(need(config, "bronze", dict, ""))
    validate_gold(need(config, "gold", dict, ""), lists)


def validate_bronze(bronze):
    """Each exception names one file, the shortfall accepted for it, and why (ADR 0009). No general tolerance."""
    exceptions = need(bronze, "exceptions", list, "bronze.")
    seen = set()
    for i, item in enumerate(exceptions):
        where = f"bronze.exceptions[{i}]."
        file_name = need(item, "file", str, where).strip()
        if not file_name or file_name in seen:
            raise ValueError(f"config: '{where}file' is empty or listed twice")
        seen.add(file_name)
        if need(item, "accepted_shortfall", int, where) < 1:
            raise ValueError(f"config: '{where}accepted_shortfall' must be at least 1")
        if not need(item, "reason", str, where).strip():
            raise ValueError(f"config: '{where}reason' is empty")


def is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_gold(gold, lists):
    known_lists = {f"{item['prefix']}@{item['domain']}" for item in lists}
    scope_lists = need(gold, "scope_lists", list, "gold.")
    if not scope_lists or any(x not in known_lists for x in scope_lists):
        raise ValueError("config: 'gold.scope_lists' must be a non-empty subset of the configured lists (prefix@domain)")

    scope_months = need(gold, "scope_months", list, "gold.")
    if not scope_months or any(not isinstance(m, str) or not MONTH_PATTERN.fullmatch(m) for m in scope_months):
        raise ValueError("config: 'gold.scope_months' must be a non-empty list of YYYY-MM strings")

    keywords = need(gold, "keywords", dict, "gold.")
    if not keywords:
        raise ValueError("config: 'gold.keywords' is empty")
    for name, variants in keywords.items():
        if not isinstance(variants, list) or not variants or any(not isinstance(v, str) or not v.strip() for v in variants):
            raise ValueError(f"config: 'gold.keywords.{name}' must be a non-empty list of non-empty strings")

    strong = need(gold, "strong_hit_min_keywords", int, "gold.")
    if strong < 1 or strong > len(keywords):
        raise ValueError("config: 'gold.strong_hit_min_keywords' must be between 1 and the number of keywords")

    rates = need(gold, "review_rates_docs_per_hour", list, "gold.")
    if len(rates) != 2 or not all(is_number(r) and r > 0 for r in rates) or rates[0] >= rates[1]:
        raise ValueError("config: 'gold.review_rates_docs_per_hour' must be two positive numbers, ascending")

    if "cost_per_review_hour" not in gold:
        raise ValueError("config: missing 'gold.cost_per_review_hour' (use null if not set)")
    cost = gold["cost_per_review_hour"]
    if cost is not None and (not is_number(cost) or cost < 0):
        raise ValueError("config: 'gold.cost_per_review_hour' must be null or a number >= 0")

    if need(gold, "wilson_z", (int, float), "gold.") <= 0:
        raise ValueError("config: 'gold.wilson_z' must be greater than 0")

    targets = need(gold, "capture_targets", list, "gold.")
    if not targets or not all(is_number(t) and 0 < t <= 1 for t in targets):
        raise ValueError("config: 'gold.capture_targets' must be a non-empty list of numbers in (0, 1]")

    for key in ("salt_secret_scope", "salt_secret_key"):   # names only, never the salt itself
        if not need(gold, key, str, "gold.").strip():
            raise ValueError(f"config: 'gold.{key}' is empty")


# COMMAND ----------

# ---- CONFIG: names and settings come from config/pipeline.json, nowhere else ------

CONFIG_PATH = find_config_path()
with open(CONFIG_PATH, encoding="utf-8") as f:
    CONFIG = json.load(f)
validate_config(CONFIG)
CONFIG_SHA256 = canonical_sha256(CONFIG)

CATALOG = CONFIG["catalog"]
SCHEMA = CONFIG["schema"]
VOLUME = CONFIG["volume"]

BASE = CONFIG["source"]["base_url"]
USER_AGENT = CONFIG["source"]["user_agent"]
PAUSE_SECONDS = CONFIG["source"]["pause_seconds"]   # wait between calls to the Apache server

LISTS = [(item["prefix"], item["domain"]) for item in CONFIG["lists"]]
START_MONTH = CONFIG["window"]["start_month"]   # inclusive
END_MONTH = CONFIG["window"]["end_month"]       # inclusive

DEDUP_MIN_BODY_CHARS = CONFIG["dedup"]["min_body_chars"]   # normalised bodies shorter than this are never grouped

RUNNING_IN_JOB = False   # True when this notebook runs as a Job task; used to refuse a dry run there
try:
    RUNNING_IN_JOB = bool(dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobId().isDefined())
except Exception:
    pass   # not on Databricks, or the context is not readable: treat as not in a Job

BRONZE_EXCEPTIONS = {e["file"]: e for e in CONFIG["bronze"]["exceptions"]}   # file name -> accepted shortfall and reason

GOLD = CONFIG["gold"]
GOLD_SCOPE_LISTS = GOLD["scope_lists"]
GOLD_SCOPE_MONTHS = GOLD["scope_months"]               # explicit data, not computed from "latest"
GOLD_KEYWORDS = GOLD["keywords"]                       # keyword name -> whole-word variants
STRONG_HIT_MIN_KEYWORDS = GOLD["strong_hit_min_keywords"]
REVIEW_RATES = GOLD["review_rates_docs_per_hour"]      # assumptions, not benchmarks
COST_PER_REVIEW_HOUR = GOLD["cost_per_review_hour"]    # None = not set; cost columns stay NULL
WILSON_Z = GOLD["wilson_z"]
CAPTURE_TARGETS = GOLD["capture_targets"]
SALT_SECRET_SCOPE = GOLD["salt_secret_scope"]          # where the salt lives (name only)
SALT_SECRET_KEY = GOLD["salt_secret_key"]

# Check 6 looks for address-shaped text only (ADR 0011). Defined once so the Gold smoke check and the Checks task cannot drift.
EMAIL_PATTERN = r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9\-]+(\.[A-Za-z0-9\-]+)*\.[A-Za-z]{2,}"
LIST_ID_MASK = "|".join(re.escape(x) for x in GOLD_SCOPE_LISTS)   # the public list ids are address-shaped, so they are masked first

# Tables written by later notebooks (names only; nothing is created here)
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_messages"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_messages"
QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA}.silver_quarantine"
DEDUP_TABLE = f"{CATALOG}.{SCHEMA}.silver_dedup"
DEDUP_COMPARE_TABLE = f"{CATALOG}.{SCHEMA}.silver_dedup_compare"
GOLD_FUNNEL_TABLE = f"{CATALOG}.{SCHEMA}.gold_funnel"
GOLD_REVIEW_SET_TABLE = f"{CATALOG}.{SCHEMA}.gold_review_set"
GOLD_CELL_SUMMARY_TABLE = f"{CATALOG}.{SCHEMA}.gold_cell_summary"
GOLD_LIST_SUMMARY_TABLE = f"{CATALOG}.{SCHEMA}.gold_list_summary"
GOLD_PRIORITY_CURVE_TABLE = f"{CATALOG}.{SCHEMA}.gold_priority_curve"
GOLD_KPIS_TABLE = f"{CATALOG}.{SCHEMA}.gold_kpis"
CHECKS_TABLE =f"{CATALOG}.{SCHEMA}.check_results"

VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# ---------------------------------------------------------------------------------

# COMMAND ----------

# Create what is missing. IF NOT EXISTS makes this idempotent.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

# COMMAND ----------

# Verify, and fail loudly if something is not there.
schemas = [r[0] for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()]
assert SCHEMA in schemas, f"Schema {CATALOG}.{SCHEMA} was not created. Schemas seen: {schemas}"

# SHOW VOLUMES puts the schema name first and the volume name last, so read the last column
volumes = [r[-1] for r in spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA}").collect()]
assert VOLUME in volumes, f"Volume {VOLUME} was not created. Volumes seen: {volumes}"

assert os.path.isdir(VOLUME_ROOT), f"{VOLUME_ROOT} is not visible from this compute"

print("OK")
print("Schema:", f"{CATALOG}.{SCHEMA}")
print("Volume path:", VOLUME_ROOT)
print("Lists:", len(LISTS), "| window:", START_MONTH, "to", END_MONTH)
print("Config:", CONFIG_PATH)
print("Config SHA-256:", CONFIG_SHA256)