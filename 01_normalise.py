# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Normalise (shared by 30_dedup and 31_dedup_compare)
# MAGIC
# MAGIC The one place where the text rule behind `dup_key` lives, so the dedup and the comparison report can never disagree (ADR 0003, ADR 0007).
# MAGIC It only defines functions and constants. It reads and writes no tables.
# MAGIC Run it after `%run ./00_setup_and_config`, because it needs the config constants.
# MAGIC
# MAGIC If `normalise()` changes: bump `NORMALISATION_VERSION` here and `dedup.normalisation_version` in `config/pipeline.json`, then re-run.
# MAGIC The assertion below stops the notebook if the two differ.

# COMMAND ----------

import hashlib
import re
import unicodedata

NORMALISATION_VERSION = "norm_v1"   # bump when normalise() changes: old and new keys must never be compared
SEPARATOR = "\x1f"   # whitespace to Python, so normalise() collapses it and it can never occur inside the text

# The config hash covers the version string; this check makes sure the code agrees with it.
CONFIG_NORMALISATION_VERSION = CONFIG["dedup"]["normalisation_version"]
assert CONFIG_NORMALISATION_VERSION == NORMALISATION_VERSION, (
    f"normalisation version differs: config says {CONFIG_NORMALISATION_VERSION}, code says {NORMALISATION_VERSION}")


def normalise(text):
    """The only normalisation. No lowercasing, no "Re:" stripping, no quote removal (ADR 0003)."""
    text = unicodedata.normalize("NFC", text or "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = "\n".join(line.rstrip() for line in text.split("\n"))
    return re.sub(r"\s+", " ", text).strip()   # also folds the newlines into single spaces


def make_key(subject, body):
    """Returns (dup_key or None, short_body). The length rule applies to the normalised body."""
    clean_body = normalise(body)
    if len(clean_body) < DEDUP_MIN_BODY_CHARS:
        return None, True
    return hashlib.sha256((normalise(subject) + SEPARATOR + clean_body).encode("utf-8")).hexdigest(), False

print("Normalisation version:", NORMALISATION_VERSION, "(matches config)")
