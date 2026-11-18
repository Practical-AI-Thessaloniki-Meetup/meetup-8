"""The same habits with no platform: Python standard library and SQLite only.

Usage:  python3 diy_pipeline.py --config config.json --inbox inbox
Exit code 0 = all checks passed; 1 = a check failed, so `set -e` or cron sees the failure.
Prints counts only, never message content.
"""
import argparse
import email
import hashlib
import json
import shutil
import sqlite3
import sys
import unicodedata
import re
from datetime import datetime, timezone
from email import policy
from pathlib import Path


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def load_config(path):
    """Habit 1: settings live in one file, are validated early, and are hashed."""
    config = json.loads(Path(path).read_text(encoding="utf-8"))
    for key in ("snapshots_dir", "min_body_chars"):
        if key not in config:
            raise SystemExit(f"config: missing '{key}'")
    canonical = json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
    return config, sha256(canonical)


def land(inbox, snap_dir, config, config_hash):
    """Habit 2: copy raw files untouched, write a manifest, save the config with the data."""
    raw_dir = snap_dir / "raw"
    raw_dir.mkdir(parents=True)
    manifest = []
    for src in sorted(Path(inbox).glob("*.mbox")):
        shutil.copyfile(src, raw_dir / src.name)
        data = (raw_dir / src.name).read_bytes()
        manifest.append({"file": src.name, "bytes": len(data), "sha256": sha256(data)})
    (snap_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    (snap_dir / "params.json").write_text(json.dumps({"config": config, "config_sha256": config_hash}, indent=2))
    return manifest


def split_mbox(data: bytes):
    """A message starts at 'From ' at the top of the file or after a blank line. No byte is lost."""
    parts, start, pos, prev_blank = [], 0, 0, True
    while pos < len(data):
        end = data.find(b"\n", pos)
        end = len(data) if end == -1 else end + 1
        line = data[pos:end]
        if line.startswith(b"From ") and prev_blank and pos > start:
            parts.append(data[start:pos])
            start = pos
        prev_blank = line.strip() == b""
        pos = end
    if start < len(data):
        parts.append(data[start:])
    return parts


def body_text(msg):
    """Return the text/plain body, or (None, reason) so a failure becomes a reason, not a skipped row."""
    part = msg.get_body(preferencelist=("plain",))
    if part is None:
        return None, "no_text_plain"
    text = part.get_content()
    return (text, None) if text.strip() else (None, "empty_text_plain")


def normalise(text):
    text = unicodedata.normalize("NFC", text or "")
    return re.sub(r"\s+", " ", text).strip()


def main(config_path, inbox):
    config, config_hash = load_config(config_path)
    snapshot_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    snap_dir = Path(config["snapshots_dir"]) / snapshot_id
    manifest = land(inbox, snap_dir, config, config_hash)

    db = sqlite3.connect(snap_dir / "pipeline.db")
    db.execute("CREATE TABLE bronze (file TEXT, msg_index INT, raw BLOB, sha256 TEXT)")
    db.execute("CREATE TABLE silver (file TEXT, msg_index INT, subject TEXT, dup_key TEXT)")
    db.execute("CREATE TABLE quarantine (file TEXT, msg_index INT, reason TEXT)")

    # Bronze: one row per message, raw bytes and a hash. Never edited after this.
    for m in manifest:
        data = (snap_dir / "raw" / m["file"]).read_bytes()
        for i, part in enumerate(split_mbox(data)):
            db.execute("INSERT INTO bronze VALUES (?, ?, ?, ?)", (m["file"], i, part, sha256(part)))

    # Silver: parse, or quarantine with a reason. Every Bronze row lands in exactly one table.
    for file, i, raw in db.execute("SELECT file, msg_index, raw FROM bronze").fetchall():
        try:
            msg = email.message_from_bytes(raw, policy=policy.default)
            body, reason = body_text(msg)
        except Exception as e:
            body, reason = None, f"parse_error:{type(e).__name__}"
        if reason:
            db.execute("INSERT INTO quarantine VALUES (?, ?, ?)", (file, i, reason))
            continue
        clean = normalise(body)
        key = sha256((normalise(msg["subject"]) + "\x1f" + clean).encode()) if len(clean) >= config["min_body_chars"] else None
        db.execute("INSERT INTO silver VALUES (?, ?, ?, ?)", (file, i, msg["subject"], key))

    # Dedup: flag, never delete. ROW_NUMBER picks one primary per key, in a fixed order.
    db.execute("""CREATE TABLE dedup AS
        SELECT file, msg_index, dup_key,
               CASE WHEN dup_key IS NOT NULL AND ROW_NUMBER() OVER (
                    PARTITION BY dup_key ORDER BY file, msg_index) > 1 THEN 1 ELSE 0 END AS is_duplicate
        FROM silver""")

    # Checks: reconciliations that fail the run.
    count = lambda sql: db.execute(sql).fetchone()[0]
    bronze, parsed, quarantined = count("SELECT COUNT(*) FROM bronze"), count("SELECT COUNT(*) FROM silver"), count("SELECT COUNT(*) FROM quarantine")
    duplicates = count("SELECT SUM(is_duplicate) FROM dedup") or 0
    unique = parsed - duplicates
    reassembled = all(
        sha256(b"".join(r[0] for r in db.execute("SELECT raw FROM bronze WHERE file = ? ORDER BY msg_index", (m["file"],)))) == m["sha256"]
        for m in manifest)
    checks = {
        "1 bronze reassembles to every file hash": reassembled,
        "2 parsed + quarantined = bronze": parsed + quarantined == bronze,
        "3 primary + duplicates = parsed": unique + duplicates == parsed,
        "4 funnel never goes up": bronze >= parsed >= unique,
    }
    print(f"snapshot {snapshot_id} | config {config_hash[:12]} | files {len(manifest)}")
    print(f"bronze {bronze} | parsed {parsed} | quarantined {quarantined} | duplicates {duplicates} | unique {unique}")
    for name, ok in checks.items():
        print("PASSED" if ok else "FAILED", name)
    db.execute("CREATE TABLE check_results (name TEXT, passed INT)")
    db.executemany("INSERT INTO check_results VALUES (?, ?)", [(n, int(ok)) for n, ok in checks.items()])
    db.commit()
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Land, Bronze, Silver, dedup and checks with no platform.")
    parser.add_argument("--config", required=True, help="path to the settings JSON file")
    parser.add_argument("--inbox", required=True, help="folder of .mbox files to land")
    args = parser.parse_args()
    sys.exit(main(args.config, args.inbox))
