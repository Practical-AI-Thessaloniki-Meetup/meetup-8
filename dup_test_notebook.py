# Databricks notebook source
# MAGIC %md
# MAGIC # Duplicate test (S4): counts only, no message text
# MAGIC
# MAGIC Reads the mbox files from one snapshot and answers one question: are there duplicates worth building a story on?
# MAGIC Prints counts only. Paste the printed summary back; it contains no message content.

# COMMAND ----------

# MAGIC %run ./00_setup_and_config

# COMMAND ----------

import glob
import hashlib
import mailbox
import os
import re
from collections import Counter, defaultdict

SNAPSHOT_ID = "20261101T210751Z"   # the folder name printed by the pull notebook

ROOT = f"{VOLUME_ROOT}/raw/{SNAPSHOT_ID}/mbox" 
files = sorted(glob.glob(f"{ROOT}/*.mbox"))
print("Files found:", len(files))

# COMMAND ----------

WS = re.compile(r"\s+")
QUOTED = re.compile(r"^\s*>.*$", re.M)


def body_text(msg):
    """Plain-text body as a string, or '' if none can be decoded."""
    parts = []
    if msg.is_multipart():
        for p in msg.walk():
            if p.get_content_type() == "text/plain":
                payload = p.get_payload(decode=True)
                if payload:
                    parts.append(payload.decode(p.get_content_charset() or "utf-8", errors="replace"))
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            parts.append(payload.decode(msg.get_content_charset() or "utf-8", errors="replace"))
    return "\n".join(parts)


def norm(text, strip_quotes=False):
    if strip_quotes:
        text = QUOTED.sub("", text)
    return WS.sub(" ", text).strip().lower()


def h(text):
    return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()


per_list = defaultdict(lambda: {
    "messages": 0, "no_msgid": 0, "no_body": 0, "errors": 0,
    "msgids": Counter(), "body_exact": Counter(), "body_noquote": Counter(),
})
msgid_lists = defaultdict(set)   # message-id -> lists it appears in

for path in files:
    lst = os.path.basename(path).rsplit("_", 1)[0]
    stats = per_list[lst]
    try:
        mbox = mailbox.mbox(path)
        for msg in mbox:
            stats["messages"] += 1
            mid = (msg.get("Message-ID") or "").strip()
            if mid:
                stats["msgids"][mid] += 1
                msgid_lists[mid].add(lst)
            else:
                stats["no_msgid"] += 1
            text = body_text(msg)
            if text.strip():
                stats["body_exact"][h(norm(text))] += 1
                stats["body_noquote"][h(norm(text, strip_quotes=True))] += 1
            else:
                stats["no_body"] += 1
    except Exception as e:  # keep going; count the failure
        stats["errors"] += 1
        print("ERROR reading", os.path.basename(path), type(e).__name__)


def extra_copies(counter):
    """Messages beyond the first for every key that repeats."""
    return sum(c - 1 for c in counter.values() if c > 1)

# COMMAND ----------

print(f"{'list':32} {'msgs':>6} {'dup by Message-ID':>18} {'dup by body':>12} {'dup body (no quotes)':>21} {'no msgid':>9} {'no body':>8}")
tot = Counter()
for lst, s in sorted(per_list.items()):
    d_id = extra_copies(s["msgids"])
    d_b = extra_copies(s["body_exact"])
    d_bq = extra_copies(s["body_noquote"])
    print(f"{lst:32} {s['messages']:>6} {d_id:>18} {d_b:>12} {d_bq:>21} {s['no_msgid']:>9} {s['no_body']:>8}")
    tot.update(messages=s["messages"], d_id=d_id, d_b=d_b, d_bq=d_bq)

cross = sum(1 for lists in msgid_lists.values() if len(lists) > 1)
print()
print("TOTAL messages:", tot["messages"])
print("Extra copies by Message-ID:", tot["d_id"])
print("Extra copies by exact normalised body:", tot["d_b"])
print("Extra copies by body with quoted lines stripped:", tot["d_bq"])
print("Message-IDs seen in more than one list (cross-posts):", cross)
if tot["messages"]:
    print(f"Rates: Message-ID {tot['d_id'] / tot['messages']:.1%} | body {tot['d_b'] / tot['messages']:.1%} | body no quotes {tot['d_bq'] / tot['messages']:.1%}")