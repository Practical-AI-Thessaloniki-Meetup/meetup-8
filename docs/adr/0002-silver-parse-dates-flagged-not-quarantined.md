# ADR 0002: Silver parse keeps date problems in the parsed table; quarantine is for unusable bodies only

- Status: accepted
- Date: 2026-11-02

## Context
Stage P parses each Bronze message. Some messages will have a missing, unparseable or out-of-window `Date`
header, and some will have no usable `text/plain` body. Check 2 needs every Bronze row in exactly one table,
and Check 5 (later) requires a date on every parsed row.

## Decision
1. A date problem never quarantines a row. The row stays in `silver_messages` with `date_parse_flag = 1` and
   `date_parse_reason` (`missing`, `unparseable`, `out_of_window`). Missing and unparseable dates give a NULL `date_utc`.
   An out-of-window date keeps its real value and is only flagged.
2. Quarantine reasons are `no_text_plain`, `empty_text_plain` and `parse_error` (catch-all, expected zero).
   `reason_detail` holds content types or an exception class, never text, subjects or addresses.
3. Two Delta tables, `silver_messages` and `silver_quarantine`. A row is routed by `reason_code`, so it cannot be in both.
4. Writes use `replaceWhere snapshot_id = ...`, so re-running a snapshot replaces its rows only.
5. The notebook defaults to `dry_run = true`: it parses, counts and runs Check 2 without writing, so the date counts are seen first.
6. No fallback date (for example from the mbox `From ` line) is invented.

## Why
- Auditability: a bad date is a fact about the message, not a reason to hide it from the funnel.
- A guessed date would look verified and is not.
- Check 5 must then accept a NULL date when `date_parse_flag = 1`.

## Alternatives considered
- **Quarantine on a bad date:** loses otherwise usable messages from the funnel.
- **Fallback date from the `From ` separator line:** a different, weaker evidence source; needs an explicit decision.
- **Append and delete-then-insert:** risks duplicates or a gap if a run stops half way.

## Consequences
- Check 5 must allow NULL `date_utc` when `date_parse_flag = 1`.
- Jobs must pass `dry_run = false` to write.
- The two table writes are separate, so a stop between them is caught by Check 2, not prevented.
