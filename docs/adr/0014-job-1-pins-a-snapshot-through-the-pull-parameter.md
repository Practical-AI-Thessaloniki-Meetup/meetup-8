# ADR 0014: Job 1 pins a snapshot through the pull's snapshot_id parameter

- Status: accepted
- Date: 2026-11-08

## Context
ADR 0012 left open how Job 1 works on an existing snapshot, because the pull notebook hardcoded `SNAPSHOT_ID = None` and so always made a new one.
Bronze already takes a `snapshot_id` parameter. After a fresh pull Bronze must load the snapshot that pull made, not guess.
The pull refuses to resume a snapshot whose `config_sha256` differs from the current config (ADR 0001). The hash covers the whole config, so any settings change, even a Gold keyword, blocks a resume.

## Decision
1. The pull takes a `snapshot_id` widget. Empty makes a new snapshot. A given id resumes that snapshot and skips files already there.
2. The pull sets a task value `snapshot_id`. Bronze uses, in order: its own parameter, the pull task's value (task key must be `pull`), and the newest snapshot only when run by hand. Inside a Job, if neither the parameter nor the value is found, Bronze raises.
3. Silver, Dedup and Gold raise when `snapshot_id` is empty inside a Job (ADR 0012, point 5). Checks already did.
4. To work on an existing snapshot, set `snapshot_id` on Job 1. If the config hash has changed since the pull, the pull task fails by design; the person then starts Job 1 with the pull task skipped or runs Bronze by hand.

## Why
- One parameter name in every notebook keeps the Jobs easy to explain.
- Bronze reading the pull's value means a new snapshot is resolved once, at its creation, not guessed twice.
- Keeping the hash check protects the rule that one snapshot comes from one set of pull settings.

## Alternatives considered
- **Bronze takes the newest snapshot after the pull:** no task value needed, but two tasks each pick a default, which ADR 0010 ruled out.
- **Hash only the pull-relevant settings (source, window, lists):** would let a cached snapshot resume after unrelated config changes, but changes what ADR 0001 hashes. Not done; revisit if the cached demo needs it.
- **Pull outside the Jobs:** ADR 0012 rejected this.

## Consequences
- Task values and a pinned pull in a Job are untested on Free Edition. If task values fail, pass `snapshot_id` to Bronze by hand.
- A cached-snapshot demo run through the pull task fails on the changed config hash. The demo runs Bronze alone on that snapshot, or the hash scope is narrowed in a later ADR.
