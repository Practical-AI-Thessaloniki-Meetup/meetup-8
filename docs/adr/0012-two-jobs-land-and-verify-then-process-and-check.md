# ADR 0012: Two Jobs: land and verify, then process and check, with a person between them

- Status: accepted
- Date: 2026-11-08
- Supersedes: 0010. Supersedes in part: 0006 (the pull now runs as a task of Job 1, together with Bronze).

## Context
ADR 0010 set one Job with five tasks starting at Bronze, and ADR 0006 kept the pull outside the Job.
ADR 0009 makes Bronze fail when a file is below its source count, unless a recorded exception covers it. An exception is a judgement that a gap is acceptable, so a person must make it, not the code.
A single Job has no natural place for that judgement: it either runs through or fails, and the next step starts automatically.

## Decision
1. **Job 1, land and verify:** two tasks, pull then Bronze (Check 1 included).
2. **Job 2, process and check:** four tasks, Silver parse, Dedup, Gold, Checks. `31_dedup_compare.py` stays outside both.
3. **Job 1 green means a person accepted the evidence.** If Bronze fails, a person reviews the file, adds an exception to `config/pipeline.json` (ADR 0009), and repairs the run so that only Bronze re-runs. Exceptions are never created by code.
4. **Job 2 is started by a person** with `snapshot_id` set, copied from the Bronze printout. Task values are not needed in Job 2: every task reads the parameter.
5. In a Job, an empty `snapshot_id` in Silver, Dedup or Gold raises, so no task picks its own default. (ADR 0014.)
6. The Checks task still repeats the row-count Check 1 (ADR 0008), so a snapshot that never passed Bronze is caught at the end even if someone starts Job 2 on it.

## Why
- The human decision sits where the doubt is: at the evidence, before anything is derived from it.
- Landing and processing are separate stages with different failure meanings, which is easy to show in the talk.
- Job 2 needs no cross-task state, so it is simpler than the five-task design.

## Alternatives considered
- **One Job, five tasks (ADR 0010):** needs no second Job, but the person's decision is a failed run followed by a manual re-run, with nothing marking the boundary.
- **Pull outside both Jobs (ADR 0006):** keeps the Job off the network, but loses a single run that shows "landed and verified".
- **Exceptions written by code:** defeats the check, since the code would approve its own gaps.

## Consequences
- Untested on Free Edition: the pull as a Job task (trusted-domain outbound), a repair run that re-runs only the failed task, and a second Job. If the pull cannot run as a task, Job 1 is Bronze only and the pull is run by hand.
- How Job 1 pins an existing snapshot is settled in ADR 0014.
- The gate is a convention, not a lock. The only enforcement behind it is point 6.
- `10_bronze.py` still sets a task value; it is harmless and unused by Job 2.
