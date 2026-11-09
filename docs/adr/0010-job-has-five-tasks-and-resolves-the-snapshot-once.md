# ADR 0010: The pipeline Job has five tasks and resolves the snapshot once

- Status: superseded by 0012
- Date: 2026-11-07

## Context
The first plan was one Job with four sequential tasks: Bronze, Silver, Gold, Checks.
Silver is three notebooks today: `20_silver.py` (parse), `30_dedup.py` and `31_dedup_compare.py` (report only).
Parsing is the expensive, stable step. Dedup is cheap and replaces only its own snapshot.
`snapshot_id` reaches the notebooks differently: `10_bronze.py` hardcodes it, the others take a parameter and default to the newest snapshot in Bronze. ADR 0006 already says the Job takes `snapshot_id` as a parameter.

## Decision
1. Five sequential tasks: Bronze, Silver parse (`20`), Dedup (`30`), Gold (`40`), Checks.
2. `31_dedup_compare.py` stays outside the Job. It is report only, second in the cut order, and nothing downstream reads it. It is run by hand.
3. `snapshot_id` is an optional Job parameter. If given, every task uses it. If not given, the Job uses the newest snapshot.
4. "Newest" is resolved once, in Bronze, and handed to the later tasks as a task value, so no task picks its own default. Bronze prints the resolved id; every task prints the id it uses at the start.

## Why
- Splitting parse from dedup means a failure in dedup does not force the expensive parse to run again. Whether a repair run on Free Edition re-runs only the failed task and what follows is untested.
- Keeping 31 out keeps the Job to what the dashboard needs and leaves the cut order intact.
- An optional parameter serves both uses: pin a snapshot for the demo or a re-run, or run with no input and take the latest.
- A default of "newest in Bronze" evaluated separately in each task could pick different snapshots in one run, for example if Bronze fails after a partial write or a pull lands between tasks. Resolving once removes that.

## Alternatives considered
- **Four tasks with Silver as one (20, 30, 31):** matches the first plan, but a dedup failure reruns the parse.
- **31 as a sixth task:** one more thing to run and explain for a report nobody downstream reads.
- **Parameter required, no default:** safest, but every run needs a value typed by hand.
- **Each task resolves its own default:** simplest, but allows different snapshots within one run.

## Consequences
- `10_bronze.py` must stop hardcoding `SNAPSHOT_ID` and read the parameter.
- `20_silver.py` defaults `dry_run` to true, so in the Job it must be passed as false or guarded.
- Task values and a second Job on Free Edition are untested; the first run will show whether they work. If task values do not, the fallback is a required parameter.
- Max 5 concurrent tasks is not a problem: the tasks run in sequence.
