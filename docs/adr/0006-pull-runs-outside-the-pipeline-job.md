# ADR 0006: The pull is a separate step, not a task in the pipeline Job

- Status: superseded in part by 0012 (the pull is now a task of Job 1 with Bronze; the reasons for pulling once and caching still stand)
- Date: 2026-11-06

## Context
Stage C builds one Lakeflow Job with four sequential tasks: Bronze, Silver, Gold, Checks.
The pull notebook lands mbox files and a manifest in the volume, keyed by `snapshot_id`.
It is the only step that needs outbound internet and it calls the public source.
An earlier drawing showed config, pull and Bronze as three tasks, so the question was whether pull belongs in the Job.

## Decision
1. The pull runs on its own, by hand or as a separate small Job. It is not a task of the pipeline Job.
2. The pipeline Job starts at Bronze and reads a snapshot already in the volume.
3. The pipeline Job takes `snapshot_id` as a Job parameter, so every task uses the same snapshot.
4. Config is loaded with `%run ./00_setup_and_config` inside each notebook. It is not a Job task.
5. Backfills (new snapshots) are done through the pull step, separately from the pipeline.

## Why
- "Pull once and cache by `snapshot_id`" is already the design. A pull on every Job run goes against it.
- The source is polite-use only (descriptive User-Agent, `PAUSE_SECONDS`). Fewer live calls means less load on it.
- The pipeline Job then runs offline on a cached snapshot, which is what the demo needs.
- Landing and processing are two separate steps, which is easy to show in the talk.

## Alternatives considered
- **Pull in the Job behind a condition task and a `run_pull` parameter:** keeps one graph, but adds a task type, a parameter and a "run if" setting on Bronze that is easy to get wrong.
- **Pull in the Job that skips itself when the snapshot exists:** a skip looks like a success, and a live run still needs internet.

## Consequences
- Someone must run the pull before the pipeline. A missing snapshot has to fail loudly in Bronze, not pass quietly.
- `10_bronze.py` must take `snapshot_id` as a parameter instead of the hardcoded value.
- Whether Free Edition allows a second Job or a Run Job task is untested.
