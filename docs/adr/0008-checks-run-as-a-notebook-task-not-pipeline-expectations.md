# ADR 0008: Checks run as a notebook task, not as pipeline expectations

- Status: accepted
- Date: 2026-11-07

## Context
The Job needs a final Checks task that decides whether anything reaches the dashboard.
Three of the six checks are reconciliations across tables (1, 2, 3), one compares stages (4), two are row rules (5, 6).
Lakeflow Declarative Pipelines offer expectations: per-table rules that warn, drop rows or fail the update.
The four notebooks already exist and Gold ran on Databricks with its smoke checks passing.

## Decision
1. Checks stay plain Python and SQL in a notebook (`50_checks.py`), run as the fourth task of the Lakeflow Job after Gold.
2. Checks that guard one task's own work stay in that task's notebook (fail fast). The Checks task re-reads the stored tables, repeats Checks 2 and 3 from them, and adds Checks 5 and 6 as standalone checks.
   - Check 6 exists twice on purpose: the smoke version in Gold and the standalone one in Checks. A leak of addresses is the failure that matters most, so the project is defensive here and accepts the duplication.
   - The dashboard is a separate piece, built after the Job. It is not part of the checks layer, and the checks do not depend on it.
3. Any failed check raises, so the Job fails and the dashboard does not read.

## Why
- Reconciliations such as "parsed + quarantined = Bronze" compare tables. Expectations test rows inside one table, so each would need a hand-built summary table to hang a rule on.
- Moving to declarative pipelines would mean rewriting four working notebooks, including the per-snapshot `replaceWhere` reruns and the append-only Bronze, with the talk on 12 Nov.
- Expectations could do this job. All six checks can be expressed that way (the reconciliations through summary tables). The choice is not that it cannot be done, but that the talk should not introduce one more framework next to the Job, the medallion layers and the checks themselves.
- Plain code is easy to explain line by line.
- Nothing reaches the dashboard because the Job stops, which is the guarantee needed. It does not depend on the pipeline feature.

## Alternatives considered
- **Pipeline with expectations:** feasible. A natural fit for Checks 5 and 6 (row rules), workable but indirect for 1 to 4 (summary tables), and a full rewrite. Not tested on Free Edition. Deferred, not rejected.
- **Hybrid (pipeline for Silver and Gold, notebook for reconciliations):** two ways to run checks, more to explain, no gain for this size.
- **Checks only inside each notebook, no Checks task:** cannot see a partial Gold left by a failure halfway through its six writes.

## Consequences
- Repeating Checks 2, 3 and 6 means two copies to maintain. Drift between the copies (especially the address pattern and the list-id mask in Check 6) is the main risk; the plan is to define the pattern once in `00_setup_and_config.py` and use it in both places.
- Check logic is hand-written, so its blind spots are ours to state. Count-only checks pass on wrong rows of the right number.
- No built-in quality metrics or event log. Results are printed counts and the Job run status.
- Expectations are still a fair talk point: the managed alternative, and why it was not used here.
- Reimplementing the checks as pipeline expectations is backlogged for after the talk. Checks 5 and 6 are the first candidates.
