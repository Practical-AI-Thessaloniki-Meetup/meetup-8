# ADR 0004: Gold ranks list-and-month cells by the Wilson lower bound of the strong-hit share

- Status: accepted for the build
- Date: 2026-11-04

## Context
A review set sized only by keyword hits makes hours proportional to hits, so it does not say what to read first.
The scenario (fictional) is a consultancy whose engineer must read public list discussion for breaking changes,
deprecations and regressions before a platform or upgrade decision. Gold must say which cells (list x month) to read first.
There are no human relevance labels, so everything here ranks a keyword proxy.

## Decision
1. A message matching at least `gold.strong_hit_min_keywords` distinct keywords (2) is a strong hit.
2. Each cell's `priority_score` is the lower bound of the Wilson interval (`gold.wilson_z`, 1.96) for strong hits out of its review-set messages.
3. Cells rank by score descending; ties go to the smaller review set, then list, then month. A cell with no review set has no score and ranks last.
4. Gold writes the cumulative curve (strong hits against review hours) for priority and for chronological order, and the hours needed to capture 50% and 80% of strong hits (`gold.capture_targets`), whole cells at a time.
5. Stability: recompute the ranking with the threshold moved by one keyword; report Spearman and top-5 overlap.
6. The month of a cell is the month of the parsed date (UTC), not the source file month. A message with no parsed date is never in scope.
7. Adding the `gold` config section changes the config hash again; `pull_notebook.py` would refuse to resume the finished snapshot, which is accepted (same trade-off as ADR 0003).

## Why
- A small cell can top a raw-share ranking by luck. Wilson penalises small samples, so a cell with more evidence ranks first.
- The benefit is robustness, not a higher curve: on the data being ranked the raw share gives the highest curve by construction.
- The curve is the ROI claim: hours to reach a share of strong hits against the chronological baseline. A flat curve is a valid, reportable result.

## Alternatives considered
- **Raw hit rate:** degenerate (tiny cells win).
- **TF-IDF or model ranking:** no labels to show it works; out of scope.
- **Threading, near-duplicates, RAG:** need data Silver does not parse, or are hard to audit.
- **Hash only the sections each stage uses:** cleaner, but changes how every notebook stamps its tables; not now.

## Consequences
- The score assumes independent messages; threads break that, so it is a ranking device, not a 95% statement.
- Rates and cost per hour are assumptions in config. `cost_per_review_hour` is null until set, and cost columns stay NULL.
- Keyword choice drives everything; Gold prints a warning if one keyword or the review set dominates.
