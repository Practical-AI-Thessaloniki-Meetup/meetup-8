# ADR 0009: Bronze fails when a file has fewer rows than the source count, unless a recorded exception covers it

- Status: accepted
- Date: 2026-11-07

## Context
Check 1 proves that Bronze reassembles to the downloaded file. Both sides come from the same bytes, so a file that was cut short during download still passes.
The manifest records `stats_count` per file: the source's own message count, taken before any mail is downloaded. It is the only count in the manifest that does not come from the downloaded file.
`10_bronze.py` already prints files where Bronze rows differ from `stats_count`, but did not fail on it.
Measured on snapshot 20261101T210751Z: 59 of 60 files match the source count exactly. One file (list `dev`, month 2026-09) has 518 Bronze rows against a source count of 520, two short. Its Bronze rows equal the rough From-line count, so the splitter did not merge messages. The cause is not known: either the download is missing two messages, or the source count includes two items that are not in the mbox.

## Decision
1. Check 1 gains a second condition: for every usable file, Bronze rows must not be fewer than `stats_count`, unless a recorded exception covers the file. Otherwise the notebook raises and the Job stops before Silver.
2. Only a shortfall fails. Bronze rows above `stats_count` are printed, not failed, because a truncated file has fewer messages, not more.
3. Exceptions live in `config/pipeline.json` (validated in `00_setup_and_config.py`, covered by the config hash). Each entry has: the file name, the accepted shortfall, and a reason.
   - A listed file passes only while its shortfall is no more than the accepted number. A larger shortfall fails.
   - A file not on the list gets no tolerance.
4. Every run prints each exception it applied: file, shortfall, reason. An entry whose reason is "unexplained" therefore shows up in every log until someone resolves it.
5. The first entry is the 2026-09 file of list `dev`: shortfall 2, reason "unexplained".
6. The failure means "completeness cannot be proven", not "our processing is wrong". Messages print counts only, never content.
7. A missing `stats_count` for a usable file fails. It does not count as zero.

## Why
- It is the first point inside the Job that can catch a short file that came back with HTTP 200.
- A named, bounded exception accepts exactly one known gap. New loss, even in the same file, still stops the run.
- Putting the list in config means a change to it is traceable and visible, which is the auditability point.
- A doubtful step must not look like a success: the printout keeps the gap in view on every run.

## Alternatives considered
- **Print only (status quo):** a short snapshot would flow into Silver and Gold, and the funnel would quietly be smaller than the archive.
- **Fail on any shortfall with no exceptions:** the existing snapshot, already used for Gold, would fail on the 2026-09 file.
- **Tolerance as a percentage:** simpler, but a number chosen from one example is a guess, and it would let any file lose a few messages unnoticed.
- **Fail on any difference, including surplus:** a surplus does not point to truncation and could stop good runs.

## Consequences
- Not implemented yet: the config key, its validation, the exception logic and the printout are still to build in `00_setup_and_config.py` and `10_bronze.py`.
- The cause of the two missing messages is open. Review step (human, not built): re-fetch the 2026-09 file into a scratch location and compare size, hash and count. Then replace "unexplained" with the cause, or remove the entry if the file is fixed.
- A failing run means a human looks. There is no automatic repair.
- The pull is unchanged: it keeps its rough From-line count, which is not used for this check.
