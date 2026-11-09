# ADR 0001: Settings live in a validated JSON file, and each snapshot records its hash

- Status: accepted
- Date: 2026-11-01

## Context
Catalog, schema, volume, source URL, User-Agent, pause, lists and the month window were hardcoded in
`00_setup_and_config.py` and `pull_notebook.py`. Changing a setting meant editing code, and a snapshot
did not record which settings produced it. Resuming a half-finished pull after a settings change would
silently mix two different pulls in one snapshot.

## Decision
1. All settings live in `config/pipeline.json`. Nothing else holds names or settings.
2. `00_setup_and_config.py` loads the file, runs `validate_config()` and fails with a clear `ValueError`
   on a bad setting (missing key, wrong type, non-https URL, pause below 0.5 s, empty lists, bad or reversed months).
3. `canonical_sha256()` hashes the parsed settings (sorted keys, compact separators), not the file bytes,
   so spacing and key order do not change the hash.
4. `pull_notebook.py` writes the full config and its hash into `pull_params.json`.
5. Resuming an existing snapshot compares the stored hash with the current one and stops on a mismatch.
   The fix is to start a new snapshot.

## Why
- **Auditability (talk point 1):** a snapshot says exactly which settings made it, and the run can be repeated.
- **One place to change:** a rename or a new list is a data edit, not a code edit.
- **Fail early:** a typo stops the run before any call to the Apache server, not halfway through a pull.
- **Politeness is enforced:** the 0.5 s floor on `pause_seconds` is a rule in code, not a habit.
- **No mixed snapshots:** the hash guard protects the "pull once and cache by `snapshot_id`" rule.

## Alternatives considered
- **Keep constants in the notebook:** simplest, but no record on the snapshot and every change touches code.
- **Databricks widgets or Job parameters:** fine for run-time choices, but they are not versioned with the repo.
  Only `SNAPSHOT_ID` stays a per-run notebook variable.
- **YAML or TOML:** nicer to read, but YAML needs a dependency (project rule: ask first) and JSON is in the standard library.
- **Hash the file bytes:** easier, but a harmless reformat would invalidate every snapshot.

## Consequences
- `config/` must be synced to the workspace next to the notebooks (for example, a Git folder), or `find_config_path()` fails.
- A snapshot made before this change has no hash, so resuming it is refused.
- Changing any setting, even the pause, means a new snapshot. That is intended, and it costs a re-pull.
- Not yet verified on Databricks: the dbutils notebook-path lookup, and `%run` exposing the names.
