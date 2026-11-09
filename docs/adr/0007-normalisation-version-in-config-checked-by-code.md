# ADR 0007: The normalisation version is named in config and checked by the code

- Status: accepted
- Date: 2026-11-06

## Context
`dup_key` is only comparable between rows built with the same `normalise()` rule (ADR 0003).
Today the version string, `NORMALISATION_VERSION = "norm_v1"`, is a constant in `30_dedup.py` next to the function.
It is stored on every `silver_dedup` row, but the config hash does not cover it.
The question was whether normalisation should be configurable.

## Decision
1. `config/pipeline.json` gets `dedup.normalisation_version` (for example `"norm_v1"`). It is validated in `00_setup_and_config.py` and is part of the config hash.
2. The normalisation rules stay in code. They are not configurable.
3. `30_dedup.py` asserts that the version in config equals the version its own `normalise()` declares, and fails loudly if they differ.
4. Changing `normalise()` still means: new version string in code and in config, rerun.

## Why
- The config hash then changes whenever the version changes, so a run can be traced to its rule.
- The assertion closes the drift gap: a changed function with an old string, or the reverse, stops the task.
- Rules as flags would multiply the cases to test and explain.

## Alternatives considered
- **Version in code only (status quo):** simple, but the config hash does not cover it.
- **Rules as config flags (lowercase, strip "Re:", strip quotes):** flexible, but ADR 0003 rejected these rules because they merge different messages.
- **Version in config with no check in code:** function and string can drift apart unnoticed.

## Consequences
- The config hash changes once more. `pull_notebook.py` refuses to resume a snapshot whose saved hash differs. How that refusal behaves for the current snapshot is not yet checked.
- One new assert in `30_dedup.py` and one new validated key.
- Implemented 8 Nov (untested on Databricks): `dedup.normalisation_version` in config, and the assertion plus `normalise()` and `make_key()` in the shared notebook `01_normalise.py`, which `30_dedup.py` and `31_dedup_compare.py` both `%run`. The logic was tested locally on synthetic text.
