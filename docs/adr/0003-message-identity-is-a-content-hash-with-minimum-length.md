# ADR 0003: Message identity is a content hash (dup_key), applied only to bodies of at least N characters

- Status: accepted
- Date: 2026-11-02

## Context
Stage N must decide when two parsed messages are the same message. The same mail can sit in several lists or months.
Message-ID is unreliable as the only key and is compared separately (task N3). Very short bodies ("thanks", "+1") are
identical across unrelated messages.

## Decision
1. `dup_key` = SHA-256 of normalised subject + `\x1f` + normalised body.
2. Normalisation (`norm_v1`): Unicode NFC, line endings to `\n`, trailing whitespace stripped per line, whitespace runs
   collapsed to one space, ends stripped. No lowercasing, no "Re:" or list-tag stripping, no removal of quoted lines.
3. The key is built only when the normalised body has at least `dedup.min_body_chars` characters (config, 50).
   Shorter rows get `dup_key = NULL` and `short_body = true`, are never grouped and are always primary.
4. Duplicates are flagged in a new table `silver_dedup`, never deleted. Primary = first by parsed date (NULLs last), list, month, `msg_index`.
5. The setting is part of the config hash. Adding it changed the hash from `5006efb6...` to `51965af9...`.
   Bronze and Silver ran under the old one, Stage N runs under the new one.
   `pull_notebook.py` refuses to resume a snapshot whose saved hash differs; accepted because the snapshot is complete.

## Why
- Exact content identity is explainable line by line and cannot merge messages that merely look alike.
- A minimum length avoids grouping unrelated short replies, which would overstate duplicates.
- Keeping "Re:" and quotes means a reply is not the same message as its parent.

## Alternatives considered
- **Message-ID as the key:** reported separately (N3), not mixed with the content rate.
- **Strip quoted lines before hashing:** could merge replies with different new text. Rejected.
- **Lowercase and strip "Re:":** merges different messages. Rejected.
- **No minimum length:** short boilerplate bodies would form huge false groups.
- **Hash only the config sections the pull uses:** keeps resume working but changes the hash definition. Not needed now.

## Consequences
- Duplicate counts depend on N; a sensitivity analysis is backlogged.
- Changing `normalise()` requires a new version string and a re-run.
- Gold must filter `is_duplicate = false` for unique volume.
