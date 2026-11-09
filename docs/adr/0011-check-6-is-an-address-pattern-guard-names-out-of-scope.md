# ADR 0011: Check 6 is an address-pattern guard; names are out of scope

- Status: accepted
- Date: 2026-11-08

## Context
Check 6 asserts that Gold holds no raw addresses. It scans every string column for address-shaped text, after masking the public list ids, and rejects a fixed set of forbidden columns (ADR 0005).
Two copies of the pattern are planned: the smoke check in `40_gold.py` and the standalone check in the Checks task (ADR 0008).
The question was how far the guard should go. A name is also personal data, and a pattern cannot tell a name from other text.

## Decision
1. Check 6 looks for address-shaped text and forbidden columns only. It does not look for names.
2. The address pattern (`EMAIL_PATTERN`) and the list-id mask (`LIST_ID_MASK`) are defined once in `00_setup_and_config.py`. Both copies of Check 6 use them.
3. Names are out of scope. Whether a name in Gold is acceptable is not decided here.
4. This is a guard rail, not proof of anonymity. The sender is a salted pseudonym, described as pseudonymised, never anonymised.

## Why
- A pattern for addresses is simple, testable and explainable line by line. Detecting names needs a different tool and a policy on what counts, which is a project of its own.
- One definition means the two copies of Check 6 cannot drift apart.
- The limit is stated openly: the check can pass while a name sits in a column it does not read.

## Alternatives considered
- **Name detection (lists or a model):** wide scope, false positives and negatives, and a policy decision the project cannot make.
- **Keep the part before the @ instead of a pseudonym:** that part is often a name, so it would put the risk in Gold.
- **Drop the sender field:** Check 6 would pass trivially, and the distinct-sender counts would go (ADR 0005).
- **Two separate patterns:** simple, but they can drift.

## Consequences
- The talk says the trade-off out loud: the guard checks one shape of personal data and not others.
- Gold tables carry no subject, body, sender value or Message-ID, which is the main protection; the pattern only backs it up.
- Not tested on Databricks: the move of the constants. The pattern text is unchanged.
