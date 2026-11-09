# ADR 0005: Sender pseudonyms are HMAC-SHA256 with a salt read from a secret

- Status: accepted
- Date: 2026-11-04

## Context
Gold needs a sender field (distinct senders per list) without raw addresses, so that Check 6 is exercised and not trivially true.
The sender is claimed, not verified. The salt must not be in the repo, config, any table or any printout.

## Decision
1. `sender_pseudonym` = HMAC-SHA256, key = salt, message = the lowercased, trimmed address parsed from `sender_claimed_value`
   (the whole trimmed value, lowercased, if no address can be parsed). A blank sender gets no pseudonym.
2. The salt is read with `dbutils.secrets.get(scope, key)`. Config holds the two names only (`gold.salt_secret_scope`, `gold.salt_secret_key`).
   The notebook fails if the secret cannot be read or is shorter than 16 characters, and reports only the error type.
3. Gold tables carry no subject, body, sender value, address or Message-ID. The smoke Check 6 scans every string column for address-shaped values,
   after masking the public list ids (which are address-shaped).
4. Described as pseudonymised, never anonymised.

## Why
- HMAC is the standard way to key a hash; a plain salted SHA-256 concatenation is easier to misuse.
- Case and whitespace normalisation make one address map to one pseudonym.
- A masked-list-id exemption keeps Check 6 strict for people while allowing the list column.

## Alternatives considered
- **Plain SHA-256 of salt + address:** works, weaker construction. The first plan said "salted SHA-256"; HMAC is the same idea done properly.
- **Salt in config or repo:** breaks the "salt outside the repo" rule.
- **No sender field in Gold:** Check 6 would pass trivially.

## Consequences
- Pseudonymised data is still personal data under GDPR.
- The same salt must be reused to compare pseudonyms across runs; changing it changes every pseudonym.
- Whether Free Edition supports secret scopes is untested; if not, this ADR needs a replacement mechanism.
