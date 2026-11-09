# 01 End-to-end setup

**Status:** Draft. These steps match what ran on the author's workspace in November 2026.  If a step does not match what you see, the step is wrong, not you. 

**You will:** bring the repo into Databricks Free Edition, run the setup, build two Jobs, run them, and read what they print.
**Running time:** a few minutes per Job (Job 2 took 4 min 33 s on the author's run). Reading and building take longer.

Throughout, boxes marked **Observe** are the point of the course. Each one shows a habit from the outlined principles. Do not skip them to get to green faster.

---

## Step 0. Accounts and the repo

1. Sign up for **Databricks Free Edition** with any email. Free Edition is serverless only, allows outbound internet to a list of trusted domains, and is for non-commercial use.
2. In the workspace tab, open **Workspace**, then **Create > Git folder**, and paste the repo URL: `https://github.com/Practical-AI-Thessaloniki-Meetup/meetup-8`. This clones the notebooks and the config into your workspace. If the repo is private, Databricks asks you to link a GitHub account first (Settings > Linked accounts).

> **Observe** The code and configuration is versioned on Git. This is the 101 course of recovering as a Data Scientist.

## Step 1. Read the config before running anything

Open `config/pipeline.json`. Everything that is a setting lives here, and nowhere else:

- `lists` and `window`: which mailing lists and which months to pull
- `source.pause_seconds`: how long to wait between calls (be polite to the source)
- `dedup.min_body_chars`: bodies shorter than this are never treated as duplicates
- `bronze.exceptions`: gaps a person has reviewed and accepted
- `gold.keywords`, `gold.scope_months`, `gold.review_rates_docs_per_hour`: the judgement calls behind the report

> **Observe** Look at the `gold` section. Keywords, scope months and review rates are not facts about the data. They are decisions, and they sit in config so anyone can see and change them.
>
> We want to use our data to drive decisions. But we cannot drive decisions trust worthily if

## Step 2. Run `00_setup_and_config`

Open the notebook and run all cells. It validates the config, prints a SHA-256 hash of it, and creates the schema and the volume if they do not exist.

> **Observe** Note the config hash it prints. Now break the config on purpose: set `"pause_seconds": 0.1` and run again. The notebook stops with a message that names the exact setting, before any data moves. Put the value back.
>
> This is an API, not a static CSV. That means we need to version each snapshot, and that we need to be polite to it! 
>
> Run the notebook a second time with no changes. Nothing breaks and nothing is duplicated. Every step in this project is safe to re-run (`IF NOT EXISTS`, files that exist are skipped).

## Step 3. Create the salt (one-off)

Gold replaces each claimed sender with a pseudonym: an HMAC-SHA256 of the address with a secret key (the salt). The salt never goes in the repo. Create it once, in a scratch notebook that you do not commit:

```python
import secrets
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.secrets.create_scope(scope="email_forensics")
w.secrets.put_secret(scope="email_forensics", key="pseudonym_salt",
                     string_value=secrets.token_urlsafe(32))
print("Salt created (value not printed)")
```

The scope and key names must match `gold.salt_secret_scope` and `gold.salt_secret_key` in the config.

> **Observe** The config holds the *names* of the secret, never the value. Gold refuses to run if the salt is missing or shorter than 16 characters, and it reports only the error type.

## Step 4. Build Job 1: land and verify

In **Jobs & Pipelines**, create a Job with two notebook tasks on serverless compute:


| Task key | Notebook        | Depends on | Parameters                                   |
| -------- | --------------- | ---------- | -------------------------------------------- |
| `pull`   | `pull_notebook` | none       | `snapshot_id` = empty (makes a new snapshot) |
| `bronze` | `10_bronze`     | `pull`     | none                                         |

The task key must be exactly `pull`. The pull publishes the new `snapshot_id` as a task value, and Bronze reads it from the task with that name.

Run the Job.

> **Observe**: Open the volume and find your snapshot folder. You will see:
>
> - `pull_params.json`: what was asked for, including the full config and its hash
> - `manifest.json`: what actually landed, one row per file with its SHA-256
> - `stats/`: what the source *says* it has, per month
> - `mbox/`: the raw files, untouched
>
> "What arrived" is now a fact on disk, not a memory. Auditable. This prevents unecessary war stories.

> **Observe:** Read the Bronze printout. Three independent counts are compared for every file: the source's own count, the number of `From ` separator lines in the file, and the rows Bronze wrote. Check 1 passes only if no file has fewer rows than the source says it should.

Copy the `snapshot_id` from the printout. Your counts will differ from the author's (about 21,600 messages across 60 files), because the archive keeps changing. That is exactly why every run is tied to a `snapshot_id`.

### If Check 1 fails

This happened on the my first snapshot: One file was 2 messages short of the source count, cause unknown. The fix is **not** to re-pull until it goes green. A person looks at the gap, decides whether it is acceptable, and records the decision in config:

```json
"bronze": {
  "exceptions": [
    {"file": "<file name>", "accepted_shortfall": 2, "reason": "<what you found and why you accept it>"}
  ]
}
```

Commit it, pull the change into your Git folder, and **repair** the run so only `bronze` re-runs.

> **Observe:** The machine found the gap. Only a person can decide whether it matters. The decision is named, bounded (2, not "some") and in git, and Bronze prints it on every run.

## Step 5. The human gate

Job 1 does not start Job 2. You do. Job 1 green means a person has accepted the evidence. Copying the `snapshot_id` by hand is the gate.

> **Observe** A Job can only pass or fail. Somebody has to decide where a person must look before the next step starts. This project puts that person between landing the evidence and deriving anything from it.

## Step 6. Build Job 2: process and check

Create a second Job with a **Job parameter** `snapshot_id`, and four tasks that each pass it on as `{{job.parameters.snapshot_id}}`:


| Task key | Notebook    | Depends on | Parameters                         |
| -------- | ----------- | ---------- | ---------------------------------- |
| `silver` | `20_silver` | none       | `snapshot_id`, `dry_run` = `false` |
| `dedup`  | `30_dedup`  | `silver`   | `snapshot_id`                      |
| `gold`   | `40_gold`   | `dedup`    | `snapshot_id`                      |
| `checks` | `50_checks` | `gold`     | `snapshot_id`                      |

Run it with the `snapshot_id` you copied.

> **Observe** In the `silver` output, find the quarantine counts. On the author's snapshot, 30 of 21,634 messages could not be parsed, mostly HTML-only messages with no readable text. They were not dropped: they sit in `silver_quarantine` with a reason code (`no_text_plain`, `empty_text_plain` or `parse_error`), and parsed + quarantined = Bronze (Check 2).

> **Observe.** In the `dedup` output, see how many rows were flagged as duplicates (65 on the author's snapshot). They were flagged, not deleted. Every copy and every location is still there, so the definition of "duplicate" can change later without losing anything.

> **Observe.** Query the results of the last task:
>
> ```sql
> SELECT check_name, passed, detail
> FROM workspace.email_forensics.check_results
> WHERE snapshot_id = '<your snapshot_id>'
> ORDER BY check_name;
> ```
>
> Six rows, six passes. Only now is Gold safe to read.

## Step 7. Make it fail on purpose

Run Job 2 again with `snapshot_id` **empty**. The `silver` task stops at once with "snapshot_id is empty inside a Job", and nothing after it runs.

> **Observe.** A pipeline that guesses ("use the newest snapshot") would have gone green on the wrong data. This one refuses to guess inside a Job. Failing loudly and early is a feature.

## Step 8 (optional). Compare the two ideas of "the same message"

Run `31_dedup_compare` by hand. It is not part of either Job. It compares two definitions of a duplicate: same Message-ID header, and same content hash.

> **Observe.** On the author's snapshot the two disagree. 25 copies are caught by both, 40 only by content, 3 only by Message-ID, and in a few cases the same Message-ID joins different text. A key everyone trusts is wrong some of the time. Both rates are reported side by side and never mixed.

## What you built

```
config -> pull -> volume (raw files, manifest) -> Bronze (Check 1)
        -> a person accepts the evidence
        -> Silver parse (+ quarantine) -> Dedup -> Gold -> Checks (six)
```

See [architecture.md](../docs/workshop/architecture.md) for the full diagram.

## Troubleshooting


| You see                                                      | Likely cause                                            |
| ------------------------------------------------------------ | ------------------------------------------------------- |
| `config: '...'` error in setup                               | A setting failed validation. The message names it       |
| Bronze: "No snapshot_id ... Check the task is named pull"    | The first task of Job 1 has a different key             |
| Silver: "dry_run is true inside a Job"                       | Set the`dry_run` task parameter to `false`              |
| Gold: "Could not read the salt secret"                       | Step 3 not done, or names differ from the config        |
| `pull` fails on the config hash when pinning an old snapshot | Intended: a snapshot is tied to the config that made it |

## Next steps

You have run the pipeline on a platform that did a lot of work for you. The deep dives (coming after the talk) look at which parts were the platform and which parts were the habits.
