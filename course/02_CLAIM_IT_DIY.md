# 02 Claim it! DIY using pure Python and SQL

![Four pillars, as basic as health: Be a 50/50, Auditability saves you, Know your worth, Be the bridge](images/four_pillars.png)

**You will:** rebuild the core of module 01 with Python's standard library and SQLite, on your laptop, with no cloud, no Spark and no orchestrator.

**Why:** to prove to yourself that the talk was never about Databricks. The platform made the habits convenient. It did not make them. If you change jobs tomorrow and find a cron server and a shared drive, you can still build something auditable.

---

## The idea in one table

Every platform feature you used in module 01 is a convenience over something you can do by hand.


| Habit                                   | On Databricks (module 01)                         | With no fancy tools                                                  |
| --------------------------------------- | ------------------------------------------------- | -------------------------------------------------------------------- |
| Settings in one validated, hashed place | `config/pipeline.json` + `00_setup_and_config`    | A small `config.json` of its own, read at the top of your script, required keys checked, hashed |
| Land raw evidence, untouched            | Unity Catalog volume, one folder per `snapshot_id` | A folder per run on disk: `snapshots/<timestamp>/raw/`                |
| Know what arrived                       | `manifest.json` with SHA-256 per file             | The same, written by `hashlib`                                        |
| One row per message, append-only        | Delta table `bronze_messages`                      | A SQLite table, written once per snapshot                            |
| Nothing disappears                      | `silver_quarantine` with reason codes             | A `quarantine` table with a reason column                             |
| Flag duplicates, do not delete          | `ROW_NUMBER()` in Spark SQL                       | `ROW_NUMBER()` in SQLite (it has window functions)                   |
| Checks that stop the run                | `50_checks` raises, the Job turns red             | The script exits with code 1; `set -e` stops the next step           |
| Tasks with dependencies                 | Lakeflow Job                                      | A shell script, run top to bottom, stopping at the first failure     |
| A person between landing and processing | Copying `snapshot_id` into Job 2                   | One script here; split it after the Bronze check to put a person in between |
| Run history                             | The Job's run page                                | The snapshot folders themselves, plus a `check_results` table in each |

## Run it

The script is [diy_pipeline.py](diy_pipeline.py): about 145 lines, standard library only. It runs land, Bronze, Silver, dedup and four checks in one go. Do everything below in the `course/` folder, next to the script.

1. Put one or more `.mbox` files in a folder called `inbox/`. Any mbox works: export one from your own mail client, or download a monthly archive of a public mailing list.
2. Create `config.json` with the only two settings the script needs, where to write snapshots and the shortest body that gets a duplicate key:
   ```json
   {"snapshots_dir": "snapshots", "min_body_chars": 50}
   ```
3. Run, from the repo root:
   ```bash
   cd course
   python3 diy_pipeline.py --config config.json --inbox inbox
   echo "exit code: $?"
   ```

On a small synthetic test (two files, six messages, one HTML-only, three copies of the same text) it prints (your snapshot id will differ; the config hash will not, if your `config.json` is the same):

```
snapshot 20261118T101500123456Z | config b2b27db8e858 | files 2
bronze 6 | parsed 5 | quarantined 1 | duplicates 2 | unique 3
PASSED 1 bronze reassembles to every file hash
PASSED 2 parsed + quarantined = bronze
PASSED 3 primary + duplicates = parsed
PASSED 4 funnel never goes up
exit code: 0
```

## Read it as a checklist

Open the script and find each habit. Each one is a few lines.

**1. Config is data, not code.** `load_config` refuses to start if a key is missing, then hashes a canonical form of the file (sorted keys, no whitespace), so the same settings always give the same hash.

**2. Land, then look.** `land` copies the files before anything reads them, writes `manifest.json` with a hash per file, and saves the config and its hash in `params.json` next to the data. Months later, the folder answers "which settings made this?" without anyone's memory.

**3. Bronze can prove it lost nothing.** Check 1 joins every message of a file back together and compares the hash with the manifest. If the splitter had dropped a single byte, the hashes would differ.

**4. Every row ends up somewhere.** The parse loop never skips a bad row silently: its one `continue` comes only after the row is written to `quarantine`. A failure becomes a row in `quarantine` with a reason, which is what makes Check 2 possible.

**5. The key is a decision you can see.** The subject and body, `normalise` and `min_body_chars` are the whole definition of "the same message". Short bodies get no key, because "+1" and "thanks" are identical across unrelated mail.

**6. Failure is an exit code.** The script returns 1 if any check fails. That single number is what lets any scheduler, from cron to Airflow, stop the next step.

## Orchestrate it with nothing

```bash
#!/usr/bin/env bash
set -euo pipefail        # stop at the first failure, like a Job
python3 diy_pipeline.py --config config.json --inbox inbox
echo "all checks passed"     # only runs if every check passed
```

Schedule it with `cron`, or do not schedule it at all and run it by hand. The point is the order and the stop, not the scheduler.

## Where the DIY version runs out

Be honest about what the platform was buying you. You will feel these limits at scale, not at 20,000 messages.


| You lose                                        | When it starts to hurt                              |
| ----------------------------------------------- | --------------------------------------------------- |
| Parallel compute                                | Millions of messages, or heavy parsing              |
| Access control and secrets management           | More than one person, or any personal data          |
| Run history, retries, repair of one failed task | More than a handful of steps, or nightly runs       |
| Lineage across tables                           | When someone asks "which report used this file?"    |
| Time travel on tables                           | When you need to see a table as it was last Tuesday |

So the ladder is: by hand, then a script, then a script with checks and exit codes, then an orchestrator, then a platform. Climb a rung when a limit actually hurts, not before. Each rung keeps the same habits.

## Exercise

Take one notebook or script from your own work and answer, in writing:

1. Where do its settings live? Could you print a hash of them?
2. Does it keep the raw input exactly as it arrived? Could you prove it?
3. If a row fails, where does it go? Can you count it?
4. Which count on one side of a step should equal a count on the other side?
5. Which step involves a judgement call? Is the decision written down anywhere?

In the next module, we will share a worksheet that turns these answers into an orchestrated pipeline.
