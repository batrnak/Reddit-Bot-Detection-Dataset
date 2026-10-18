# AGENTS.md — Reddit Bot Detection (weakly labeled dataset)

Read this whole file before changing any code. It describes the project and the rules that must not be broken.

## 0. How to work with the owner

**Language: reply, review and explain in Vietnamese.** Keep technical terms in English (ingestion, feature, label model, split, etc.) instead of translating them. Source files are written in English identifiers; code comments and `print` log messages follow the repo's existing convention (Vietnamese). Do not rewrite existing comments or logs into another language.

**Review rules**
- Start with a verdict: does the code run, and does it produce correct results? Then list problems from most to least severe.
- Report only bugs that crash the script or make results wrong. Skip style issues and bugs that cannot happen. If something is minor, say it in one line or leave it out.
- For each reported bug give a concrete example of what goes wrong. If you cannot, do not report it.
- Say what you verified and what you only inferred. Never claim code "runs" unless you actually ran it. If you have not seen a file's real structure (for example a JSON schema), say so instead of guessing.
- Do not add tables, columns, steps or new concepts on your own. If you think one is needed, explain which concrete failure appears without it and wait for approval.
- Be direct and give realistic trade-offs. Push back when the owner's idea has a real problem, with the reason. Do not flatter.
- Prefer the smallest change that fixes the problem. When asked to "fix", return complete code the owner can paste.

## 1. Goal

Build the strongest possible weakly labeled dataset for detecting bots on Reddit, covering both traditional bots and new-generation LLM bots. A bot anywhere is a bad bot. Model direction: deep learning / neural networks that learn latent features. Hand-built features and rules (labeling functions, LF) are used to **create labels**. The owner wants a research direction good enough for a Q1 journal paper.

**Decided against:** hand labeling (the problem cannot be hand-labeled), simulating more bots with LLMs, Iceberg tables (not needed yet).

## 2. Data and label sources

| Source | Content | Notes |
|---|---|---|
| Arctic Shift dump | Posts and comments for 2026-06, 07, 08 from 18 subreddits | 15 politics/news subs plus BotBouncer, TheseFuckingAccounts, RedditBotHunters |
| BotBouncer (`bot_users`) | 345,381 rows (99.98% from BotBouncer; TFA 67, RBH 2) | See flair below |
| BotSim-24 | 2,907 simulated users: 1,907 humans + 1,000 LLM bots | **The last 1,000 rows of `Users.csv` are bots** (by row order, not independently verified). `user_post_comment.json` exists but its structure is unverified |

**`bot_users` flair distribution:** `banned` 92.4% (318,965), `pending` + `pending/H` 4.5%, null 3.0%, `purged` 0.09%, `organic` + `service` 0.02%, the rest about 0%.
- **Positive = `banned` only**, and only users with no `organic`/`service` row.
- **Negatives (comparison set) = exclude every user present in `bot_users`**, whatever the flair. `pending`, null and `purged` mean unknown, so they must never count as human.
- BotBouncer uses rule-based evaluators plus human review for about half of the accounts. According to early-2025 sources it targets traditional bots (karma farming, spam), and there is no evidence that it catches LLM bots. Its label means "matches a rule or a human reviewer thought it looked like a bot".
- Only about 0.9% of flagged users have any activity in the 15 political subs (about 3,217 users, counted before the flair filter). The number of `banned` users with enough data is the real size of the evaluation set, and it has not been counted yet.

## 3. Infrastructure and paths

- Bucket: `data-pipeline-datalake-537390611425`. Terraform: `terraform/glue.tf` (silver), `terraform/gold_schema.tf` (gold, 8 tables). Glue databases: `silver_arctic_shift`, `gold`. Queries run on Athena.
- Bronze: `bronze/arctic_shift/raw/{posts|comments}/year=YYYY/month=MM/{normal|botflag}.jsonl`. Comments only have `normal` (comments from the 3 bot subs were dropped).
- Silver: `silver/arctic_shift/{posts|comments|bot_users}/data/{kind}_{YYYY-MM}.parquet` (one file per month, fixed key, safe to overwrite).
- Gold: `gold/{table}/{table}.parquet` (one file per table, overwritten by `write_gold`). Backups go to `gold/_backups/`, outside every table prefix so Athena does not read them twice.
- Code layout: `scripts/ingest/` (crawl, split, load bronze), `scripts/silver/` (`common.py`, `build_*.py`), `scripts/gold/` (`common.py`, `build_*.py`). Run with `python -m scripts.gold.<name>` from the repo root.
- Stack: Python, polars (gold), pandas/pyarrow (silver), boto3, Terraform, Airflow. Dev machine: Windows (PowerShell), mini PC i3-8100, **8 GB RAM, no GPU**.

**Silver schema**
- `posts`: submission_id, author_name, title, selftext, score, num_comments, upvote_ratio, created_utc (timestamp us, UTC), subreddit, url, permalink, ingest_month
- `comments`: comment_id, author_name, body, score, link_id, parent_id, created_utc, subreddit, permalink, ingest_month. **There is no `url` column**; links must be found inside `body`.
- `bot_users`: username (lowercase), source_subreddit, source_post_id, flair, flagged_at, ingest_month
- `author_name` is already lowercase. `parent_id` keeps the `t1_`/`t3_` prefix while `comment_id` and `submission_id` do not (strip the first 3 characters when joining reply edges).

## 4. Gold schema (8 tables)

| Table | Key | Role | Status |
|---|---|---|---|
| `users` | user_id | Fixed node_idx, n_events, eligible (n_events >= 5), split train/val/test (70/15/15 by hash) | Built |
| `user_features` | user_id | 9 model features + `has_enough_data` + 4 `lf_*` columns (LF only) + `feature_cutoff_ts` | Built (`lf_coord_score` is 0 until the graph exists) |
| `eval_users` | user_id, eval_role | Evaluation set: `botbouncer_B` (half B, eval_label=1), `botsim_heldout` (humans and bots) | Built **with old code, must be rebuilt** (section 7) |
| `graph_edges` | src_idx, dst_idx | `coord_*` edges (label propagation) and `interact_*` edges (representation only), `weight_idf`, `first_ts`, `last_ts` | Not built |
| `botsim_users` | user_id (`bs_<name>`) | BotSim with true label `is_bot`, `split` train/heldout, 9 features with the same column names as `user_features` | Not built |
| `user_events` | user_id, ts | Event sequence for the neural network, cut at `feature_cutoff_ts` | Not built |
| `lf_votes` | (user_id, lf_name) | Long-format LF votes: -1 abstain / 0 human / 1 bot, `score`, `evidence` | Not built |
| `weak_labels` | user_id | `p_bot`, `label` (-1 = unsure), `n_lfs_voted`, `label_source` | Not built |

The 9 model features: `log_events, n_subs, hour_entropy, iat_median, iat_cv, burstiness, len_median, dup_ratio, link_ratio`.

## 5. Mandatory rules

**Identity and data**
- `user_id` = lowercase `author_name`. BotSim users get the `bs_` prefix. Always drop `[deleted]` and `automoderator`.
- `node_idx` is append-only and is **never reassigned**. New users get numbers after the max of the old table.
- Use one shared definition (same columns, same formulas) to compute features for Reddit and BotSim. Otherwise comparing the two domains is meaningless.

**Preventing label leakage**
- For flagged users, every feature and every graph edge uses only activity **before `flagged_at`** (`feature_cutoff_ts`).
- `lf_*` columns **never** go into the model.
- Flags are split into two halves with `stable_bucket(user_id, SALT_FLAG, 2)`. Half A (bucket 0) feeds the LF and seeds. Half B (bucket 1) is for evaluation only and gets **no vote from `lf_community_flag`**. Use the shared `flags_half_a/b` in `common.py` with the same input list.
- `eval_users` (half B, BotSim held-out) must not appear in any LF, seed or training set. BotSim is held out when `stable_bucket(user_id, "botsim_heldout_v1", 100) < 30`; training uses bucket >= 30 only. Use one shared function for this.
- Constants: `MIN_EVENTS = 5`, `SALT_SPLIT = "split_v1"`, `SALT_FLAG = "flag_v1"`. Each split uses its own salt.

**Labeling functions**
- At most one vote per (user, LF). The community-flag LF only votes 1 and abstains for everyone else. Not being flagged does not mean human.
- Threshold LFs only run on users with `has_enough_data` (n_events >= 5, counted after the `flagged_at` cut).
- **Do not** write LFs like "low activity means human" or "high activity and low duplication means human" (LLM bots are exactly that kind of user).
- LF thresholds are guesses. Tune them on half A only, never on half B. LFs that share a feature column are double counted by the label model, so record which LF uses which column.
- A GNN/embedding must **not** be an LF of the label model it helps train. Only `coord_*` edges propagate labels. Reply edges (`interact_*`) connect humans to bots, so they are for representation only.
- `p_bot` is for **ranking**, not a probability. Never call weak labels "ground truth".

**Evaluation**
- Two separate measurements, never merged: (1) BotSim held-out (simulated LLM bots, one generator), (2) `botbouncer_B` against ordinary users with the same activity level (traditional bots). Report AUC with a bootstrap CI and AUC inside each activity bin.
- There is currently no way to verify detection of new-generation LLM bots from an unseen generator. State this limitation.

**Technical**
- Parquet timestamps use microsecond resolution. Integers are written as `Int64` (Glue declares `bigint`); polars `UInt64` hashes must go through `reinterpret(signed=True)`. Never declare `int` in Glue.
- Polars regex (Rust) has no lookahead.
- Read silver month by month, never all at once (8 GB RAM). Download large S3 files to disk before streaming (reading directly from S3 has dropped connections before).
- Log with `print`. Set `$env:PYTHONIOENCODING="utf-8"` if the Windows terminal breaks Vietnamese text.
- Scripts that write gold must be idempotent (rerunning overwrites cleanly) and must write through `write_gold`.

## 6. Build order

1. `users` (done), 2. `user_features` (done), 3. `eval_users` (done, rebuild per section 7)
4. `graph_edges` (measure: users with at least one edge, size of the largest connected component, number of seeds with a neighbor), then recompute `lf_coord_score` in `user_features`
5. `botsim_users` (needs the real structure of `user_post_comment.json` and the same feature definitions as Reddit)
6. `lf_votes`, 7. `weak_labels` (add BotSim true labels from the train part with a high weight)
8. `user_events` (only if a sequence model is used), 9. training (`split = train` only), 10. evaluation

## 7. Work in progress

- **Rebuild `eval_users`:** the current table takes half B from every user that has a post in BotBouncer (including pending/null/purged/organic/service). Add two functions to `scripts/gold/common.py`: `read_flags()` (with `is_banned` and `is_benign` columns) and `positive_flags()` (banned and not benign). Then in `eval_users.py` and `lf_votes.py` use `flags_all = read_flags()` and `all_flags = positive_flags(flags_all)`; split halves A/B on `all_flags`; exclude `flags_all` when counting negatives. The split is hash based, so users in half B stay in half B.
- Add `eval_label` (bigint) to the Terraform `eval_users` table if it is not applied yet. After editing `gold_schema.tf`, run `terraform apply`.

## 8. Unverified (do not assume)

- The structure of BotSim's `user_post_comment.json` (key names, timestamp field names, whether `comment_2` exists). BotSim timestamps may be simulation steps instead of real epoch times; if so, drop the hour and inter-arrival features for the BotSim domain.
- The BotSim label by row order (last 1,000 rows), which holds only while `Users.csv` has not been re-sorted.
- The meaning of flair `pending/H` and null, and how many `banned` users have enough data in the 15 subs.
- The real density of the coordination graph (is it dense enough to propagate labels?).
- `_is_autogen_name` (>= 5 digits or `user_`, `bot_` prefix) does not catch the `word-word-digits` names Reddit generates by default; the rate among flagged users versus the rest has not been checked.
