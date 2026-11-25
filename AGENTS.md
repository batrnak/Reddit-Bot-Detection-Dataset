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
| BotSim-24 | 1,907 real Reddit humans + 1,000 simulated LLM bots in the original file | Official `Users.csv`: first 1,907 humans, last 1,000 bots. Local file checksum matches upstream; S3 copy lacks two bots with names colliding after lowercase. Both colliding users are excluded from eval. |

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
| `users` | user_id | Fixed node_idx, n_events, eligible (n_events >= 4), split train/val/test (70/15/15 by hash) | Rebuilt for 2025-09–2026-08: 1,475,061 users, 572,199 eligible |
| `user_features` | user_id | 9 model features + `has_enough_data` + 4 `lf_*` columns (LF only) + `feature_cutoff_ts` | Rebuilt for 204,957 eligible users; 204,679 have enough data (`lf_coord_score` is 0) |
| `eval_users` | user_id, eval_role | Evaluation set: `botbouncer_B` (half B, eval_label=1), `botsim_heldout` (humans and bots) | Rebuilt; B: 221 eligible, 160 usable; BotSim heldout: 873 |
| `graph_edges` | src_idx, dst_idx | `coord_*` edges (label propagation) and `interact_*` edges (representation only), `weight_idf`, `first_ts`, `last_ts` | Not built |
| `botsim_users` | user_id (`bs_<name>`) | BotSim with true label `is_bot`, `split` train/heldout, 9 features with the same column names as `user_features` | Not built |
| `user_events` | user_id, ts | Event sequence for the neural network, cut at `feature_cutoff_ts` | Not built |
| `lf_votes` | (user_id, lf_name) | Long-format positive LF votes, `score`, `evidence` | Rebuilt; 4,371 votes after heldout identity exclusion |
| `weak_labels` | user_id | `n_lfs_vote`, `lf_mask`, `evidence_score`, `label_status`, `y_pu` | Rebuilt; 4,333 evidence_positive and 200,205 unknown after identity exclusion |

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
- Constants: `MIN_EVENTS = 4`, `SALT_SPLIT = "split_v1"`, `SALT_FLAG = "flag_v1"`. Each split uses its own salt.

**Labeling functions**
- At most one vote per (user, LF). The community-flag LF only votes 1 and abstains for everyone else. Not being flagged does not mean human.
- Threshold LFs only run on users with `has_enough_data` (n_events >= 4, counted after the `flagged_at` cut).
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

1. `python -m scripts.data_source.backfill_year --end-month 2026-08 --work-dir C:\arctic_year_test`
2. `python -m scripts.gold.build_users`
3. `python -m scripts.gold.export_verified_year --end-month 2026-08`

Exporter dùng `scripts/gold/botsim.py` để loại BotSim held-out và `scripts/gold/dataset_files.py` để ghi và kiểm tra hai file bàn giao.

## 7. Work in progress

- Luồng nhãn theo nguồn trong 12 tháng: `scripts.data_source.backfill_year` → `scripts.gold.build_users` → `scripts.gold.export_verified_year`. Cửa sổ mặc định 2025-09–2026-08. Chỉ rebuild `gold.users` và dùng lại split/identity hiện có. Nhãn Bot lấy BotBouncer half A với cutoff, nhãn User lấy verified author flair từ hai nhóm nguồn đã kiểm tra trong `exports/verified_user_probe_2026.py` và `exports/expand_verified_users_2026.py`. Loại account có bằng chứng xung đột và heldout. Half B chỉ đếm cho eval. Luồng này bỏ LF, weak labels và labeler. Kết quả xuất hai file ngoài Gold theo version. Test backfill tháng 2025-09 đã ghi đủ ba file Bronze và ba file Silver (51.075 posts, 2.316.086 comments, 36.032 dòng `bot_users`). Posts và comments normal đã kiểm tra đúng 15 subreddit và đúng tháng UTC. Log ngày 2026-11-25 xác nhận `gold.users` đã ghi 1.475.061 tài khoản và có 572.199 eligible. Export đã qua kiểm tra cửa sổ 12 tháng và ghi cache cho r/AskDocs, r/science và r/selfie (504 account khác nhau trước lọc cờ và held-out). Lần chạy tiếp theo dừng ở r/ProtectAndServe khi cả truy vấn aggregate và search của Arctic Shift timeout. Nhánh thu hẹp cửa sổ search đã pass với API giả lập. Chưa có counts nhãn của bộ 12 tháng.

### Kết quả thí nghiệm trước đây

Các tên script dưới đây ghi lại provenance. Mã nguồn đã rút khỏi luồng chạy được sao lưu local trong `exports/legacy_sources/`.

- v1 builds `users`, `user_features`, `eval_users`, `lf_votes`, `weak_labels`, then exports the train split. `exports/dataset_v1` was produced with 143,318 rows: 2,999 evidence_positive and 140,319 unknown. Graph, BotSim-train weighting, label model, user events and detector are out of scope for v1. The export accepts `--version vN` or `--version YYYY-MM` and never overwrites an existing version.
- `read_flags()` aggregates available Silver flair rows; `positive_flags()` selects banned and not benign. Half B is eval-only. Glue `eval_label bigint` is applied; the v1 `weak_labels` schema was applied.
- BotSim S3 `Users.csv` is missing two bots. `eval_users` verifies the original checksum and falls back to the verified local original. Both colliding names are excluded after applying original row-order labels. The local original must be available for refresh until the S3 source is corrected.
- Binary-labeler experiment: `build_binary_labeler.py` trains LogisticRegression on 103 BotBouncer A and 331 BotSim-human historical seeds; 34 conflicting human candidates are excluded. It predicts on unflagged Reddit train outside seed and eval; p_user <= 0.2 gives bot=0, >= 0.8 gives user=1. The score is not calibrated. Result: 22,137 predicted bots, 42,777 predicted users; 77,481 excluded by threshold. These are pseudo labels, not ground truth.
- Identity leakage correction: `eval_exclusion_users` also excludes the Reddit alias of each BotSim heldout human from LF, weak labels, and new exports. Gold LF and weak_labels have been rebuilt. The immutable `exports/dataset_v1` remains contaminated: 122 BotSim heldout human identities appear as Reddit train users and it must not be used for this experiment.
- Evaluation uses fixed model/thresholds, no tuning on heldout. BotBouncer B vs unflagged activity-bin-matched test comparison: AUC 0.731, bootstrap 95% CI 0.672–0.784. BotSim heldout: AUC 0.245, bootstrap 95% CI 0.210–0.280, n=758. On selected BotSim heldout samples, agreement is 3/22 predicted bot and 342/594 predicted user. This indicates very poor transfer to this BotSim benchmark; it does not establish Reddit label accuracy. Output is experimental, not a validated handoff. See `exports/labeler_experiment/metrics.json` and `botsim_metrics.json`.
- `exports/dataset_binary_v1/Users.csv` and `user_post_comment.json` were created and validated: 64,914 predicted users, 32,796 posts, 662,296 top-level comments and 1,508,377 replies. `comment_2` includes all replies to t1_, not necessarily depth 2; `level=null`. Provenance and SHA-256 are in `exports/labeler_experiment/metadata.json`. Original seed users are not in the two files.
- Three-class labeler experiment: `build_three_class_labeler.py` uses BotBouncer A, BotSim simulated bots, and human sources, merging bot scores only for binary pseudo labels. BotSim events restricted to the first 92 days (2023-06-20 to 2023-09-20 exclusive) and passed through Reddit's shared feature code; link_ratio excluded. Seeds are split by real identity into train/dev and excluded from predictions. All eval identities and aliases are excluded from training/output.
- Results: on the same 207 BotSim heldout users with >=4 events in the 92-day window, old AUC 0.481, new AUC 0.761 (95% bootstrap CI 0.699–0.822). BotBouncer B vs unflagged matched comparison: old AUC 0.731, new AUC 0.689 (CI 0.628–0.750). BotSim heldout was seen in the earlier experiment: these comparisons are exploratory, not an untouched final test. The new pseudo labels contain 22,270 bot, 7,387 user; 113,035 uncertain are excluded. Scores are uncalibrated; no unseen-generator claim. See `exports/labeler_3class_experiment/metrics.json`.
- `exports/dataset_binary_v2/Users.csv` and `user_post_comment.json` are the three-class model's merged binary predictions, not ground truth. Verified 29,657 users, 28,015 posts, 361,482 top-level comments, 835,091 replies; metadata/checksums are in `exports/labeler_3class_experiment/metadata.json`. The script fails fast unless Silver posts/comments cover the same three contiguous months totaling 92 days; Gold features must be rebuilt from current Silver. It cannot silently handle a longer monthly window.
- Controlled train/dev study: `analyze_labeler_variants.py` compares binary/three-class and balanced/unweighted, plus a three-class ablation without time-interval features, on identical seeds/split/candidates. Only BotSim train and BotBouncer A are used; eval_users are read solely to exclude identities. Output `exports/labeler_variant_analysis.json`: three-class balanced selects 22,270 bot/7,387 user, unweighted 20,109/11,807, binary balanced 10,386/18,290, three-class balanced without time features 14,644/565; remaining candidates are unknown. Dev has 95 rows, with possible duplicate identities across data sources but no identity overlap with train. These scores do not establish Reddit accuracy, prevalence, or a final model choice; no v3 was exported.
- Stability test actually run: `stability_test.py` compares binary and three-class balanced over 20 identity-grouped train/dev splits, using BotBouncer A and BotSim train only and 142,692 fixed Reddit candidates. Binary: AUC std 0.0420, agreement including unknown 93.12%, Bot CV 15.56%, User CV 18.73%. Three-class: 0.0293, 93.00%, 9.10%, 26.41% respectively. Both fail the agreed CV ≤10% gate, so no v3 was exported. Full metrics and Reddit/BotSim Cohen's d: `exports/labeler_stability_v3/metadata.json`. Splits overlap; stability alone cannot validate correctness. BotSim heldout and BotBouncer B were not evaluated during this test.

## 8. Unverified (do not assume)

- Local BotSim JSON was inspected: user keys contain `posts`, `comment_1`, `comment_2`, and timestamp strings are calendar datetimes. The paper describes simulated bot timelines; their comparability with Reddit remains unverified.
- The meaning of flair `pending/H` and null, and how many `banned` users have enough data in the 15 subs.
- The real density of the coordination graph (is it dense enough to propagate labels?).
- `_is_autogen_name` (>= 5 digits or `user_`, `bot_` prefix) does not catch the `word-word-digits` names Reddit generates by default; the rate among flagged users versus the rest has not been checked.

9. Monthly automation (Airflow) — code must be refresh-safe
Purpose: each month Airflow ingests the new Arctic Shift month and re-publishes an updated
dataset version. Write all code NOW so that run needs no rewrites. v1 does NOT build the DAG;
it only makes every script DAG-ready.

9.1 Incremental where cheap, FULL recompute where correctness demands
- Bronze/silver: incremental per month. Ingest writes bronze/.../year=YYYY/month=MM/; silver
  builds one fixed-key file per month. Never rewrite old months.
- Gold: FULL recompute every run from ALL silver months. Derive the month list from S3
  (silver_keys / iter_silver); never hardcode months. Reason: a user unflagged last month may be
  banned this month, so feature_cutoff_ts changes retroactively; incremental feature append would
  keep stale, leaky features. Same for eval-half membership and the negative/comparison set.
- BotSim: static external benchmark. Monthly runs must not re-ingest or modify botsim_users.

9.2 Determinism & stable identity across runs
- No datetime.now()/today inside feature, split or label logic. Use data timestamps; the Airflow
  logical date only selects which month to ingest. Reruns must be deterministic.
- node_idx append-only. split, flag halves A/B, and BotSim heldout are hash-based with fixed
  salts => a user never moves between train/eval or A/B across months. Half B stays eval-only in
  EVERY run; newly flagged users hash into A/B automatically.
- Active constants (MIN_EVENTS and split salts) live in scripts/gold/common.py. Changes to label selection require a new dataset version.

9.3 Idempotency & fail-fast gates
- Every build script rerunnable: same input => same output; writes only via write_gold; backups
  outside table prefixes.
- Each script ends with asserts (unique keys; halfA+halfB==total; eval∩LF==; eval∩train==;
  coverage thresholds; timestamp range sane). A failed assert aborts the run; never publish.
- No interactive input, no manual step inside scripts. One `python -m ...` module = one Airflow task.

9.4 Versioned export & lineage (outside Gold; no new Gold tables)
- Export writes exports/dataset_<YYYY-MM>/ (or vN): train/val/test parquet + metadata.json +
  README.md. Never overwrite a handed-off version; a new month publishes a NEW version.
- metadata.json records: source silver months, run id / Airflow logical date, git commit,
  MIN_EVENTS, salts, LF thresholds + lf->column map, label-semantics version, counts (eligible,
  usable positives per eval role, unknown), known limitations.
- Publish only after all gates pass; keep previous versions for reproducibility (paper citations).

9.5 Resource & DAG shape
- 8 GB RAM: read silver month-by-month; download large S3 objects to disk before streaming.
- The active source-label flow is backfill for Bronze/Silver, then `gold.users`, then the verified export. A future DAG wraps those steps and their validation gates.
