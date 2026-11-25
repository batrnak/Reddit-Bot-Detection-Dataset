"""Xuất dataset nhị phân một năm từ BotBouncer và các verified flair đã kiểm chứng."""

import argparse
from collections import Counter
from contextlib import closing
from datetime import datetime, timedelta, timezone
from functools import partial
import json
from pathlib import Path
import sqlite3
import tempfile

import polars as pl

from exports import expand_verified_users_2026 as expansion
from exports import verified_user_probe_2026 as probe
from scripts.data_source.backfill_year import assert_silver_window, year_months
from scripts.gold.botsim import build_botsim_heldout, read_botsim24
from scripts.gold.common import (
    MIN_EVENTS, SALT_FLAG, SALT_SPLIT, eval_exclusion_users, flags_half_a, flags_half_b,
    positive_flags, read_flags,
)
from scripts.gold.dataset_files import FORMAT_NOTE, _publish_staged, _stage_events, _validate_files, _write_files


def _verified_flairs(activity, months, out):
    api = probe.Archive()
    start, *_, end = probe.month_bounds(months)
    hits = set()
    counts = {}
    for sources, verify_flair in ((probe.SOURCES, probe.is_verified), (expansion.SOURCES, expansion.verified)):
        verify = partial(verify_flair, until=end)
        for source in sources:
            print(f"Kiểm tra verified flair r/{source}", flush=True)
            if source == "AskLE":
                result = expansion.fetch_askle(api, activity, months=months, out=out)
            else:
                result = probe.fetch_source(
                    api, source, activity, sources=sources, verify=verify, months=months, out=out,
                )
            evidence = result["evidence"]
            for name, row in evidence.items():
                assert activity.get(name, (0, None))[0] >= MIN_EVENTS, f"Cache lệch activity: {name}"
                assert row["author"].casefold() == name and row["subreddit"].casefold() == source.casefold()
                assert start <= row["created_utc"] <= row["retrieved_on"] < end and verify(source, row)
            hits.update(evidence)
            counts[source] = len(evidence)
            print(f"  r/{source}: {counts[source]:,} account có bằng chứng", flush=True)
    return hits, counts


def export_year(end_month, destination):
    months = year_months(end_month)
    assert not destination.exists(), f"Version đã tồn tại: {destination}"
    assert_silver_window(months)
    activity = probe.read_activity(months)
    all_flags = read_flags(banned_source="BotBouncer")
    flagged = set(all_flags["user_id"])
    bot_flags = positive_flags(all_flags)
    half_a, half_b = flags_half_a(bot_flags), flags_half_b(bot_flags)
    assert half_a.height + half_b.height == bot_flags.height, "Flag split lệch"

    heldout = build_botsim_heldout(read_botsim24())
    exclusion = set(eval_exclusion_users(heldout)["user_id"])
    evidence_dir = destination.with_name(destination.name + "_evidence")
    user_hits, source_counts = _verified_flairs(activity, months, evidence_dir)
    human_ids = user_hits - flagged - exclusion
    bot_ids = set(half_a["user_id"]) - user_hits - exclusion
    eval_ids = set(half_b["user_id"]) - user_hits - exclusion

    assert not (bot_ids & human_ids or eval_ids & bot_ids or eval_ids & human_ids)
    bot_ids = {name for name in bot_ids if activity.get(name, (0, None))[0] >= MIN_EVENTS}
    eval_ids = {name for name in eval_ids if activity.get(name, (0, None))[0] >= MIN_EVENTS}
    assert bot_ids and human_ids, "Cần ứng viên đủ activity cho cả Bot và User"
    print(f"Ứng viên Bot A={len(bot_ids):,}, User={len(human_ids):,}, Eval B={len(eval_ids):,}")

    bounds = probe.month_bounds(months)
    start = datetime.fromtimestamp(bounds[0], tz=timezone.utc)
    end = datetime.fromtimestamp(bounds[-1], tz=timezone.utc)
    utc_us = pl.Datetime("us", "UTC")
    bot_selection = bot_flags.filter(pl.col("user_id").is_in(sorted(bot_ids | eval_ids))).select(
        "user_id", pl.col("flagged_at").cast(utc_us),
    )
    assert bot_selection["flagged_at"].null_count() == 0, "Bot ứng viên thiếu flagged_at"
    assert bot_selection.filter(
        (pl.col("flagged_at") < start) | (pl.col("flagged_at") >= end)
    ).is_empty(), "flagged_at nằm ngoài cửa sổ"
    selection = pl.concat([
        bot_selection.with_columns(pl.col("flagged_at").alias("feature_cutoff_ts")),
        pl.DataFrame({"user_id": sorted(human_ids)}).with_columns(
            pl.lit(None, dtype=utc_us).alias("flagged_at"),
            pl.lit(end - timedelta(microseconds=1), dtype=utc_us).alias("feature_cutoff_ts"),
        ),
    ])

    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="reddit_verified_", dir=destination.parent) as temp:
        stage = Path(temp) / "handoff"
        stage.mkdir()
        with closing(sqlite3.connect(Path(temp) / "events.db")) as db:
            db.execute("CREATE TABLE events (user_id TEXT, category TEXT, ts TEXT, item_id TEXT, "
                       "subreddit TEXT, event_json TEXT, PRIMARY KEY (user_id, category, ts, item_id))")
            _stage_events(db, selection)
            usable = {user_id for (user_id,) in db.execute(
                "SELECT user_id FROM events GROUP BY user_id HAVING count(*) >= ?", (MIN_EVENTS,),
            )}
            assert human_ids <= usable, "Activity của User thiếu events so với gold.users"
            actual_bot_ids = bot_ids & usable
            usable_b = len(eval_ids & usable)
            assert actual_bot_ids, "Không có Bot A nào đủ activity trước cutoff"
            selected = pl.DataFrame({
                "user_id": sorted(actual_bot_ids) + sorted(human_ids),
                "label": [0] * len(actual_bot_ids) + [1] * len(human_ids),
            })
            assert not set(selected["user_id"]) & (eval_ids | exclusion)
            _write_files(db, selected, stage)
            _validate_files(selected, stage)
        split_counts = {"Bot": Counter(activity[name][1] for name in actual_bot_ids),
                        "User": Counter(activity[name][1] for name in human_ids)}
        report = {
            "months": months, "min_events": MIN_EVENTS, "salt_split": SALT_SPLIT, "salt_flag": SALT_FLAG,
            "labels": {"0": "BotBouncer A banned, không organic/service, đủ events trước flagged_at",
                       "1": "Verified author flair, đủ events trong 15 subs, không có trong bot_users"},
            "limit": "Nhãn theo nguồn. Verified flair xác minh thông tin của chủ account, "
                     f"không xác minh từng bài do người viết. BotBouncer B là proxy eval. {FORMAT_NOTE}",
            "verified_by_source": source_counts,
            "verified_with_flag_excluded": len(user_hits & flagged),
            "verified_with_positive_flag_excluded": len(user_hits & set(bot_flags["user_id"])),
            "botbouncer_b_usable_eval_only": usable_b,
            "counts_by_label": {"Bot": len(actual_bot_ids), "User": len(human_ids)},
            "counts_by_split": {label: {split: counts[split] for split in ("train", "val", "test")}
                                for label, counts in split_counts.items()},
        }
        report_path = evidence_dir / "report.json"
        report_tmp = Path(temp) / "report.json"
        report_tmp.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        report_tmp.replace(report_path)
        _publish_staged(stage, destination)

    print(f"Dataset: {destination}\nBáo cáo: {report_path}")
    print(json.dumps(report["counts_by_split"], ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--end-month", default="2026-08")
    parser.add_argument("--destination", type=Path)
    args = parser.parse_args()
    months = year_months(args.end_month)
    destination = args.destination or Path(f"exports/dataset_verified_{months[0]}_{months[-1]}_v2")
    export_year(args.end_month, destination)


if __name__ == "__main__":
    main()
