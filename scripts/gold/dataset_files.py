"""Ghi, kiểm tra và xuất bản hai file dữ liệu theo cấu trúc BotSim."""

import csv
import json
import os
import tempfile
import time
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from scripts.gold.common import MIN_EVENTS, silver_keys
from scripts.silver.common import BUCKET, s3


POST_COLUMNS = ["submission_id", "author_name", "title", "selftext", "score",
                "num_comments", "upvote_ratio", "created_utc", "subreddit"]
COMMENT_COLUMNS = ["comment_id", "author_name", "body", "score", "link_id",
                   "parent_id", "created_utc", "subreddit"]


def _timestamp(value):
    assert value is not None and value.tzinfo is not None, "Timestamp thiếu hoặc không có timezone"
    return value.strftime("%Y-%m-%d %H:%M:%S.%f")


def _stage_events(conn, selection):
    """Mỗi tháng download xuống disk, đọc record batch và lưu các event hợp lệ vào SQLite."""
    count = {"posts": 0, "comment_1": 0, "comment_2": 0}
    with tempfile.TemporaryDirectory(prefix="reddit_binary_silver_") as tmp:
        for kind, columns in (("posts", POST_COLUMNS), ("comments", COMMENT_COLUMNS)):
            for index, key in enumerate(silver_keys(kind)):
                path = Path(tmp) / f"{kind}_{index}.parquet"
                print(f"  Tải {key}")
                s3.download_file(BUCKET, key, str(path))
                parquet = pq.ParquetFile(path)
                for batch in parquet.iter_batches(batch_size=50000, columns=columns):
                    df = pl.from_arrow(batch).rename({"author_name": "user_id"})
                    df = df.join(selection, on="user_id", how="inner")
                    assert df.filter(pl.col("created_utc").is_null()).is_empty(), "Activity thiếu created_utc"
                    df = df.filter(
                        pl.when(pl.col("flagged_at").is_not_null())
                        .then(pl.col("created_utc") < pl.col("feature_cutoff_ts"))
                        .otherwise(pl.col("created_utc") <= pl.col("feature_cutoff_ts"))
                    )
                    rows = []
                    for row in df.iter_rows(named=True):
                        uid = row["user_id"]
                        ts = _timestamp(row["created_utc"])
                        sub = row["subreddit"]
                        assert isinstance(sub, str) and sub, "Activity thiếu subreddit"
                        if kind == "posts":
                            item_id = row["submission_id"]
                            assert item_id, "submission_id rỗng"
                            text = " ".join(s for s in (row["title"], row["selftext"]) if s).strip()
                            item = {"submission_id": item_id, "author_name": uid, "posts": text,
                                    "score": row["score"], "num_comments": row["num_comments"],
                                    "upvote_ratio": row["upvote_ratio"], "created_utc": ts, "subreddit": sub}
                            category = "posts"
                        else:
                            item_id = row["comment_id"]
                            assert item_id, "comment_id rỗng"
                            parent = row["parent_id"]
                            assert isinstance(parent, str) and (parent.startswith("t1_") or parent.startswith("t3_")), "parent_id thiếu hoặc không hợp lệ"
                            category = "comment_1" if parent.startswith("t3_") else "comment_2"
                            item = {"comment_id": item_id, "comment_author_name": uid,
                                    "comment_score": row["score"], "comment_body": row["body"],
                                    "link_id": row["link_id"], "parent_id": parent, "subreddit": sub,
                                    "created_utc": ts, "level": 1 if category == "comment_1" else None}
                        count[category] += 1
                        rows.append((uid, category, ts, item_id, sub, json.dumps(item, ensure_ascii=False, allow_nan=False)))
                    if rows:
                        conn.executemany("INSERT INTO events VALUES (?, ?, ?, ?, ?, ?)", rows)
                conn.commit()
                parquet.close()
                path.unlink()
    print(f"  Activity sau cutoff: {count}")
    return count


def _write_files(conn, selection, destination):
    cursor = conn.cursor()
    with (destination / "Users.csv").open("w", newline="", encoding="utf-8") as csvfile, \
            (destination / "user_post_comment.json").open("w", encoding="utf-8", newline="\n") as jsonfile:
        fields = ["user_id", "name", "description", "submission_num", "comment_num",
                  "character_setting", "comment_num_1", "comment_num_2", "subreddit", "label"]
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        jsonfile.write("{\n")
        first_user = True
        # BotSim gốc xếp human trước bot; label vẫn được ghi tường minh.
        for row in selection.sort(["label", "user_id"], descending=[True, False]).iter_rows(named=True):
            uid = row["user_id"]
            counts = dict(cursor.execute("SELECT category, count(*) FROM events WHERE user_id=? GROUP BY category", (uid,)))
            n_posts = counts.get("posts", 0)
            n1, n2 = counts.get("comment_1", 0), counts.get("comment_2", 0)
            assert n_posts + n1 + n2 >= MIN_EVENTS, f"{uid}: số activity sau cutoff không đủ"
            subs = [r[0] for r in cursor.execute("SELECT DISTINCT subreddit FROM events WHERE user_id=? ORDER BY subreddit", (uid,))]
            writer.writerow({"user_id": uid, "name": uid, "description": "", "submission_num": n_posts,
                             "comment_num": n1+n2, "character_setting": "", "comment_num_1": n1,
                             "comment_num_2": n2, "subreddit": repr(subs), "label": int(row["label"])})
            if not first_user:
                jsonfile.write(",\n")
            first_user = False
            jsonfile.write(json.dumps(uid, ensure_ascii=False) + ":{")
            for index, category in enumerate(("posts", "comment_1", "comment_2")):
                if index:
                    jsonfile.write(",")
                jsonfile.write(json.dumps(category) + ":[")
                first_event = True
                for (event_json,) in cursor.execute(
                        "SELECT event_json FROM events WHERE user_id=? AND category=? ORDER BY ts,item_id", (uid, category)):
                    if not first_event:
                        jsonfile.write(",")
                    first_event = False
                    jsonfile.write(event_json)
                jsonfile.write("]")
            jsonfile.write("}")
        jsonfile.write("\n}\n")


def _validate_files(selection, destination):
    """Đọc lại từng user, chặn JSON/CSV hỏng trước khi công bố."""
    expected = selection.select("user_id", "label").sort(["label", "user_id"], descending=[True, False])
    max_line_bytes = 64 * 1024 * 1024
    with (destination / "Users.csv").open("r", newline="", encoding="utf-8") as csvfile, \
            (destination / "user_post_comment.json").open("rb") as jsonfile:
        reader = csv.DictReader(csvfile)
        assert reader.fieldnames == ["user_id", "name", "description", "submission_num", "comment_num",
                                    "character_setting", "comment_num_1", "comment_num_2", "subreddit", "label"]
        assert jsonfile.readline() == b"{\n", "JSON thiếu mở object"
        for i, item in enumerate(expected.iter_rows(named=True)):
            row = next(reader, None)
            assert row is not None and row["user_id"] == row["name"] == item["user_id"], "CSV sai identity"
            assert row["label"] == str(item["label"]), "CSV sai label"
            line = jsonfile.readline(max_line_bytes + 1)
            assert len(line) <= max_line_bytes and line.endswith(b"\n"), "JSON một user quá lớn hoặc thiếu newline"
            payload = line[:-1]
            if i < expected.height - 1:
                assert payload.endswith(b","), "JSON thiếu dấu phân cách"
                payload = payload[:-1]
            else:
                assert not payload.endswith(b","), "JSON dư dấu phân cách"
            single = json.loads(b"{" + payload + b"}")
            assert list(single) == [item["user_id"]], "JSON sai key"
            events = single[item["user_id"]]
            assert list(events) == ["posts", "comment_1", "comment_2"], "JSON sai danh sách activity"
            assert len(events["posts"]) == int(row["submission_num"])
            assert len(events["comment_1"]) == int(row["comment_num_1"])
            assert len(events["comment_2"]) == int(row["comment_num_2"])
            assert int(row["comment_num"]) == len(events["comment_1"]) + len(events["comment_2"])
        assert next(reader, None) is None, "CSV dư rows"
        assert jsonfile.readline() == b"}\n" and jsonfile.read(1) == b"", "JSON thiếu đóng hoặc dư dữ liệu"


def _publish_staged(stage, destination):
    """Tạo version độc quyền, di chuyển từng file; rollback nếu publish lỗi."""
    names = ("Users.csv", "user_post_comment.json")
    assert all((stage / name).is_file() for name in names), "Stage thiếu file"
    destination.mkdir(exist_ok=False)
    moved = []

    def move(source, target):
        assert not target.exists(), f"File đích đã tồn tại: {target}"
        for attempt in range(10):
            try:
                os.replace(source, target)
                return
            except PermissionError:
                if attempt == 9:
                    raise
                time.sleep(0.5)

    try:
        for name in names:
            move(stage / name, destination / name)
            moved.append(name)
    except Exception as error:
        try:
            for name in reversed(moved):
                move(destination / name, stage / name)
            destination.rmdir()
        except Exception as rollback_error:
            raise RuntimeError(
                f"Rollback chưa hoàn tất; kiểm tra {stage.resolve()} và {destination.resolve()}"
            ) from rollback_error
        raise RuntimeError(f"Không thể publish; hai file còn tại {stage.resolve()}") from error
    try:
        stage.rmdir()
    except OSError:
        print(f"  Hai file đã publish; thư mục stage rỗng còn tại {stage.resolve()}")
