"""Ghi, kiểm tra và xuất bản hai file dữ liệu theo cấu trúc BotSim."""

import csv
import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from scripts.gold.common import MIN_EVENTS, silver_keys
from scripts.silver.common import BUCKET, s3


POST_COLUMNS = ["submission_id", "author_name", "title", "selftext", "score",
                "num_comments", "upvote_ratio", "created_utc", "subreddit"]
COMMENT_COLUMNS = ["comment_id", "author_name", "body", "score", "link_id",
                   "parent_id", "created_utc", "subreddit"]
CSV_FIELDS = ["user_id", "name", "description", "submission_num", "comment_num",
              "character_setting", "comment_num_1", "comment_num_2", "subreddit"]
ACTIVITY_GROUPS = ("posts", "comment_1", "comment_2")
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
FORMAT_NOTE = ("Users.csv xếp User trước Bot. Đọc counts_by_label.User trong report.json để xác định nhãn. "
               "comment_2 chứa reply ở nhiều độ sâu. level và levels là null khi thiếu chuỗi parent.")


def _timestamp(value):
    assert value is not None and value.tzinfo is not None, "Timestamp thiếu hoặc không có timezone"
    return value.astimezone(timezone.utc).strftime(TIMESTAMP_FORMAT)


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
                        timestamp = row["created_utc"]
                        ts = _timestamp(timestamp)
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
                                    "created_utc": ts}
                        count[category] += 1
                        # Giữ độ chính xác khi sắp xếp, chỉ định dạng giây trong JSON bàn giao.
                        sort_ts = timestamp.astimezone(timezone.utc).isoformat(timespec="microseconds")
                        rows.append((uid, category, sort_ts, item_id, sub, json.dumps(item, ensure_ascii=False, allow_nan=False)))
                    if rows:
                        conn.executemany("INSERT INTO events VALUES (?, ?, ?, ?, ?, ?)", rows)
                conn.commit()
                parquet.close()
                path.unlink()
    print(f"  Activity sau cutoff: {count}")
    return count


def _comment_depths(conn):
    parents = {}
    for item_id, event_json in conn.execute("SELECT item_id, event_json FROM events WHERE category != 'posts'"):
        assert item_id not in parents, f"Trùng comment_id: {item_id}"
        parents[item_id] = json.loads(event_json)["parent_id"]

    depths = {}
    for item_id in parents:
        chain = []
        current = item_id
        while current in parents and current not in depths:
            chain.append(current)
            depths[current] = None
            parent_id = parents[current]
            if parent_id.startswith("t3_"):
                depth = 0
                break
            assert parent_id.startswith("t1_"), f"Parent không hợp lệ: {parent_id}"
            current = parent_id[3:]
        else:
            depth = depths.get(current)
        for comment_id in reversed(chain):
            if depth is not None:
                depth += 1
                depths[comment_id] = depth
    return depths


def _write_files(conn, selection, destination):
    cursor = conn.cursor()
    depths = _comment_depths(conn)
    with (destination / "Users.csv").open("w", newline="", encoding="utf-8") as csvfile, \
            (destination / "user_post_comment.json").open("w", encoding="utf-8", newline="\n") as jsonfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDS)
        writer.writeheader()
        jsonfile.write("{\n")
        first_user = True
        # BotSim gốc xếp human trước bot. report.json lưu số dòng từng lớp.
        for row in selection.sort(["label", "user_id"], descending=[True, False]).iter_rows(named=True):
            uid = row["user_id"]
            counts = dict(cursor.execute("SELECT category, count(*) FROM events WHERE user_id=? GROUP BY category", (uid,)))
            n_posts = counts.get("posts", 0)
            n1, n2 = counts.get("comment_1", 0), counts.get("comment_2", 0)
            assert n_posts + n1 + n2 >= MIN_EVENTS, f"{uid}: số activity sau cutoff không đủ"
            subs = [r[0] for r in cursor.execute("SELECT DISTINCT subreddit FROM events WHERE user_id=? ORDER BY subreddit", (uid,))]
            writer.writerow({"user_id": uid, "name": uid, "description": "", "submission_num": f"{n_posts}.0",
                             "comment_num": f"{n1 + n2}.0", "character_setting": "", "comment_num_1": f"{n1}.0",
                             "comment_num_2": f"{n2}.0", "subreddit": repr(subs)})
            if not first_user:
                jsonfile.write(",\n")
            first_user = False
            jsonfile.write(json.dumps(uid, ensure_ascii=False) + ":{")
            for index, category in enumerate(ACTIVITY_GROUPS):
                if index:
                    jsonfile.write(",")
                jsonfile.write(json.dumps(category) + ":[")
                first_event = True
                for (event_json,) in cursor.execute(
                        "SELECT event_json FROM events WHERE user_id=? AND category=? ORDER BY ts,item_id", (uid, category)):
                    if category != "posts":
                        event = json.loads(event_json)
                        depth = depths[event["comment_id"]]
                        event["level"] = float(depth) if depth is not None else None
                        if category == "comment_2":
                            event["levels"] = event["level"]
                        event_json = json.dumps(event, ensure_ascii=False, allow_nan=False)
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
        assert reader.fieldnames == CSV_FIELDS, "CSV sai schema BotSim"
        assert jsonfile.readline() == b"{\n", "JSON thiếu mở object"
        for i, item in enumerate(expected.iter_rows(named=True)):
            row = next(reader, None)
            assert row is not None and row["user_id"] == row["name"] == item["user_id"], "CSV sai identity hoặc thứ tự nhãn"
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
            assert tuple(events) == ACTIVITY_GROUPS, "JSON sai danh sách activity"
            assert row["submission_num"] == f"{len(events['posts'])}.0"
            assert row["comment_num_1"] == f"{len(events['comment_1'])}.0"
            assert row["comment_num_2"] == f"{len(events['comment_2'])}.0"
            assert row["comment_num"] == f"{len(events['comment_1']) + len(events['comment_2'])}.0"
            for category, records in events.items():
                for record in records:
                    timestamp = datetime.fromisoformat(record["created_utc"])
                    assert timestamp.strftime(TIMESTAMP_FORMAT) == record["created_utc"], "Timestamp sai format BotSim"
                    if category == "comment_1":
                        assert isinstance(record["level"], float) and record["level"] == 1.0
                    if category == "comment_2":
                        depth = record["level"]
                        assert record["levels"] == depth and (depth is None or isinstance(depth, float) and depth >= 2.0)
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
