# scripts/silver/build_posts.py
"""
Build silver → posts.parquet.

Tự động:
  - Scan bronze S3 tìm tháng có posts/normal.jsonl
  - Skip tháng đã build silver (so mtime)
  - Chỉ build tháng mới hoặc tháng có bronze mới hơn
  - Chuẩn hoá author_name về lowercase (Reddit username case-insensitive)
  - Giữ stats thay vì full DataFrame → giảm RAM

Chạy:
  python -m scripts.silver.build_posts                # auto
  python -m scripts.silver.build_posts 2026-09        # chỉ tháng 9
  python -m scripts.silver.build_posts --force        # rebuild hết
"""

import sys
import pandas as pd

from scripts.silver.common import (
    BronzeReadError,
    SILVER_PREFIX,
    discover_bronze_months,
    months_needing_build,
    read_bronze_jsonl,
    schema_of,
    upload_parquet,
    write_metadata,
    _to_ts,
    to_int,
    to_float,
)


# ================== TRANSFORM ==================
SKIP_AUTHORS = {"[deleted]", "automoderator"}


def clean_post(obj: dict, month: str) -> dict:
    """Post JSON → row sạch cho silver."""
    author = obj.get("author")
    if isinstance(author, str):
        author_clean = author.lower()
    else:
        author_clean = None

    return {
        "submission_id": obj.get("id"),
        "author_name":   author_clean,
        "title":         obj.get("title") or "",
        "selftext":      obj.get("selftext") or "",
        "score":         to_int(obj.get("score")),
        "num_comments":  to_int(obj.get("num_comments")),
        "upvote_ratio":  to_float(obj.get("upvote_ratio")),
        "created_utc":   _to_ts(obj.get("created_utc")),
        "subreddit":     obj.get("subreddit"),
        "url":           obj.get("url"),
        "permalink":     obj.get("permalink"),
        "ingest_month":  month,
    }


# ================== BUILD 1 THÁNG ==================
def build_posts_month(month: str) -> dict | None:
    """
    Đọc bronze posts/normal tháng X → clean → ghi parquet.
    Trả về dict stats (nhẹ) hoặc None.
    """
    rows = []
    n_scanned = 0

    for obj in read_bronze_jsonl("posts", month, "normal"):
        n_scanned += 1
        rows.append(clean_post(obj, month))

    if not rows:
        print(f"    ⏭️  posts {month}: 0 rows")
        return None

    df = pd.DataFrame(rows)
    df["upvote_ratio"] = df["upvote_ratio"].astype("float64")

    n_valid_authors = int(df["author_name"].notna().sum())
    n_deleted = int((df["author_name"] == "[deleted]").sum())
    stats = {
        "rows":   len(df),
        "schema": schema_of(df),
    }

    upload_parquet(df, f"{SILVER_PREFIX}/posts/data/posts_{month}.parquet")

    print(f"    📊 scan {n_scanned:,} posts")
    print(f"       author hợp lệ: {n_valid_authors:,}")
    print(f"       [deleted]:     {n_deleted:,}")

    # df ra khỏi scope sau return → giải phóng RAM
    return stats


# ================== MAIN ==================
def main(months: list | None = None, force: bool = False):
    print("=" * 60)
    print("Build Silver → posts")
    print("=" * 60)

    if months:
        months_to_build = months
        print(f"Chỉ định: {len(months_to_build)} tháng → {months_to_build}\n")
    else:
        print("Scanning bronze S3...")
        all_months = discover_bronze_months()
        if not all_months:
            raise RuntimeError("Không tìm thấy tháng nào trong bronze.")

        if force:
            months_to_build = all_months
            print(f"  → {len(all_months)} tháng: {all_months} (--force)\n")
        else:
            months_to_build = months_needing_build("posts")
            skipped = sorted(set(all_months) - set(months_to_build))
            print(f"  → {len(all_months)} tháng trong bronze: {all_months}")
            if skipped:
                print(f"  → Skip {len(skipped)} tháng đã build: {skipped}")
            if months_to_build:
                print(f"  → Build {len(months_to_build)} tháng: {months_to_build}\n")
            else:
                print(f"  → Không có tháng mới cần build.\n")
                return

    # ---- Build từng tháng — giữ stats thay vì DataFrame ----
    stats = {}
    failed = []

    for month in months_to_build:
        print(f"\n=== {month} ===")
        try:
            s = build_posts_month(month)
            if s is not None:
                stats[month] = s
        except BronzeReadError as e:
            print(f"  ❌ {month}: {e}")
            failed.append(month)
        except Exception as e:
            print(f"  ❌ {month}: {type(e).__name__}: {e}")
            failed.append(month)

    # ---- Ghi metadata ----
    if stats:
        print(f"\n=== Metadata ===")
        write_metadata("posts", stats)

    # ---- Tổng kết ----
    print("\n" + "=" * 60)
    if failed:
        print(f"❌ THẤT BẠI {len(failed)}/{len(months_to_build)} tháng: {failed}")
        print("=" * 60)
        raise RuntimeError(f"Build fail {len(failed)} tháng: {failed}")

    if stats:
        total = sum(s["rows"] for s in stats.values())
        print(f"✅ POSTS HOÀN TẤT — {total:,} rows ({len(stats)} tháng)")
    else:
        print(f"✅ Không có tháng nào cần build.")
    print("=" * 60)


if __name__ == "__main__":
    args = sys.argv[1:]
    force = "--force" in args
    months = [a for a in args if not a.startswith("--")] or None
    main(months, force=force)