# scripts/silver/build_comments.py
"""
Build silver → comments.parquet.

STREAMING WRITE — RAM cố định ~500 MB dù file lớn cỡ nào:
  - Đọc bronze → clean → gom batch 200K rows
  - Ghi mỗi batch vào file tạm local (ParquetWriter)
  - Upload file tạm lên S3 → xoá file tạm

Tự động:
  - Scan bronze S3 tìm tháng có comments/normal.jsonl
  - Skip tháng đã build (so mtime)
  - Chuẩn hoá author_name về lowercase

Chạy:
  python -m scripts.silver.build_comments                # auto
  python -m scripts.silver.build_comments 2026-09        # chỉ tháng 9
  python -m scripts.silver.build_comments --force        # rebuild hết
"""

import sys
import tempfile
from pathlib import Path

# pyrefly: ignore [missing-import]
import pyarrow as pa
# pyrefly: ignore [missing-import]
import pyarrow.parquet as pq

from scripts.silver.common import (
    BUCKET,
    s3,
    BronzeReadError,
    SILVER_PREFIX,
    discover_bronze_months,
    months_needing_build,
    read_bronze_jsonl,
    write_metadata,
    _to_ts,
    to_int,
)

# Số dòng mỗi batch — càng nhỏ RAM càng thấp nhưng chậm hơn
BATCH_SIZE = 200_000


# ================== SCHEMA ==================
# Cố định schema — tránh pyarrow tự suy từng batch (gây null/int mismatch)
COMMENTS_SCHEMA = pa.schema([
    ("comment_id",   pa.string()),
    ("author_name",  pa.string()),
    ("body",         pa.string()),
    ("score",        pa.int64()),
    ("link_id",      pa.string()),
    ("parent_id",    pa.string()),
    ("created_utc",  pa.timestamp("us", tz="UTC")),
    ("subreddit",    pa.string()),
    ("permalink",    pa.string()),
    ("ingest_month", pa.string()),
])


# ================== TRANSFORM ==================
def clean_comment(obj: dict, month: str) -> dict:
    """Comment JSON → row sạch cho silver."""
    author = obj.get("author")
    if isinstance(author, str):
        author_clean = author.lower()
    else:
        author_clean = None

    return {
        "comment_id":   obj.get("id"),
        "author_name":  author_clean,
        "body":         obj.get("body") or "",
        "score":        to_int(obj.get("score")),
        "link_id":      obj.get("link_id"),
        "parent_id":    obj.get("parent_id"),
        "created_utc":  _to_ts(obj.get("created_utc")),
        "subreddit":    obj.get("subreddit"),
        "permalink":    obj.get("permalink"),
        "ingest_month": month,
    }


# ================== BUILD 1 THÁNG ==================
def build_comments_month(month: str) -> dict | None:
    """
    Đọc bronze comments/normal tháng X → clean → ghi parquet streaming.
    - Schema explicit → không lệch giữa các batch.
    - try/finally đóng writer → không PermissionError khi cleanup.
    """
    n_scanned = 0
    n_total = 0
    batch = []
    writer = None
    s3_key = f"{SILVER_PREFIX}/comments/data/comments_{month}.parquet"

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir) / f"comments_{month}.parquet"

        try:
            for obj in read_bronze_jsonl("comments", month, "normal"):
                n_scanned += 1
                batch.append(clean_comment(obj, month))

                if len(batch) >= BATCH_SIZE:
                    if writer is None:
                        writer = pq.ParquetWriter(
                            str(tmp_path),
                            COMMENTS_SCHEMA,
                            compression="snappy",
                        )
                    table = pa.Table.from_pylist(batch, schema=COMMENTS_SCHEMA)
                    writer.write_table(table)
                    n_total += len(batch)
                    batch = []
                    print(f"       ghi {n_total:,} rows...")

            # Batch cuối
            if batch:
                if writer is None:
                    writer = pq.ParquetWriter(
                        str(tmp_path),
                        COMMENTS_SCHEMA,
                        compression="snappy",
                    )
                table = pa.Table.from_pylist(batch, schema=COMMENTS_SCHEMA)
                writer.write_table(table)
                n_total += len(batch)
                batch = []
        finally:
            # Đóng writer trước khi TemporaryDirectory cleanup
            if writer is not None:
                writer.close()
                writer = None

        if n_total == 0:
            print(f"    ⏭️  comments {month}: 0 rows")
            return None

        # Lấy schema + size từ file tạm
        arrow_schema = pq.read_schema(str(tmp_path))
        schema_list = [{"name": f.name, "type": str(f.type)} for f in arrow_schema]
        size_mb = tmp_path.stat().st_size / 1024 / 1024

        # Upload lên S3 (streaming)
        s3.upload_file(str(tmp_path), BUCKET, s3_key)

    print(f"    ✅ s3://{BUCKET}/{s3_key}  ({size_mb:.1f} MB, {n_total:,} rows)")
    print(f"    📊 scan {n_scanned:,} comments")

    return {"rows": n_total, "schema": schema_list}


# ================== MAIN ==================
def main(months: list | None = None, force: bool = False):
    print("=" * 60)
    print("Build Silver → comments")
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
            months_to_build = months_needing_build("comments")
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
            s = build_comments_month(month)
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
        write_metadata("comments", stats)

    # ---- Tổng kết ----
    print("\n" + "=" * 60)
    if failed:
        print(f"❌ THẤT BẠI {len(failed)}/{len(months_to_build)} tháng: {failed}")
        print("=" * 60)
        raise RuntimeError(f"Build fail {len(failed)} tháng: {failed}")

    if stats:
        total = sum(s["rows"] for s in stats.values())
        print(f"✅ COMMENTS HOÀN TẤT — {total:,} rows ({len(stats)} tháng)")
    else:
        print(f"✅ Không có tháng nào cần build.")
    print("=" * 60)


if __name__ == "__main__":
    args = sys.argv[1:]
    force = "--force" in args
    months = [a for a in args if not a.startswith("--")] or None
    main(months, force=force)