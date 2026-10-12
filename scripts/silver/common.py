import json
from datetime import datetime, timezone
from io import BytesIO

import boto3
import pandas as pd 
# pyrefly: ignore [missing-import]
import pyarrow as pa
from botocore.exceptions import ClientError

# ================== CẤU HÌNH ==================
BUCKET        = "data-pipeline-datalake-537390611425"
BRONZE_PREFIX = "bronze/arctic_shift/raw"
SILVER_PREFIX = "silver/arctic_shift"

DEFAULT_MONTHS = ["2026-06", "2026-07", "2026-08"]

s3 = boto3.client("s3")


# ================== READ BRONZE ==================
class BronzeReadError(Exception):
    """Lỗi đọc bronze — không được nuốt."""
    pass


def read_bronze_jsonl(kind: str, month: str, label: str, allow_missing: bool = False):
    """
    Stream JSONL từ bronze S3 → generator of dict.

    - allow_missing=True : file không tồn tại → return empty (bỏ qua im lặng).
    - allow_missing=False: file không tồn tại → raise BronzeReadError (mặc định).
    - Các lỗi S3 khác (AccessDenied, NetworkError, ...) → LUÔN raise.
    - File rỗng (0 dòng) → raise.
    - Parse error > 1% → raise.
    """
    y, m = month.split("-")
    key = f"{BRONZE_PREFIX}/{kind}/year={y}/month={m}/{label}.jsonl"
    uri = f"s3://{BUCKET}/{key}"

    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        if code in ("NoSuchKey", "404", "NotFound"):
            if allow_missing:
                print(f"  ⚠️  {uri}: không tồn tại, bỏ qua.")
                return
            raise BronzeReadError(f"Bronze thiếu file: {uri}") from e
        raise BronzeReadError(f"S3 read fail ({code}): {uri}") from e
    except Exception as e:
        raise BronzeReadError(f"S3 read fail: {uri}: {e}") from e

    n_lines = 0
    n_parse_err = 0
    for line in obj["Body"].iter_lines():
        if not line:
            continue
        n_lines += 1
        try:
            yield json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            n_parse_err += 1

    # Sau khi stream xong hết
    if n_lines == 0:
        raise BronzeReadError(f"File rỗng: {uri}")

    if n_parse_err > max(10, n_lines * 0.01):
        raise BronzeReadError(
            f"Quá nhiều parse error: {n_parse_err}/{n_lines} dòng ở {uri}"
        )
    if n_parse_err:
        print(f"  ⚠️  {uri}: {n_parse_err}/{n_lines} dòng parse fail (bỏ qua)")


# ================== TRANSFORM ==================
def _to_ts(u):
    """Unix ts (seconds) → pandas Timestamp độ phân giải us (Athena tương thích)."""
    if not u:
        return None
    try:
        return pd.Timestamp(int(u), unit="s", tz="UTC").as_unit("us")
    except (ValueError, OSError, TypeError):
        return None


# ================== WRITE PARQUET ==================
def upload_parquet(df: pd.DataFrame, key: str):
    """Ghi DataFrame → Parquet → S3 (in-memory)."""
    buf = BytesIO()
    df.to_parquet(
        buf,
        index=False,
        engine="pyarrow",
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )
    buf.seek(0)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    size_mb = buf.getbuffer().nbytes / 1024 / 1024
    print(f"    ✅ s3://{BUCKET}/{key}  ({size_mb:.1f} MB, {len(df):,} rows)")


def schema_of(df: pd.DataFrame) -> list:
    """Pandas dtypes → schema list cho metadata."""
    arrow_schema = pa.Table.from_pandas(df, preserve_index=False).schema
    return [{"name": f.name, "type": str(f.type)} for f in arrow_schema]


# ================== WRITE METADATA ==================
def write_metadata(kind: str, stats: dict):
    """
    Ghi metadata/manifest.json.

    stats: {month: DataFrame | {"rows": int, "schema": list}}
      - DataFrame  : kiểu cũ (giữ đến khi chuyển hết sang stats)
      - dict       : kiểu mới (nhẹ RAM)
    """
    if not stats:
        raise RuntimeError(f"{kind}: không có dữ liệu để ghi metadata (stats rỗng).")

    # Chuẩn hoá về {"rows": ..., "schema": ...}
    norm = {}
    for m, s in stats.items():
        if isinstance(s, pd.DataFrame):
            norm[m] = {"rows": len(s), "schema": schema_of(s)}
        else:
            norm[m] = s
    stats = norm

    manifest = {
        "kind":         kind,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "months":       sorted(stats.keys()),
        "total_rows":   sum(s["rows"] for s in stats.values()),
        "files": [
            {
                "month":  m,
                "key":    f"{SILVER_PREFIX}/{kind}/data/{kind}_{m}.parquet",
                "rows":   s["rows"],
                "schema": s["schema"],
            }
            for m, s in sorted(stats.items())
        ],
    }
    key = f"{SILVER_PREFIX}/{kind}/metadata/manifest.json"
    s3.put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8"),
    )
    print(f"    📄 s3://{BUCKET}/{key}")
# scripts/silver/common.py
# ... (giữ nguyên phần trên)

# ================== DISCOVER MONTHS ==================
def discover_bronze_months() -> list[str]:
    """
    Scan S3 bronze để tìm các tháng có đủ 3 file nguồn:
      - posts/normal.jsonl
      - comments/normal.jsonl
      - posts/botflag.jsonl

    Trả về list sorted: ["2026-06", "2026-07", "2026-08", ...]
    """
    import re
    from collections import defaultdict

    paginator = s3.get_paginator("list_objects_v2")
    prefix = f"{BRONZE_PREFIX}/"

    # {kind: {month: has_file}}
    presence = defaultdict(lambda: defaultdict(set))   # {kind: {ym: {label}}}
    pattern = re.compile(
        r"^" + re.escape(BRONZE_PREFIX) + r"/(posts|comments)/"
        r"year=(\d{4})/month=(\d{2})/(\w+)\.jsonl$"
    )

    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            m = pattern.match(key)
            if not m:
                continue
            kind, year, mon, label = m.groups()
            ym = f"{year}-{mon}"
            # Chỉ tính file > 100 bytes (loại file rỗng)
            if obj["Size"] > 100:
                presence[kind][ym].add(label)

    # Yêu cầu: có đủ 3 file
    valid = []
    for ym in set(presence["posts"]) | set(presence["comments"]):
        posts_labels   = presence["posts"].get(ym, set())
        comment_labels = presence["comments"].get(ym, set())
        if ("normal" in posts_labels
                and "botflag" in posts_labels
                and "normal" in comment_labels):
            valid.append(ym)

    return sorted(valid)


SILVER_SOURCES = {
    "posts":     [("posts", "normal")],
    "comments":  [("comments", "normal")],
    "bot_users": [("posts", "botflag")],
}


from botocore.exceptions import BotoCoreError, ClientError

def _head(key):
    try:
        return s3.head_object(Bucket=BUCKET, Key=key)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return None
        raise
    except BotoCoreError as e:
        raise BronzeReadError(f"S3 head fail: {key}: {e}") from e


def needs_build(kind: str, ym: str) -> bool:
    """True nếu silver chưa có, hoặc bất kỳ nguồn bronze nào mới hơn silver."""
    silver = _head(f"{SILVER_PREFIX}/{kind}/data/{kind}_{ym}.parquet")
    if silver is None:
        return True
    y, m = ym.split("-")
    for src_kind, label in SILVER_SOURCES[kind]:
        b = _head(f"{BRONZE_PREFIX}/{src_kind}/year={y}/month={m}/{label}.jsonl")
        if b is not None and b["LastModified"] > silver["LastModified"]:
            return True
    return False


def months_needing_build(kind: str) -> list[str]:
    return [ym for ym in discover_bronze_months() if needs_build(kind, ym)]


def to_int(v, default=0):
    try:
        return int(v or 0)
    except (ValueError, TypeError):
        return default


def to_float(v, default=None):
    if v is None:
        return default
    try:
        return float(v)
    except (ValueError, TypeError):
        return default