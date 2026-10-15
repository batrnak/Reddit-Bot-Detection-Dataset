# scripts/gold/common.py

import hashlib
from datetime import datetime
from io import BytesIO
from typing import List, Optional

# pyrefly: ignore [missing-import]
import polars as pl
from botocore.exceptions import ClientError

from scripts.silver.common import BUCKET, SILVER_PREFIX, s3

GOLD_PREFIX    = "gold"
BACKUP_PREFIX  = "gold/_backups"   # ngoài prefix mọi bảng, Athena không scan
SKIP_AUTHORS   = ["[deleted]", "automoderator"]
MIN_EVENTS     = 5

SALT_SPLIT = "split_v1"
SALT_FLAG  = "flag_v1"


# ---------- hash ổn định ----------
def stable_bucket(user_id: str, salt: str, n: int = 100) -> int:
    return int(hashlib.md5(f"{salt}:{user_id}".encode()).hexdigest(), 16) % n


# ---------- silver ----------
def silver_keys(kind: str) -> List[str]:
    keys_with_date = []
    pages = s3.get_paginator("list_objects_v2").paginate(
        Bucket=BUCKET, Prefix=f"{SILVER_PREFIX}/{kind}/data/"
    )
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                filename = obj["Key"].split("/")[-1]
                keys_with_date.append((filename, obj["Key"]))
    if not keys_with_date:
        raise RuntimeError(f"Không có file silver nào cho {kind}")
    keys_with_date.sort(key=lambda x: x[0])
    return [k for _, k in keys_with_date]


def iter_silver(kind: str, columns: List[str]):
    for key in silver_keys(kind):
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        yield key, pl.read_parquet(BytesIO(body), columns=columns)


def drop_bad_authors(df: pl.DataFrame, col: str = "author_name") -> pl.DataFrame:
    return df.filter(pl.col(col).is_not_null() & ~pl.col(col).is_in(SKIP_AUTHORS))


# ---------- gold ----------
def gold_key(table: str) -> str:
    return f"{GOLD_PREFIX}/{table}/{table}.parquet"


def read_gold(table: str, columns: Optional[List[str]] = None, missing_ok: bool = False):
    try:
        body = s3.get_object(Bucket=BUCKET, Key=gold_key(table))["Body"].read()
    except ClientError as e:
        if missing_ok and e.response["Error"]["Code"] in ("NoSuchKey", "404", "NotFound"):
            return None
        raise
    return pl.read_parquet(BytesIO(body), columns=columns)


def write_gold(df: pl.DataFrame, table: str, backup: bool = True) -> None:
    """Ghi đè cả bảng (idempotent). Backup file cũ ra ngoài prefix bảng để Athena không quét."""
    key = gold_key(table)

    # Backup: đưa ra gold/_backups/{table}/, không nằm trong gold/{table}/
    if backup:
        try:
            head = s3.head_object(Bucket=BUCKET, Key=key)
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            backup_key = f"{BACKUP_PREFIX}/{table}/{table}.{ts}.parquet"
            s3.copy_object(
                Bucket=BUCKET,
                CopySource={"Bucket": BUCKET, "Key": key},
                Key=backup_key,
            )
            print(f"  backup -> s3://{BUCKET}/{backup_key}  "
                  f"({head['ContentLength']/1024/1024:.1f} MB)")
        except ClientError as e:
            if e.response["Error"]["Code"] != "404":
                raise

    df = df.with_columns(pl.col(pl.Datetime).dt.cast_time_unit("us"))
    buf = BytesIO()
    df.write_parquet(buf, compression="snappy")
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    print(f"  ghi s3://{BUCKET}/{key}  ({df.height:,} dòng)")


def eligible_users() -> pl.DataFrame:
    """Tập user chung mọi bảng khác phải join vào: user_id, node_idx, split.
    Yêu cầu: build_users_table.py đã chạy trước.
    """
    users = read_gold("users", ["user_id", "node_idx", "eligible", "split"], missing_ok=True)
    if users is None:
        raise RuntimeError("Bảng 'users' chưa tồn tại. Chạy build_users_table.py trước.")
    return users.filter(pl.col("eligible")).select(["user_id", "node_idx", "split"])


def assert_unique(df: pl.DataFrame, cols: List[str], name: str) -> None:
    n = df.select(cols).unique().height
    assert n == df.height, f"{name}: trùng khoá {cols} ({df.height - n} dòng)"