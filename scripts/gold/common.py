# scripts/gold/common.py

import hashlib
from datetime import datetime
from io import BytesIO
import tempfile
from pathlib import Path
from typing import List, Optional

# pyrefly: ignore [missing-import]
import polars as pl
from botocore.exceptions import ClientError

from scripts.silver.common import BUCKET, SILVER_PREFIX, s3

GOLD_PREFIX    = "gold"
BACKUP_PREFIX  = "gold/_backups"   # ngoài prefix mọi bảng, Athena không scan
SKIP_AUTHORS   = ["[deleted]", "automoderator"]
MIN_EVENTS     = 4

SALT_SPLIT = "split_v1"
SALT_FLAG  = "flag_v1"
# ---------- BotSim-24 ----------
BOTSIM_USERS_CSV_KEY = "bronze/botsim24/raw/Users.csv"
BOTSIM_N_BOT = 1000
SALT_BOTSIM_HELDOUT = "botsim_heldout_v1"
HELDOUT_PCT = 30


# ---------- hash ổn định ----------
def stable_bucket(user_id: str, salt: str, n: int = 100) -> int:
    return int(hashlib.md5(f"{salt}:{user_id}".encode()).hexdigest(), 16) % n


def botsim_bucket(user_id: str) -> int:
    return stable_bucket(user_id, SALT_BOTSIM_HELDOUT, 100)

def flags_half_a(flags: pl.DataFrame) -> pl.DataFrame:
    """
    Nửa A (bucket 0) dùng để chọn nhãn Bot, nửa B (bucket 1) giữ cho đánh giá.
    PHẢI dùng cùng danh sách flags đầu vào với flags_half_b.
    """
    return flags.filter(
        pl.col("user_id").map_elements(
            lambda u: stable_bucket(u, SALT_FLAG, 2), return_dtype=pl.Int64
        ) == 0
    )


def flags_half_b(flags: pl.DataFrame) -> pl.DataFrame:
    """
    Nửa B (bucket 1) dùng để evaluate. KHÔNG dùng làm LF.
    PHẢI dùng cùng danh sách flags đầu vào với flags_half_a.
    """
    return flags.filter(
        pl.col("user_id").map_elements(
            lambda u: stable_bucket(u, SALT_FLAG, 2), return_dtype=pl.Int64
        ) == 1
    )

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
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "silver.parquet"
            s3.download_file(BUCKET, key, str(path))
            yield key, pl.read_parquet(path, columns=columns)


def drop_bad_authors(df: pl.DataFrame, col: str = "author_name") -> pl.DataFrame:
    return df.filter(pl.col(col).is_not_null() & ~pl.col(col).is_in(SKIP_AUTHORS))


def read_flags(*, banned_source: str | None = None) -> pl.DataFrame:
    """Tổng hợp mọi flair và cutoff. banned_source chỉ giới hạn nguồn của is_banned."""
    parts = []
    columns = ["username", "flair", "flagged_at"]
    is_banned = pl.col("flair") == "banned"
    if banned_source is not None:
        columns.append("source_subreddit")
        is_banned = is_banned & (pl.col("source_subreddit").str.to_lowercase() == banned_source.lower())
    for _, df in iter_silver("bot_users", columns):
        df = drop_bad_authors(df, "username").with_columns(
            pl.col("username").str.to_lowercase(),
            pl.col("flair").str.to_lowercase().str.strip_chars(),
        )
        parts.append(df.group_by("username").agg(
            is_banned.fill_null(False).any().alias("is_banned"),
            pl.col("flair").is_in(["organic", "service"]).fill_null(False).any().alias("is_benign"),
            pl.col("flagged_at").min().alias("flagged_at"),
        ))
    assert parts, "Không có bot_users"
    flags = pl.concat(parts).group_by("username").agg(
        pl.col("is_banned").any(), pl.col("is_benign").any(),
        pl.col("flagged_at").min(),
    ).rename({"username": "user_id"})
    assert_unique(flags, ["user_id"], "flags")
    return flags


def positive_flags(flags: pl.DataFrame) -> pl.DataFrame:
    return flags.filter(pl.col("is_banned") & ~pl.col("is_benign"))


def eval_exclusion_users(eval_users: pl.DataFrame) -> pl.DataFrame:
    """Loại cả alias Reddit của human BotSim heldout; bot BotSim là simulation."""
    ids = eval_users.select("user_id")
    humans = eval_users.filter(
        (pl.col("eval_role") == "botsim_heldout") & (pl.col("eval_label") == 0)
    ).select("user_id")
    assert humans.filter(~pl.col("user_id").str.starts_with("bs_")).is_empty(), "BotSim human thiếu prefix"
    aliases = humans.select(pl.col("user_id").str.slice(3).alias("user_id"))
    return pl.concat([ids, aliases]).unique()


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


def assert_unique(df: pl.DataFrame, cols: List[str], name: str) -> None:
    n = df.select(cols).unique().height
    assert n == df.height, f"{name}: trùng khoá {cols} ({df.height - n} dòng)"
