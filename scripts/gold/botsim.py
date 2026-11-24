"""Đọc danh sách BotSim-24 chính thức để loại identity held-out khi export."""

import hashlib
from io import BytesIO
from pathlib import Path

import polars as pl
from botocore.exceptions import ClientError

from scripts.gold.common import (
    BOTSIM_N_BOT,
    BOTSIM_USERS_CSV_KEY,
    HELDOUT_PCT,
    assert_unique,
    botsim_bucket,
)
from scripts.silver.common import BUCKET, s3


BOTSIM_EXPECTED_TOTAL = 2907
BOTSIM_OFFICIAL_SHA256 = "b23a30dcbc2f9cfbbe5b9ebdf88c430eb66e74290033c2d95c2986b7808e7b47"


def read_botsim24() -> pl.DataFrame:
    """Ưu tiên file S3 nếu checksum khớp bản chính thức, rồi thử bản local."""
    key = BOTSIM_USERS_CSV_KEY
    try:
        with s3.get_object(Bucket=BUCKET, Key=key)["Body"] as stream:
            body = stream.read()
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") not in ("NoSuchKey", "404", "NotFound"):
            raise
    else:
        if hashlib.sha256(body).hexdigest() == BOTSIM_OFFICIAL_SHA256:
            print(f"  Đọc từ s3://{BUCKET}/{key}")
            return pl.read_csv(BytesIO(body))
        print(f"  ⚠️  {key}: checksum khác file BotSim gốc, thử bản local đã xác minh")

    local = Path("data/raw/botsim24/Users.csv")
    if not local.is_file():
        raise RuntimeError(f"Cần bản BotSim-24 Users.csv chính thức tại {local}")
    body = local.read_bytes()
    assert hashlib.sha256(body).hexdigest() == BOTSIM_OFFICIAL_SHA256, "BotSim local không khớp checksum bản gốc"
    print(f"  Đọc {local}, checksum khớp BotSim gốc")
    return pl.read_csv(BytesIO(body))


def label_botsim(botsim: pl.DataFrame) -> pl.DataFrame:
    """
    Gán nhãn theo thứ tự dòng: BOTSIM_N_BOT dòng cuối là bot.
    Lưu ý: đúng chừng nào Users.csv giữ nguyên thứ tự gốc (chưa bị sắp xếp lại hay lưu lại).
    Trả về: user_id (bs_<tên thường>), is_bot.
    """
    n = botsim.height
    if n < BOTSIM_N_BOT:
        raise RuntimeError(f"Users.csv chỉ có {n} dòng, ít hơn {BOTSIM_N_BOT} bot.")
    assert n == BOTSIM_EXPECTED_TOTAL, f"Users.csv có {n:,} dòng, kỳ vọng {BOTSIM_EXPECTED_TOTAL:,}"

    df = (
        botsim.with_row_index("_row")
        .with_columns(
            (pl.col("_row") >= n - BOTSIM_N_BOT).cast(pl.Int64).alias("is_bot"),
            (pl.lit("bs_") + pl.col("name").cast(pl.String).str.to_lowercase()).alias("user_id"),
        )
        .filter(pl.col("name").is_not_null())
        .select(["user_id", "is_bot"])
    )

    duplicate_ids = df.group_by("user_id").len().filter(pl.col("len") > 1).select("user_id")
    if duplicate_ids.height:
        duplicate_rows = df.join(duplicate_ids, on="user_id", how="semi")
        print(f"  Loại {duplicate_rows.height:,} user BotSim trùng sau lowercase")
        df = df.join(duplicate_ids, on="user_id", how="anti")
    assert_unique(df, ["user_id"], "botsim")

    n_bot = int(df["is_bot"].sum())
    print(f"  BotSim: {df.height - n_bot:,} người / {n_bot:,} bot (theo {BOTSIM_N_BOT} dòng cuối)")
    return df


def build_botsim_heldout(botsim: pl.DataFrame) -> pl.DataFrame:
    df = label_botsim(botsim).with_columns(
        pl.col("user_id").map_elements(botsim_bucket, return_dtype=pl.Int64).alias("_bucket")
    )
    parts = []
    for label_val, name in [(0, "người"), (1, "bot")]:
        sub = df.filter(pl.col("is_bot") == label_val)
        held = sub.filter(pl.col("_bucket") < HELDOUT_PCT)
        print(f"    {name}: {held.height:,} / {sub.height:,} giữ làm held-out "
              f"({held.height / max(sub.height, 1):.0%})")
        parts.append(held)
    return (
        pl.concat(parts)
        .with_columns(
            pl.lit("botsim_heldout").alias("eval_role"),
            pl.col("is_bot").cast(pl.Int64).alias("eval_label"),
        )
        .select(["user_id", "eval_role", "eval_label"])
    )
