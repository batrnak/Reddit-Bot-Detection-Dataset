"""
Build bảng gold.user_features: 9 feature cho model + 4 feature chỉ cho LF.

Model : log_events, n_subs, hour_entropy, iat_median, iat_cv, burstiness,
        len_median, dup_ratio, link_ratio
LF    : lf_name_autogen, lf_dup_count, lf_burst_count, lf_coord_score

Quy tắc:
  - Với user bị flag, chỉ tính từ hoạt động TRƯỚC flagged_at (chống rò rỉ nhãn).
  - feature_cutoff_ts = flagged_at nếu bị flag, ngược lại = mốc cuối dữ liệu chung.
  - lf_coord_score = 0 cho đến khi có graph_edges. Đừng viết LF dùng cột này trước đó.

Input : silver/{posts,comments,bot_users}, gold/users
Output: gold/user_features
"""

from collections import Counter
from typing import List

# pyrefly: ignore [missing-import]
import numpy as np
# pyrefly: ignore [missing-import]
import polars as pl

from scripts.gold.common import (
    MIN_EVENTS,
    assert_unique,
    drop_bad_authors,
    eligible_users,
    iter_silver,
    write_gold,
)

UTC_US = pl.Datetime("us", "UTC")

# Nhận diện link ngoài Reddit (regex không dùng lookahead để hợp lệ trong polars)
_REDDIT_DOMAINS = r"(?:^|[/.])(?:reddit\.com|redd\.it|redditmedia\.com|redditstatic\.com)"
_REDDIT_LINK = (
    r"(?i)https?://(?:[a-z0-9-]+\.)?"
    r"(?:reddit\.com|redd\.it|redditmedia\.com|redditstatic\.com)\S*"
)

OUT_SCHEMA = {
    "user_id": pl.String,
    "has_enough_data": pl.Boolean,
    "log_events": pl.Float64,
    "n_subs": pl.Int64,
    "hour_entropy": pl.Float64,
    "iat_median": pl.Float64,
    "iat_cv": pl.Float64,
    "burstiness": pl.Float64,
    "len_median": pl.Float64,
    "dup_ratio": pl.Float64,
    "link_ratio": pl.Float64,
    "lf_name_autogen": pl.Boolean,
    "lf_dup_count": pl.Int64,
    "lf_burst_count": pl.Int64,
    "lf_coord_score": pl.Float64,
}


# ------------------------------------------------------------------ đọc dữ liệu
def _read_all_silver(kind: str, columns: List[str]) -> pl.DataFrame:
    parts = []
    for key, df in iter_silver(kind, columns=columns):
        df = drop_bad_authors(df, col="author_name")
        parts.append(df)
        print(f"    {key}: {df.height:,} rows")
    return pl.concat(parts)


def _read_flags() -> pl.DataFrame:
    """flagged_at sớm nhất cho mỗi user bị flag."""
    parts = [df for _, df in iter_silver("bot_users", columns=["username", "flagged_at"])]
    return (
        pl.concat(parts)
        .rename({"username": "user_id"})
        .filter(pl.col("flagged_at").is_not_null())
        .group_by("user_id")
        .agg(pl.col("flagged_at").min().cast(UTC_US).alias("flagged_at"))
    )


def _apply_cutoff(df: pl.DataFrame, users: pl.DataFrame, flags: pl.DataFrame) -> pl.DataFrame:
    """Giữ user eligible; với user bị flag chỉ giữ hoạt động trước flagged_at."""
    df = df.rename({"author_name": "user_id"})
    df = df.join(users.select("user_id"), on="user_id", how="semi")
    df = df.join(flags, on="user_id", how="left")
    return df.filter(
        pl.col("flagged_at").is_null() | (pl.col("created_utc") < pl.col("flagged_at"))
    ).drop("flagged_at")


# ------------------------------------------------------------------ hàm tính
def _hour_entropy(ts: np.ndarray) -> float:
    if ts.size == 0:
        return 0.0
    hours = (ts // 3600) % 24
    counts = np.bincount(hours, minlength=24)
    counts = counts[counts > 0].astype(float)
    p = counts / counts.sum()
    return float(-np.sum(p * np.log2(p))) + 0.0


def _iat_stats(ts: np.ndarray):
    """(median, cv, burstiness) của khoảng cách giữa 2 sự kiện (giây). None nếu thiếu dữ liệu."""
    if ts.size < 2:
        return None, None, None
    iats = np.diff(np.sort(ts)).astype(float)
    iats = iats[iats > 0]
    if iats.size == 0:
        return None, None, None
    median = float(np.median(iats))
    if iats.size < 2:
        return median, None, None
    mean, std = float(iats.mean()), float(iats.std())
    if mean <= 0:
        return median, None, None
    cv = std / mean
    burst = (std - mean) / (std + mean) if (std + mean) > 0 else 0.0
    return median, cv, burst


def _burst_count(ts: np.ndarray) -> int:
    """Số cửa sổ 6 sự kiện liên tiếp nằm trong 60 giây."""
    if ts.size < 6:
        return 0
    s = np.sort(ts)
    return int(np.sum((s[5:] - s[:-5]) <= 60))


def _is_autogen_name(name: str) -> bool:
    if not name:
        return False
    n_digits = sum(c.isdigit() for c in name)
    if n_digits >= 5:
        return True
    lower = name.lower()
    if lower.startswith(("user_", "bot_", "test_", "auto_", "temp_")):
        return True
    if lower.isalnum() and n_digits / max(len(name), 1) > 0.5:
        return True
    return False


# ------------------------------------------------------------------ main
def build_user_features_table() -> None:
    print("== build_user_features_table (v5) ==")

    print("[1/7] Đọc eligible users...")
    users = eligible_users()
    print(f"  Eligible users: {users.height:,}")

    print("[2/7] Đọc bot_users flags...")
    flags = _read_flags()
    print(f"  Flagged users: {flags.height:,}")

    print("[3/7] Đọc posts và comments...")
    posts = _read_all_silver(
        "posts", ["author_name", "subreddit", "created_utc", "title", "selftext", "url"]
    )
    comments = _read_all_silver(
        "comments", ["author_name", "subreddit", "created_utc", "body"]
    )

    # Mốc cuối dữ liệu chung, tính trước khi cắt theo flagged_at
    data_end = max(posts["created_utc"].max(), comments["created_utc"].max())
    print(f"  Mốc cuối dữ liệu: {data_end}")

    posts = _apply_cutoff(posts, users, flags)
    comments = _apply_cutoff(comments, users, flags)
    print(f"  Posts sau cutoff   : {posts.height:,}")
    print(f"  Comments sau cutoff: {comments.height:,}")

    print("[4/7] Chuẩn bị cột...")
    # ---- posts
    text = pl.concat_str(
        [pl.col("title").fill_null(""), pl.col("selftext").fill_null("")], separator=" "
    ).str.strip_chars()
    u = pl.col("url").fill_null("").str.to_lowercase()
    posts = posts.with_columns(text.alias("text")).with_columns(
        pl.col("created_utc").dt.epoch("s").alias("ts"),
        pl.col("text").str.len_chars().alias("text_len"),
        pl.when(pl.col("text").str.len_chars() > 0)
        .then(pl.col("text").hash(seed=42))
        .otherwise(None)
        .alias("text_hash"),
        (u.str.contains(r"^https?://") & ~u.str.contains(_REDDIT_DOMAINS)).alias("has_external_url"),
    ).select(["user_id", "subreddit", "ts", "text_len", "text_hash", "has_external_url"])

    # ---- comments
    body = pl.col("body").fill_null("")
    is_removed = body.str.to_lowercase().is_in(["[removed]", "[deleted]"])
    comments = comments.with_columns(
        pl.col("created_utc").dt.epoch("s").alias("ts"),
        # [removed]/[deleted] vẫn giữ timestamp cho feature thời gian, nhưng không tính độ dài
        pl.when(is_removed).then(None).otherwise(body.str.len_chars()).alias("text_len"),
        # chỉ comment đủ 50 ký tự mới tham gia kiểm tra trùng lặp
        pl.when(~is_removed & (body.str.len_chars() >= 50))
        .then(body.hash(seed=42))
        .otherwise(None)
        .alias("text_hash"),
        body.str.replace_all(_REDDIT_LINK, "").str.contains(r"(?i)https?://").alias("has_external_url"),
    ).select(["user_id", "subreddit", "ts", "text_len", "text_hash", "has_external_url"])

    print("[5/7] Gộp theo user...")
    n_subs_df = (
        pl.concat([posts.select("user_id", "subreddit"), comments.select("user_id", "subreddit")])
        .group_by("user_id")
        .agg(pl.col("subreddit").n_unique().alias("n_subs"))
    )
    post_agg = posts.group_by("user_id").agg(
        pl.col("ts").alias("post_ts"),
        pl.col("text_len").alias("post_lens"),
        pl.col("text_hash").drop_nulls().alias("post_hashes"),
        pl.col("has_external_url").sum().alias("post_url_count"),
        pl.len().alias("post_count"),
    )
    cmt_agg = comments.group_by("user_id").agg(
        pl.col("ts").alias("cmt_ts"),
        pl.col("text_len").alias("cmt_lens"),
        pl.col("text_hash").drop_nulls().alias("cmt_hashes"),
        pl.col("has_external_url").sum().alias("cmt_url_count"),
        pl.len().alias("cmt_count"),
    )
    merged = (
        users.select("user_id")
        .join(post_agg, on="user_id", how="left")
        .join(cmt_agg, on="user_id", how="left")
        .join(n_subs_df, on="user_id", how="left")
        .with_columns(
            pl.col(["post_count", "cmt_count", "post_url_count", "cmt_url_count", "n_subs"]).fill_null(0)
        )
    )

    print("[6/7] Tính feature cho từng user...")
    records = []
    for row in merged.iter_rows(named=True):
        uid = row["user_id"]
        ts = np.array((row["post_ts"] or []) + (row["cmt_ts"] or []), dtype=np.int64)

        iat_median, iat_cv, burstiness = _iat_stats(ts)

        lens = [l for l in (row["post_lens"] or []) + (row["cmt_lens"] or []) if l]
        len_median = float(np.median(lens)) if lens else 0.0

        hashes = (row["post_hashes"] or []) + (row["cmt_hashes"] or [])
        counts = Counter(hashes)
        dup_count = sum(c - 1 for c in counts.values() if c > 1)
        dup_ratio = dup_count / len(hashes) if hashes else 0.0

        n_events = int(row["post_count"]) + int(row["cmt_count"])
        n_links = int(row["post_url_count"]) + int(row["cmt_url_count"])
        link_ratio = n_links / n_events if n_events else 0.0

        records.append({
            "user_id": uid,
            "has_enough_data": n_events >= MIN_EVENTS,
            "log_events": float(np.log2(n_events + 1)),
            "n_subs": int(row["n_subs"]),
            "hour_entropy": _hour_entropy(ts),
            "iat_median": iat_median,
            "iat_cv": iat_cv,
            "burstiness": burstiness,
            "len_median": len_median,
            "dup_ratio": dup_ratio,
            "link_ratio": link_ratio,
            "lf_name_autogen": _is_autogen_name(uid),
            "lf_dup_count": dup_count,
            "lf_burst_count": _burst_count(ts),
            "lf_coord_score": 0.0,
        })
        if len(records) % 10000 == 0:
            print(f"    {len(records):,} / {merged.height:,}")

    features = pl.DataFrame(records, schema=OUT_SCHEMA)

    print("[7/7] Hoàn tất cột...")
    features = (
        features.with_columns(
            pl.col("iat_median").fill_null(0.0),
            pl.col("iat_cv").fill_null(0.0),
            pl.col("burstiness").fill_null(0.0),
        )
        .join(flags, on="user_id", how="left")
        .with_columns(
            pl.coalesce(pl.col("flagged_at"), pl.lit(data_end).cast(UTC_US)).alias("feature_cutoff_ts")
        )
        .select([
            "user_id", "feature_cutoff_ts", "has_enough_data",
            "log_events", "n_subs", "hour_entropy", "iat_median", "iat_cv",
            "burstiness", "len_median", "dup_ratio", "link_ratio",
            "lf_name_autogen", "lf_dup_count", "lf_burst_count", "lf_coord_score",
        ])
    )
    assert_unique(features, ["user_id"], "user_features")

    # ---- báo cáo và kiểm tra
    flagged_ok = (
        features.join(flags.select("user_id"), on="user_id", how="semi")
        .filter(pl.col("has_enough_data")).height
    )
    print("\n  Thống kê (mean):")
    for c in ["log_events", "n_subs", "hour_entropy", "iat_median", "iat_cv",
              "burstiness", "len_median", "dup_ratio", "link_ratio"]:
        print(f"    {c:<14}: {features[c].mean():.3f}")
    print(f"    lf_name_autogen : {int(features['lf_name_autogen'].sum()):,}")
    print(f"    has_enough_data : {int(features['has_enough_data'].sum()):,} / {features.height:,}")
    print(f"    Flagged có has_enough_data: {flagged_ok:,}  (positive thật dùng được)")
    print(f"    n_subs max: {features['n_subs'].max()}  (không thể vượt 15)")

    print("\n  Ghi vào gold...")
    write_gold(features, "user_features")


if __name__ == "__main__":
    build_user_features_table()