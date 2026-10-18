"""
Build bảng gold.lf_votes: phiếu vote từ các labeling functions.

Các LFs:
  1. lf_community_flag   : Community flags (BotBouncer/TFA/RBH) nửa A (vote=1)
  2. lf_burstiness       : burstiness > 0.8, chỉ users đủ data (vote=1)
  3. lf_hour_entropy     : hour_entropy < 1.0, chỉ users đủ data (vote=1)
  4. lf_dup_ratio        : dup_ratio > 0.3, chỉ users đủ data (vote=1)
  5. lf_name_autogen     : lf_name_autogen == True (vote=1)
  6. lf_link_ratio       : link_ratio > 0.5, chỉ users đủ data (vote=1)

Vote convention:
  -1: abstain (không vote) — KHÔNG được ghi vào bảng
   0: human
   1: bot

Lưu ý:
  - Bảng chỉ chứa dòng khi LF bắn (vote != -1).
  - Label model phải pivot user × LF rồi điền -1 cho ô trống.
  - Nửa A của flags dùng cho LF, nửa B giữ cho eval (KHÔNG overlap).
  - Feature-based LFs chỉ hoạt động trên users đủ data (has_enough_data + log_events >= 4.4).

Input : silver/bot_users/data/*.parquet, gold/user_features/user_features.parquet
Output: gold/lf_votes/lf_votes.parquet
"""

from typing import List

# pyrefly: ignore[missing-import]
import polars as pl 

from scripts.gold.common import (
    assert_unique,
    drop_bad_authors,
    eligible_users,
    flags_half_a,
    iter_silver,
    read_gold,
    write_gold,
)

# Ngưỡng tối thiểu để LF feature-based hoạt động đáng tin cậy
# log2(21) ≈ 4.39 → khoảng 20 events
MIN_LOG_EVENTS = 4.4


# ------------------------------------------------------------------ readers
def _read_bot_users_flags() -> pl.DataFrame:
    """Đọc TOÀN BỘ community flags từ silver."""
    monthly: List[pl.DataFrame] = []
    for key, df in iter_silver("bot_users", columns=["username", "flagged_at"]):
        df = drop_bad_authors(df, col="username")
        monthly.append(df)
    if not monthly:
        return pl.DataFrame(schema={"user_id": pl.String, "flagged_at": pl.Datetime})
    flags = pl.concat(monthly).rename({"username": "user_id"})
    flags = flags.filter(pl.col("flagged_at").is_not_null())
    return flags.select("user_id").unique()


def _read_user_features() -> pl.DataFrame:
    """Đọc user_features từ gold."""
    features = read_gold("user_features", missing_ok=True)
    if features is None:
        raise RuntimeError(
            "Bảng 'user_features' chưa tồn tại. Chạy build_user_features_table.py trước."
        )
    return features


def _enough(features: pl.DataFrame) -> pl.DataFrame:
    """Filter users đủ data cho LF feature-based."""
    return features.filter(
        pl.col("has_enough_data") & (pl.col("log_events") >= MIN_LOG_EVENTS)
    )


# ------------------------------------------------------------------ labeling functions
def apply_lf_community_flag(users: pl.DataFrame, flags_a: pl.DataFrame) -> pl.DataFrame:
    """
    LF 1: Community flags (BotBouncer/TFA/RBH) nửa A.
    Vote = 1 (bot) cho users bị flag trong nửa A.
    """
    return (
        users.select("user_id")
        .join(flags_a, on="user_id", how="inner")
        .with_columns([
            pl.lit("lf_community_flag").alias("lf_name"),
            pl.lit(1).cast(pl.Int64).alias("vote"),
        ])
    )


def apply_lf_burstiness(users: pl.DataFrame, features: pl.DataFrame) -> pl.DataFrame:
    """
    LF 2: High burstiness (bursty behavior).
    Vote = 1 nếu burstiness > 0.8 và user đủ data.
    """
    return (
        _enough(features)
        .filter(pl.col("burstiness") > 0.8)
        .select("user_id")
        .with_columns([
            pl.lit("lf_burstiness").alias("lf_name"),
            pl.lit(1).cast(pl.Int64).alias("vote"),
        ])
    )


def apply_lf_hour_entropy(users: pl.DataFrame, features: pl.DataFrame) -> pl.DataFrame:
    """
    LF 3: Low hour entropy (regularity = bot).
    Vote = 1 nếu hour_entropy < 1.0 và user đủ data.
    """
    return (
        _enough(features)
        .filter(pl.col("hour_entropy") < 1.0)
        .select("user_id")
        .with_columns([
            pl.lit("lf_hour_entropy").alias("lf_name"),
            pl.lit(1).cast(pl.Int64).alias("vote"),
        ])
    )


def apply_lf_dup_ratio(users: pl.DataFrame, features: pl.DataFrame) -> pl.DataFrame:
    """
    LF 4: High duplicate ratio.
    Vote = 1 nếu dup_ratio > 0.3 và user đủ data.
    """
    return (
        _enough(features)
        .filter(pl.col("dup_ratio") > 0.3)
        .select("user_id")
        .with_columns([
            pl.lit("lf_dup_ratio").alias("lf_name"),
            pl.lit(1).cast(pl.Int64).alias("vote"),
        ])
    )


def apply_lf_name_autogen(users: pl.DataFrame, features: pl.DataFrame) -> pl.DataFrame:
    """
    LF 5: Auto-generated names.
    Vote = 1 nếu lf_name_autogen == True.
    (Không phụ thuộc hoạt động, không cần filter _enough)
    """
    return (
        features.filter(pl.col("lf_name_autogen"))
        .select("user_id")
        .with_columns([
            pl.lit("lf_name_autogen").alias("lf_name"),
            pl.lit(1).cast(pl.Int64).alias("vote"),
        ])
    )


def apply_lf_link_ratio(users: pl.DataFrame, features: pl.DataFrame) -> pl.DataFrame:
    """
    LF 6: High link ratio (spam links).
    Vote = 1 nếu link_ratio > 0.5 và user đủ data.
    """
    return (
        _enough(features)
        .filter(pl.col("link_ratio") > 0.5)
        .select("user_id")
        .with_columns([
            pl.lit("lf_link_ratio").alias("lf_name"),
            pl.lit(1).cast(pl.Int64).alias("vote"),
        ])
    )


# ------------------------------------------------------------------ main
def build_lf_votes_table() -> None:
    print("== build_lf_votes_table ==")

    # 1. Đọc eligible users
    print("[1/5] Đọc eligible users...")
    users = eligible_users()
    print(f"  Eligible users: {users.height:,}")

    # 2. Đọc community flags
    print("[2/5] Đọc community flags...")
    all_flags = _read_bot_users_flags()
    print(f"  Total flagged users: {all_flags.height:,}")

    # Chia nửa A/B (dùng hàm chung từ common.py)
    flags_a = flags_half_a(all_flags)
    flags_b_count = all_flags.height - flags_a.height
    
    # Verify: half A + half B = toàn bộ (phải đúng vì cùng hàm)
    assert flags_a.height + flags_b_count == all_flags.height, (
        f"Half A ({flags_a.height}) + Half B ({flags_b_count}) != Total ({all_flags.height})"
    )
    print(f"  Nửa A (LF): {flags_a.height:,}")
    print(f"  Nửa B (eval): {flags_b_count:,}")

    # 3. Đọc user_features
    print("[3/5] Đọc user_features...")
    features = _read_user_features()
    print(f"  Users with features: {features.height:,}")

    # 4. Apply tất cả LFs
    print("[4/5] Áp dụng labeling functions...")

    votes = []

    print("  [LF1] lf_community_flag...")
    lf1 = apply_lf_community_flag(users, flags_a)
    votes.append(lf1)
    print(f"    {lf1.height:,} votes")

    print("  [LF2] lf_burstiness...")
    lf2 = apply_lf_burstiness(users, features)
    votes.append(lf2)
    print(f"    {lf2.height:,} votes")

    print("  [LF3] lf_hour_entropy...")
    lf3 = apply_lf_hour_entropy(users, features)
    votes.append(lf3)
    print(f"    {lf3.height:,} votes")

    print("  [LF4] lf_dup_ratio...")
    lf4 = apply_lf_dup_ratio(users, features)
    votes.append(lf4)
    print(f"    {lf4.height:,} votes")

    print("  [LF5] lf_name_autogen...")
    lf5 = apply_lf_name_autogen(users, features)
    votes.append(lf5)
    print(f"    {lf5.height:,} votes")

    print("  [LF6] lf_link_ratio...")
    lf6 = apply_lf_link_ratio(users, features)
    votes.append(lf6)
    print(f"    {lf6.height:,} votes")

    # Merge tất cả votes
    all_votes = pl.concat(votes)

    # Validate
    assert_unique(all_votes, ["user_id", "lf_name"], "lf_votes")

    # Verify không overlap với nửa B (eval set)
    from scripts.gold.common import flags_half_b
    flags_b = flags_half_b(all_flags)
    eval_b_ids = set(flags_b["user_id"].to_list())
    lf_ids = set(all_votes["user_id"].to_list())
    overlap = lf_ids & eval_b_ids
    assert not overlap, (
        f"Có {len(overlap)} users vừa trong LF votes vừa trong eval (nửa B)! "
        "Kiểm tra flags_half_a/b trong common.py"
    )
    print(f"  Verify: 0 overlap giữa LF votes và eval nửa B ✅")

    # Report
    print("\n" + "=" * 70)
    print("LF SUMMARY")
    print("=" * 70)

    lf_summary = (
        all_votes.group_by("lf_name")
        .agg([
            pl.len().alias("n_votes"),
            pl.col("vote").mean().alias("avg_vote"),
        ])
        .sort("lf_name")
    )

    print(f"{'LF Name':<25} {'Votes':<10} {'Avg Vote':<10}")
    print("-" * 70)
    for row in lf_summary.iter_rows(named=True):
        print(f"{row['lf_name']:<25} {row['n_votes']:<10,} {row['avg_vote']:<10.3f}")

    print("\n" + "=" * 70)
    print("VOTE DISTRIBUTION")
    print("=" * 70)

    vote_dist = all_votes.group_by("vote").agg(pl.len().alias("count"))
    for row in vote_dist.iter_rows(named=True):
        vote_type = {-1: "abstain", 0: "human", 1: "bot"}[row["vote"]]
        print(f"  {vote_type}: {row['count']:,} ({row['count']/all_votes.height*100:.1f}%)")

    print("\n" + "=" * 70)
    print("USER COVERAGE")
    print("=" * 70)

    unique_users = all_votes.select("user_id").unique().height
    print(f"  Unique users with votes: {unique_users:,} ({unique_users/users.height*100:.1f}%)")
    print(f"  Total votes: {all_votes.height:,}")
    print(f"  Avg votes per user: {all_votes.height/unique_users:.2f}")

    # 5. Kiểm tra coverage và correlation với flags (chỉ trên nửa A)
    print("\n" + "=" * 70)
    print("LF QUALITY CHECK (trên nửa A)")
    print("=" * 70)

    fa = set(flags_a["user_id"].to_list())
    base = _enough(features).with_columns(
        pl.col("user_id").is_in(fa).alias("fa")
    )
    base_rate = float(base["fa"].mean())
    print(f"Nền (đủ dữ liệu): {base.height:,} user, tỉ lệ flag nửa A = {base_rate:.4f}")

    for lf in ["lf_community_flag", "lf_burstiness", "lf_hour_entropy",
               "lf_dup_ratio", "lf_name_autogen", "lf_link_ratio"]:
        v = all_votes.filter(pl.col("lf_name") == lf)["user_id"]
        sub = base.filter(pl.col("user_id").is_in(v))
        if sub.height:
            r = float(sub["fa"].mean())
            lift = r / base_rate if base_rate > 0 else 0
            print(f"  {lf:<20} bao phủ {sub.height:>8,}  flag {r:.4f}  (x{lift:.1f} so với nền)")
        else:
            print(f"  {lf:<20} bao phủ        0  (không có user nào)")

    # Write
    print("\nGhi vào gold...")
    write_gold(all_votes, "lf_votes")

    return all_votes


if __name__ == "__main__":
    build_lf_votes_table()