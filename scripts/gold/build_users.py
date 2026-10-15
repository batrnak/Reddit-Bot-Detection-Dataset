"""
Build bảng gold.users: tập user chung với node_idx cố định và split cố định.

Quy trình:
  1. Đọc silver posts + comments từng tháng qua iter_silver (không nạp hết vào RAM).
  2. Group by author trong từng tháng, rồi cộng dồn n_posts / n_comments.
  3. Outer join hai bảng count → n_events = n_posts + n_comments.
  4. **PERSISTENT node_idx**: đọc users.parquet cũ, giữ node_idx cho user cũ,
     chỉ assign node_idx mới cho user mới (sequential từ max + 1).
  5. Tính eligible = (n_events >= MIN_EVENTS).
  6. Chia split theo hash ổn định (stable_bucket với SALT_SPLIT).
  7. Validate uniqueness, ghi S3 qua write_gold.

Input : silver/fact_posts/data/*.parquet, silver/fact_comments/data/*.parquet
Output: gold/users/users.parquet

Dependency: phải chạy sau khi silver layer đã hoàn tất.
"""

# pyrefly: ignore [missing-import]
import polars as pl

from scripts.gold.common import (
    MIN_EVENTS,
    SALT_SPLIT,
    assert_unique,
    drop_bad_authors,
    iter_silver,
    read_gold,
    stable_bucket,
    write_gold,
)


# ------------------------------------------------------------------ helpers
def _accumulate_counts(kind: str) -> pl.DataFrame:
    """
    Đọc từng tháng, group-by author, cộng dồn số events theo user.
    Trả về DataFrame 2 cột: [author_name, n].
    """
    monthly: list[pl.DataFrame] = []

    for key, df in iter_silver(kind, columns=["author_name"]):
        df = drop_bad_authors(df, col="author_name")
        if df.height == 0:
            continue
        month_agg = df.group_by("author_name").agg(pl.len().alias("n"))
        monthly.append(month_agg)
        print(f"    {key}: {df.height:,} dòng -> {month_agg.height:,} users")

    if not monthly:
        return pl.DataFrame(schema={"author_name": pl.String, "n": pl.UInt64})

    return (
        pl.concat(monthly)
        .group_by("author_name")
        .agg(pl.col("n").sum().alias("n"))
    )


def _assign_split(user_id: str) -> str:
    """Chia 70/15/15 theo hash, deterministic."""
    b = stable_bucket(user_id, SALT_SPLIT, n=100)
    if b < 70:
        return "train"
    if b < 85:
        return "val"
    return "test"


def _assign_persistent_node_idx(users: pl.DataFrame) -> pl.DataFrame:
    """
    Gán node_idx persistent: giữ node_idx cũ, assign mới cho user mới.
    """
    old_users = read_gold("users", ["user_id", "node_idx"], missing_ok=True)
    
    if old_users is not None and old_users.height > 0:
        print(f"  Found {old_users.height:,} existing users, preserving node_idx...")
        
        # Lấy max node_idx từ bảng cũ
        max_old_idx = old_users["node_idx"].max()
        if max_old_idx is None:
            max_old_idx = -1
        
        # Join để lấy node_idx cũ
        users = users.join(
            old_users.select(["user_id", "node_idx"]),
            on="user_id",
            how="left"
        )
        
        # Tìm users mới (node_idx is null sau left join)
        new_mask = users["node_idx"].is_null()
        n_new = new_mask.sum()
        
        if n_new > 0:
            print(f"  Assigning {n_new:,} new node_idx (from {max_old_idx + 1})...")
            
            # Tách users cũ và mới
            old_users_df = users.filter(~new_mask)
            new_users_df = users.filter(new_mask).sort("user_id")
            
            # Assign node_idx mới
            new_users_df = new_users_df.with_columns([
                pl.int_range(
                    max_old_idx + 1,
                    max_old_idx + 1 + n_new,
                    dtype=pl.Int64
                ).alias("node_idx")
            ])
            
            # Merge back
            users = pl.concat([old_users_df, new_users_df])
        else:
            print(f"  All {users.height:,} users have existing node_idx")
            # Không cần làm gì, node_idx đã có từ join
    
    else:
        print("  No existing users table, assigning node_idx from 0...")
        users = users.sort("user_id")
        users = users.with_columns([
            pl.int_range(0, users.height, dtype=pl.Int64).alias("node_idx")
        ])
    
    return users


# ------------------------------------------------------------------ main
def build_users_table() -> None:
    print("== build_users_table ==")

    # 1. Đếm posts và comments cho từng user
    print("[1/6] Đếm posts từ silver...")
    post_counts = _accumulate_counts("posts").rename({"n": "n_posts"})

    print("[2/6] Đếm comments từ silver...")
    comment_counts = _accumulate_counts("comments").rename({"n": "n_comments"})

    # 2. Outer join để có mọi user
    print("[3/6] Join post + comment counts...")
    users = (
        post_counts.join(comment_counts, on="author_name", how="full", coalesce=True)
        .fill_null(0)
        .rename({"author_name": "user_id"})
        .with_columns(
            pl.col("n_posts").cast(pl.UInt64),
            pl.col("n_comments").cast(pl.UInt64),
        )
        .with_columns((pl.col("n_posts") + pl.col("n_comments")).alias("n_events"))
        .drop(["n_posts", "n_comments"])
    )

    # 3. Assign persistent node_idx
    print("[4/6] Gán node_idx (persistent)...")
    users = _assign_persistent_node_idx(users)

    # 4. Tính eligible và split
    print("[5/6] Tính eligible và split...")
    users = users.with_columns([
        (pl.col("n_events") >= MIN_EVENTS).alias("eligible"),
        pl.col("user_id")
            .map_elements(_assign_split, return_dtype=pl.String)
            .alias("split"),
    ])

    users = users.select(["user_id", "node_idx", "n_events", "eligible", "split"])
    users = users.with_columns(pl.col("n_events").cast(pl.Int64))

    # 5. Validate
    assert_unique(users, ["user_id"], "users")
    assert_unique(users, ["node_idx"], "users")

    # 6. Báo cáo
    n_total    = users.height
    n_eligible = users.filter(pl.col("eligible")).height
    split_dist = (
        users.group_by("split")
             .agg([pl.len().alias("count"),
                   pl.col("eligible").sum().alias("eligible_count")])
             .sort("split")
    )

    print(f"  Tổng users           : {n_total:,}")
    print(f"  Eligible (>={MIN_EVENTS} events): {n_eligible:,} "
          f"({n_eligible / n_total * 100:.1f}%)")
    print("  Split distribution:")
    print(split_dist)

    # 7. Ghi
    print("[6/6] Ghi gold/users...")
    write_gold(users, "users")


if __name__ == "__main__":
    build_users_table()