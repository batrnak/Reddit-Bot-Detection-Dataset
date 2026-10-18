"""
Build bảng gold.eval_users: danh sách user dùng để đánh giá, KHÔNG dùng để train.

Nguồn:
  1. botbouncer_B  : nửa B của community flags, chỉ giữ user có trong eligible set.
                     eval_label = 1. Tập so sánh (user thường) KHÔNG nằm trong bảng này,
                     tính lúc đánh giá từ eligible_users trừ đi mọi user bị flag.
  2. botsim_heldout: 30% mỗi lớp của BotSim-24, chia theo hash.
                     Nhãn: 1.000 dòng CUỐI của Users.csv là bot, phần còn lại là người.
                     user_id có tiền tố "bs_". Các user này KHÔNG có trong users/user_features
                     của Reddit, cần đường đánh giá riêng (feature tính từ dữ liệu BotSim).

Cờ:
  --no-botsim : chỉ build botbouncer_B. Mặc định FAIL nếu không đọc được BotSim.

Thứ tự chạy: sau build_user_features_table.py (để đếm số user dùng được).
Schema: user_id string | eval_role string | eval_label bigint
"""

import argparse
from io import BytesIO
from typing import List

# pyrefly: ignore[missing-import]
import polars as pl
from botocore.exceptions import ClientError

from scripts.gold.common import (
    assert_unique,
    drop_bad_authors,
    eligible_users,
    flags_half_a,
    flags_half_b,
    iter_silver,
    read_gold,
    stable_bucket,
    write_gold,
)
from scripts.silver.common import BUCKET, s3

BOTSIM_S3_KEYS = ["bronze/botsim24/raw/Users.csv"]
BOTSIM_N_BOT = 1000            # 1.000 dòng cuối là bot
BOTSIM_EXPECTED_TOTAL = 2907   # 1.907 người + 1.000 bot (chỉ để cảnh báo)
SALT_BOTSIM_HELDOUT = "botsim_heldout_v1"
HELDOUT_PCT = 30               # bucket < 30 là held-out; train BotSim phải dùng bucket >= 30


# ------------------------------------------------------------------ flags
def _read_all_flags() -> pl.DataFrame:
    """Toàn bộ user bị flag (cùng danh sách đầu vào cho nửa A và nửa B)."""
    parts: List[pl.DataFrame] = []
    for _, df in iter_silver("bot_users", columns=["username", "flagged_at"]):
        parts.append(drop_bad_authors(df, col="username"))
    if not parts:
        return pl.DataFrame(schema={"user_id": pl.String})
    flags = pl.concat(parts).rename({"username": "user_id"})
    return flags.filter(pl.col("flagged_at").is_not_null()).select("user_id").unique()


# ------------------------------------------------------------------ BotSim
def _read_botsim24_from_s3() -> pl.DataFrame:
    """Đọc Users.csv. Báo lỗi rõ nếu không thấy, không bỏ qua ngầm."""
    tried = []
    for key in BOTSIM_S3_KEYS:
        try:
            body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        except ClientError as e:
            if e.response.get("Error", {}).get("Code", "") in ("NoSuchKey", "404", "NotFound"):
                tried.append(key)
                continue
            raise   # lỗi khác (credentials, mạng): dừng, không thử key khác
        print(f"  Đọc từ s3://{BUCKET}/{key}")
        df = pl.read_csv(BytesIO(body))
        if "name" not in df.columns:
            raise RuntimeError(f"Users.csv không có cột 'name'. Các cột: {df.columns}")
        return df
    raise RuntimeError(
        f"Không thấy BotSim trên S3. Đã thử: {tried}. "
        f"Kiểm tra tên: aws s3 ls s3://{BUCKET}/botsim/ --recursive. "
        "Dùng --no-botsim nếu cố ý bỏ qua."
    )


def label_botsim(botsim: pl.DataFrame) -> pl.DataFrame:
    """
    Gán nhãn theo thứ tự dòng: BOTSIM_N_BOT dòng cuối là bot.
    Lưu ý: đúng chừng nào Users.csv giữ nguyên thứ tự gốc (chưa bị sắp xếp lại hay lưu lại).
    Trả về: user_id (bs_<tên thường>), is_bot.
    """
    n = botsim.height
    if n < BOTSIM_N_BOT:
        raise RuntimeError(f"Users.csv chỉ có {n} dòng, ít hơn {BOTSIM_N_BOT} bot.")
    if n != BOTSIM_EXPECTED_TOTAL:
        print(f"  ⚠️  Users.csv có {n:,} dòng, kỳ vọng {BOTSIM_EXPECTED_TOTAL:,}. "
              f"Vẫn coi {BOTSIM_N_BOT} dòng cuối là bot.")

    df = (
        botsim.with_row_index("_row")
        .with_columns(
            (pl.col("_row") >= n - BOTSIM_N_BOT).cast(pl.Int64).alias("is_bot"),
            (pl.lit("bs_") + pl.col("name").cast(pl.String).str.to_lowercase()).alias("user_id"),
        )
        .filter(pl.col("name").is_not_null())
        .select(["user_id", "is_bot"])
    )

    dup = df.height - df["user_id"].n_unique()
    if dup:
        raise RuntimeError(f"BotSim có {dup} user_id trùng sau khi chuẩn hoá tên. Kiểm tra Users.csv.")

    n_bot = int(df["is_bot"].sum())
    print(f"  BotSim: {df.height - n_bot:,} người / {n_bot:,} bot (theo {BOTSIM_N_BOT} dòng cuối)")
    return df


def botsim_bucket(user_id: str) -> int:
    """Phép chia dùng chung với bước huấn luyện BotSim: held-out nếu < HELDOUT_PCT."""
    return stable_bucket(user_id, SALT_BOTSIM_HELDOUT, 100)


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


# ------------------------------------------------------------------ main
def build_eval_users_table(skip_botsim: bool = False):
    print(f"== build_eval_users_table (v4) {'[--no-botsim]' if skip_botsim else ''} ==")

    print("[1/4] Đọc eligible users và user_features...")
    users = eligible_users()
    print(f"  Eligible users: {users.height:,}")

    usable_users = None
    try:
        feats = read_gold("user_features", ["user_id", "has_enough_data"])
        usable_users = feats.filter(pl.col("has_enough_data")).select("user_id")
        print(f"  Users có has_enough_data=True: {usable_users.height:,}")
    except ClientError as e:
        print(f"  ⚠️  Chưa đọc được user_features ({e.response.get('Error', {}).get('Code', '')}). "
              "Số 'usable' sẽ không có, chạy lại sau khi build user_features.")

    print("[2/4] Community flags nửa B...")
    all_flags = _read_all_flags()
    fa, fb = flags_half_a(all_flags), flags_half_b(all_flags)
    assert fa.height + fb.height == all_flags.height, (
        f"Nửa A ({fa.height}) + nửa B ({fb.height}) != tổng ({all_flags.height}). "
        "Kiểm tra flags_half_a/b trong common.py"
    )
    print(f"  Tổng flagged: {all_flags.height:,} | nửa A (LF): {fa.height:,} | nửa B (đo): {fb.height:,}")

    eval_bb = (
        fb.join(users.select("user_id"), on="user_id", how="inner")
        .select("user_id")
        .with_columns(
            pl.lit("botbouncer_B").alias("eval_role"),
            pl.lit(1).cast(pl.Int64).alias("eval_label"),
        )
    )
    print(f"  Nửa B có trong eligible: {eval_bb.height:,} (loại {fb.height - eval_bb.height:,} user không có hoạt động)")

    # Nửa B không được trùng nửa A (user bỏ phiếu cho LF)
    clash = set(eval_bb["user_id"].to_list()) & set(fa["user_id"].to_list())
    assert not clash, f"{len(clash)} user vừa ở nửa A vừa ở nửa B"

    eval_bb_usable = None
    if usable_users is not None:
        eval_bb_usable = eval_bb.join(usable_users, on="user_id", how="inner")
        print(f"  USABLE (has_enough_data=True): {eval_bb_usable.height:,}  <- cỡ mẫu thực khi đánh giá")

    eval_botsim = None
    if skip_botsim:
        print("[3/4] BotSim held-out... bỏ qua (--no-botsim)")
    else:
        print("[3/4] BotSim held-out...")
        eval_botsim = build_botsim_heldout(_read_botsim24_from_s3())
        print(f"  BotSim held-out: {eval_botsim.height:,}")

    print("[4/4] Gộp và kiểm tra...")
    parts = [eval_bb] + ([eval_botsim] if eval_botsim is not None and eval_botsim.height else [])
    eval_users = pl.concat(parts)
    assert_unique(eval_users, ["user_id", "eval_role"], "eval_users")

    print("\n" + "=" * 70)
    print("EVAL USERS SUMMARY")
    print("=" * 70)
    summary = (
        eval_users.group_by(["eval_role", "eval_label"]).agg(pl.len().alias("count"))
        .sort(["eval_role", "eval_label"])
    )
    for r in summary.iter_rows(named=True):
        print(f"  {r['eval_role']:<16} {'bot' if r['eval_label'] == 1 else 'người':<6} {r['count']:>8,}")
    print(f"  {'Tổng':<16} {'':<6} {eval_users.height:>8,}")

    n_neg = users.join(all_flags, on="user_id", how="anti").height
    print("\n  Tập so sánh KHÔNG nằm trong bảng này. Khi đánh giá botbouncer_B:")
    print(f"    negatives = eligible không bị flag, có has_enough_data, cùng mức hoạt động "
          f"(~{n_neg:,} eligible không bị flag, có lẫn bot chưa bị bắt)")
    if eval_botsim is not None:
        print("  BotSim held-out không có trong users/user_features của Reddit: cần feature tính từ dữ liệu BotSim.")
        print(f"  Khi train bộ phân loại BotSim, chỉ dùng user có botsim_bucket >= {HELDOUT_PCT}.")

    bb_ids = set(eval_bb["user_id"].to_list())
    eligible_ids = set(users["user_id"].to_list())
    print(f"\n  botbouncer_B trong eligible: {len(bb_ids & eligible_ids):,}/{len(bb_ids):,}")
    if eval_botsim is not None:
        bs_ids = set(eval_botsim["user_id"].to_list())
        print(f"  botsim_heldout trùng user Reddit: {len(bs_ids & eligible_ids):,} (kỳ vọng 0)")

    print("\nGhi vào gold...")
    write_gold(eval_users, "eval_users")
    return eval_users


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Build gold.eval_users")
    ap.add_argument("--no-botsim", action="store_true", help="Chỉ build botbouncer_B")
    args = ap.parse_args()
    build_eval_users_table(skip_botsim=args.no_botsim)