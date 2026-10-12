# scripts/silver/build_bot_users.py
"""
Build silver → bot_users.parquet.

Tự động:
  - Scan bronze S3 tìm tháng có posts/botflag.jsonl
  - Skip tháng đã build silver (so mtime)
  - Chỉ build tháng mới hoặc tháng có bronze mới hơn

Chạy:
  python -m scripts.silver.build_bot_users           # auto
  python -m scripts.silver.build_bot_users 2026-09   # chỉ tháng 9
  python -m scripts.silver.build_bot_users --force   # rebuild hết
"""

import re
import sys
import pandas as pd

from scripts.silver.common import (
    BronzeReadError,
    SILVER_PREFIX,
    discover_bronze_months,
    months_needing_build,
    read_bronze_jsonl,
    upload_parquet,
    write_metadata,
    _to_ts,
)


# ================== REGEX EXTRACT ==================
# BotBouncer: title "Overview for {username}"
_RE_BB_TITLE = re.compile(r"Overview for (\S+)", re.I)

# u/{username} mention (TheseFuckingAccounts, RedditBotHunters)
_RE_U_MENTION = re.compile(r"\bu/([A-Za-z0-9_-]{3,20})\b")

# /user/{username} URL
_RE_USER_URL = re.compile(r"(?:/user/|/u/)([A-Za-z0-9_-]{3,20})", re.I)

SKIP_NAMES = {"automoderator", "deleted", "bot-bouncer", "[deleted]"}


def extract_username(obj: dict) -> str | None:
    """Trích xuất username bị flag từ 1 post botflag."""
    sub   = (obj.get("subreddit") or "").lower()
    title = obj.get("title") or ""
    body  = obj.get("selftext") or ""
    url   = obj.get("url") or ""
    text  = f"{title} {body}"

    username = None

    if sub == "botbouncer":
        m = _RE_BB_TITLE.search(title)
        if m:
            username = m.group(1)
        else:
            m = _RE_USER_URL.search(url)
            if m:
                username = m.group(1)
    else:
        # TheseFuckingAccounts / RedditBotHunters
        m = _RE_U_MENTION.search(text)
        if m:
            username = m.group(1)
        else:
            m = _RE_USER_URL.search(url) or _RE_USER_URL.search(text)
            if m:
                username = m.group(1)

    if not username:
        return None
    if username.lower() in SKIP_NAMES:
        return None
    return username


def extract_bot_row(obj: dict, month: str) -> dict | None:
    username = extract_username(obj)
    if not username:
        return None
    return {
        "username":         username,
        "source_subreddit": obj.get("subreddit"),
        "source_post_id":   obj.get("id"),
        "flair":            obj.get("link_flair_text"),
        "flagged_at":       _to_ts(obj.get("created_utc")),
        "ingest_month":     month,
    }


# ================== BUILD 1 THÁNG ==================
def build_bot_users_month(month: str) -> pd.DataFrame | None:
    """Đọc bronze posts/botflag tháng X → extract username → ghi parquet."""
    rows = []
    n_scanned = 0
    n_extracted = 0

    for obj in read_bronze_jsonl("posts", month, "botflag"):
        n_scanned += 1
        r = extract_bot_row(obj, month)
        if r:
            rows.append(r)
            n_extracted += 1

    MIN_SCANNED_FOR_FAIL = 1000   # ngưỡng an toàn

    if not rows:
        if n_scanned >= MIN_SCANNED_FOR_FAIL:
            raise BronzeReadError(
                f"bot_users {month}: scan {n_scanned:,} posts nhưng 0 username extract được. "
                f"Regex có thể hỏng (Reddit đổi format?)."
            )
        print(f"    ⏭️  bot_users {month}: 0 username từ {n_scanned:,} posts (ít, bỏ qua)")
        return None

    df = pd.DataFrame(rows)
    df["username"] = df["username"].str.lower()
    df = df.drop_duplicates(subset=["username"]).sort_values("username").reset_index(drop=True)

    n_unique = len(df)

    upload_parquet(df, f"{SILVER_PREFIX}/bot_users/data/bot_users_{month}.parquet")

    print(f"    📊 scan {n_scanned:,} → extract {n_extracted:,} → unique {n_unique:,}")
    for sub, n in df["source_subreddit"].value_counts().items():
        print(f"       {sub}: {n:,}")

    return df


# ================== MAIN ==================
def main(months: list | None = None, force: bool = False):
    print("=" * 60)
    print("Build Silver → bot_users")
    print("=" * 60)

    # ---- Xác định tháng cần build ----
    if months:
        # User chỉ định tháng cụ thể
        months_to_build = months
        print(f"Chỉ định: {len(months_to_build)} tháng → {months_to_build}\n")
    else:
        # Auto-discover
        print("Scanning bronze S3...")
        all_months = discover_bronze_months()
        if not all_months:
            raise RuntimeError("Không tìm thấy tháng nào trong bronze.")

        if force:
            months_to_build = all_months
            print(f"  → {len(all_months)} tháng: {all_months} (--force)\n")
        else:
            months_to_build = months_needing_build("bot_users")
            skipped = sorted(set(all_months) - set(months_to_build))
            print(f"  → {len(all_months)} tháng trong bronze: {all_months}")
            if skipped:
                print(f"  → Skip {len(skipped)} tháng đã build: {skipped}")
            if months_to_build:
                print(f"  → Build {len(months_to_build)} tháng: {months_to_build}\n")
            else:
                print(f"  → Không có tháng mới cần build.\n")
                return

    # ---- Build từng tháng ----
    dfs = {}
    failed = []

    for month in months_to_build:
        print(f"\n=== {month} ===")
        try:
            df = build_bot_users_month(month)
            if df is not None:
                dfs[month] = df
        except BronzeReadError as e:
            print(f"  ❌ {month}: {e}")
            failed.append(month)
        except Exception as e:
            print(f"  ❌ {month}: {type(e).__name__}: {e}")
            failed.append(month)

    # ---- Ghi metadata ----
    if dfs:
        print(f"\n=== Metadata ===")
        write_metadata("bot_users", dfs)

    # ---- Tổng kết ----
    print("\n" + "=" * 60)
    if failed:
        print(f"❌ THẤT BẠI {len(failed)}/{len(months_to_build)} tháng: {failed}")
        print("=" * 60)
        raise RuntimeError(f"Build fail {len(failed)} tháng: {failed}")

    if dfs:
        total = sum(len(d) for d in dfs.values())
        print(f"✅ BOT_USERS HOÀN TẤT — {total:,} rows ({len(dfs)} tháng)")
    else:
        print(f"✅ Không có tháng nào cần build.")
    print("=" * 60)


if __name__ == "__main__":
    args = sys.argv[1:]
    force = "--force" in args
    months = [a for a in args if not a.startswith("--")] or None
    main(months, force=force)