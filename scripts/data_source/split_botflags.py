# scripts/ingest/split_botflag.py
"""
Tách dữ liệu thành:
  - posts:    {kind}_{month}_botflag.jsonl  +  {kind}_{month}_normal.jsonl
  - comments: chỉ giữ {kind}_{month}_normal.jsonl (bỏ botflag)

Lý do: comment trong 3 sub bot-flag là comment của người nói về bot,
không phải comment của bot → vô dụng cho label.
"""

import json
from pathlib import Path

LABEL_SUBS = ["BotBouncer", "TheseFuckingAccounts", "RedditBotHunters"]
LABEL_LOWER = {s.lower() for s in LABEL_SUBS}

NEEDLES = []
for s in LABEL_SUBS:
    NEEDLES.append(f'"subreddit":"{s}"'.encode("utf-8").lower())
    NEEDLES.append(f'"subreddit": "{s}"'.encode("utf-8").lower())

FILTERED_DIR = Path("C:/arctic_filtered")
SPLIT_DIR    = FILTERED_DIR / "split"


def _should_skip(src: Path, *dsts: Path) -> bool:
    """Skip nếu tất cả dst tồn tại và mới hơn src."""
    if not all(d.exists() for d in dsts):
        return False
    if src.stat().st_mtime <= max(d.stat().st_mtime for d in dsts):
        return True
    return False


def split_posts(src: Path, dst_bot: Path, dst_normal: Path):
    """Tách posts thành 2 file bot/normal (check top-level subreddit)."""
    if _should_skip(src, dst_bot, dst_normal):
        print(f"  ✅ {src.name} đã tách & mới hơn → skip.")
        return
    for d in (dst_bot, dst_normal):
        d.unlink(missing_ok=True)

    tmp_bot    = dst_bot.with_suffix(".tmp")
    tmp_normal = dst_normal.with_suffix(".tmp")
    tmp_bot.unlink(missing_ok=True)
    tmp_normal.unlink(missing_ok=True)

    print(f"  → Tách {src.name} ({src.stat().st_size/1024/1024:.0f} MB)...")

    n_bot = n_normal = prefilter_hits = parse_err = no_sub = 0

    with open(src, "rb") as fin, \
         open(tmp_bot, "wb") as fbot, \
         open(tmp_normal, "wb") as fnormal:
        for line in fin:
            lower = line.lower()
            if not any(n in lower for n in NEEDLES):
                fnormal.write(line); n_normal += 1
                continue
            prefilter_hits += 1
            try:
                obj = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                parse_err += 1
                fnormal.write(line); n_normal += 1
                continue
            if not isinstance(obj, dict):
                fnormal.write(line); n_normal += 1
                continue
            sub = obj.get("subreddit")
            if not isinstance(sub, str):
                no_sub += 1
                fnormal.write(line); n_normal += 1
                continue
            if sub.lower() in LABEL_LOWER:
                fbot.write(line); n_bot += 1
            else:
                fnormal.write(line); n_normal += 1

    tmp_bot.replace(dst_bot)
    tmp_normal.replace(dst_normal)
    print(f"    ✅ bot={n_bot:,}, normal={n_normal:,}, tổng={n_bot+n_normal:,}")
    print(f"       pre-filter hits={prefilter_hits:,}, parse_err={parse_err:,}, no_sub={no_sub:,}")


def split_comments_normal(src: Path, dst_normal: Path):
    """Comments: chỉ giữ sub normal, bỏ 3 sub bot (không tạo file botflag)."""
    if _should_skip(src, dst_normal):
        print(f"  ✅ {src.name} đã xử lý & mới hơn → skip.")
        return
    dst_normal.unlink(missing_ok=True)

    tmp = dst_normal.with_suffix(".tmp")
    tmp.unlink(missing_ok=True)

    print(f"  → Lọc {src.name} (bỏ 3 sub bot, {src.stat().st_size/1024/1024:.0f} MB)...")

    n_normal = n_dropped_bot = parse_err = 0

    with open(src, "rb") as fin, open(tmp, "wb") as fout:
        for line in fin:
            lower = line.lower()
            if not any(n in lower for n in NEEDLES):
                fout.write(line); n_normal += 1
                continue
            try:
                obj = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                parse_err += 1
                fout.write(line); n_normal += 1
                continue
            sub = obj.get("subreddit", "") if isinstance(obj, dict) else ""
            if isinstance(sub, str) and sub.lower() in LABEL_LOWER:
                n_dropped_bot += 1
            else:
                fout.write(line); n_normal += 1

    tmp.replace(dst_normal)
    print(f"    ✅ normal={n_normal:,}, dropped(bot)={n_dropped_bot:,}, parse_err={parse_err:,}")


if __name__ == "__main__":
    SPLIT_DIR.mkdir(parents=True, exist_ok=True)

    for month in ["2026-06", "2026-07", "2026-08"]:
        print(f"\n===== {month} =====")

        # Posts: tách bot + normal
        src_p = FILTERED_DIR / f"posts_{month}.jsonl"
        if src_p.exists():
            split_posts(
                src_p,
                SPLIT_DIR / f"posts_{month}_botflag.jsonl",
                SPLIT_DIR / f"posts_{month}_normal.jsonl",
            )
        else:
            print(f"  ⏭️  {src_p.name} chưa có.")

        # Comments: chỉ ra normal
        src_c = FILTERED_DIR / f"comments_{month}.jsonl"
        if src_c.exists():
            split_comments_normal(
                src_c,
                SPLIT_DIR / f"comments_{month}_normal.jsonl",
            )
        else:
            print(f"  ⏭️  {src_c.name} chưa có.")

    print("\n✅ Xong.")