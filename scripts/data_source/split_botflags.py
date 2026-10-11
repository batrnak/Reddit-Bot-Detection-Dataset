# scripts/ingest/split_botflag.py
"""
Tách 3 sub bot flag ra khỏi 15 sub normal.
Pipeline: bytes pre-filter | json.loads check TOP-LEVEL subreddit | ghi 2 file.

Không dùng substring để quyết định label (tránh crosspost_parent_list match nhầm).
"""

import json
from pathlib import Path

LABEL_SUBS = ["BotBouncer", "TheseFuckingAccounts", "RedditBotHunters"]
LABEL_LOWER = {s.lower() for s in LABEL_SUBS}

# Pre-filter bytes — chỉ để skip nhanh, KHÔNG dùng để quyết định label
NEEDLES = []
for s in LABEL_SUBS:
    NEEDLES.append(f'"subreddit":"{s}"'.encode("utf-8").lower())
    NEEDLES.append(f'"subreddit": "{s}"'.encode("utf-8").lower())

FILTERED_DIR = Path("C:/arctic_filtered")


def split_file(src: Path, dst_bot: Path, dst_normal: Path):
    # Skip nếu output còn mới hơn src (không stale)
    if dst_bot.exists() and dst_normal.exists():
        if src.stat().st_mtime <= dst_bot.stat().st_mtime:
            print(f"  ✅ {src.name} đã tách & mới hơn → skip.")
            return
        print(f"  → {src.name} mới hơn output cũ → tách lại.")
        dst_bot.unlink()
        dst_normal.unlink()

    tmp_bot = dst_bot.with_suffix(".tmp")
    tmp_normal = dst_normal.with_suffix(".tmp")
    tmp_bot.unlink(missing_ok=True)
    tmp_normal.unlink(missing_ok=True)

    print(f"  → Đang tách {src.name} ({src.stat().st_size/1024/1024:.0f} MB)...")

    n_bot = n_normal = 0
    prefilter_hits = parse_err = 0
    no_sub_field = 0

    with open(src, "rb") as fin, \
         open(tmp_bot, "wb") as fbot, \
         open(tmp_normal, "wb") as fnormal:
        for line in fin:
            # Pre-filter: bỏ dòng không chứa needle nào → giảm json.loads 99%
            lower = line.lower()
            if not any(n in lower for n in NEEDLES):
                fnormal.write(line)
                n_normal += 1
                continue

            prefilter_hits += 1

            # Có needle → parse JSON để check top-level subreddit
            try:
                obj = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                parse_err += 1
                # Không parse được → cho vào normal (an toàn cho label)
                fnormal.write(line)
                n_normal += 1
                continue

            if not isinstance(obj, dict):
                fnormal.write(line)
                n_normal += 1
                continue

            sub = obj.get("subreddit")
            if not isinstance(sub, str):
                no_sub_field += 1
                fnormal.write(line)
                n_normal += 1
                continue

            # QUYẾT ĐỊNH LABEL dựa vào TOP-LEVEL subreddit
            if sub.lower() in LABEL_LOWER:
                fbot.write(line)
                n_bot += 1
            else:
                # Có needle ở đâu đó (crosspost, body, title) nhưng
                # top-level không phải label sub → normal
                fnormal.write(line)
                n_normal += 1

    # Atomic replace
    tmp_bot.replace(dst_bot)
    tmp_normal.replace(dst_normal)

    total = n_bot + n_normal
    print(f"    ✅ bot={n_bot:,}, normal={n_normal:,}, tổng={total:,}")
    print(f"       pre-filter hits={prefilter_hits:,}, parse_err={parse_err:,}, no_sub={no_sub_field:,}")


if __name__ == "__main__":
    for month in ["2026-06", "2026-07", "2026-08"]:
        for kind in ["posts", "comments"]:
            src = FILTERED_DIR / f"{kind}_{month}.jsonl"
            if not src.exists():
                print(f"  ⏭️  {src.name} chưa có.")
                continue

            dst_bot    = FILTERED_DIR / f"split" / f"{kind}_{month}_botflag.jsonl"
            dst_normal = FILTERED_DIR / f"split" / f"{kind}_{month}_normal.jsonl"

            split_file(src, dst_bot, dst_normal)

    print("\n✅ Xong.")