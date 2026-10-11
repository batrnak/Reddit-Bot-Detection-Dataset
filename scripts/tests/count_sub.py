# scripts/ingest/count_subs.py
"""Đếm phân bố subreddit — xuất ra file text để tránh tràn terminal."""

import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

FILTERED_DIR = Path("C:/arctic_filtered")
OUT_FILE = Path("C:/arctic_filtered/subreddit_counts.txt")

TARGET_SUBS = {
    "worldnews", "politics", "news", "geopolitics",
    "ukraine", "UkrainianConflict", "IsraelPalestine",
    "InternationalNews", "GlobalTalk", "UpliftingNews",
    "europe", "northkorea", "LessCredibleDefence",
    "Israel", "Palestine",
    "BotBouncer", "TheseFuckingAccounts", "RedditBotHunters",
}
TARGET_LOWER = {s.lower() for s in TARGET_SUBS}

FILES = []
for month in ["2026-06", "2026-07", "2026-08"]:
    for kind in ["posts", "comments"]:
        FILES.append(FILTERED_DIR / f"{kind}_{month}.jsonl")


def count_one(path: Path, out):
    """Đếm 1 file. Ghi kết quả vào `out` (file handle)."""
    if not path.exists():
        print(f"\n📁 {path.name}: KHÔNG TỒN TẠI", file=out)
        return

    size_mb = path.stat().st_size / 1024 / 1024
    print(f"\n📁 {path.name}  ({size_mb:.1f} MB)", file=out)
    print("-" * 70, file=out)

    counter_lower = Counter()
    original_name = {}
    parse_errors = 0
    non_dict = 0
    no_sub = 0
    total = 0

    with open(path, encoding="utf-8", errors="ignore") as f:
        for line in f:
            total += 1
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                parse_errors += 1
                continue
            if not isinstance(obj, dict):
                non_dict += 1
                continue
            sub = obj.get("subreddit")
            if not isinstance(sub, str) or not sub:
                no_sub += 1
                continue
            key = sub.lower()
            counter_lower[key] += 1
            original_name.setdefault(key, sub)

    matched = sum(v for k, v in counter_lower.items() if k in TARGET_LOWER)

    print(f"Tổng dòng:       {total:>12,}", file=out)
    print(f"Lỗi parse:       {parse_errors:>12,}", file=out)
    print(f"Không phải dict: {non_dict:>12,}", file=out)
    print(f"Không có sub:    {no_sub:>12,}", file=out)
    print(f"Match mục tiêu:  {matched:>12,}", file=out)
    print(file=out)

    # Phân bố — chỉ in TOP 50 để file gọn, đủ debug
    print("Phân bố subreddit (top 50):", file=out)
    for key, count in counter_lower.most_common(50):
        sub_display = original_name[key]
        in_target = "✅" if key in TARGET_LOWER else "🚨"
        print(f"  {in_target}  {count:>10,}  {sub_display}", file=out)

    # Sub mục tiêu thiếu
    missing = TARGET_LOWER - set(counter_lower.keys())
    if missing:
        print(f"\n⚠️  THIẾU {len(missing)} sub mục tiêu:", file=out)
        for s in sorted(missing):
            print(f"    - {s}", file=out)

    # Sub ngoài danh sách
    extra = set(counter_lower.keys()) - TARGET_LOWER
    if extra:
        print(f"\n🚨 CÓ {len(extra)} sub NGOÀI danh sách:", file=out)
        for key in sorted(extra):
            print(f"    - {original_name[key]}  (count={counter_lower[key]:,})", file=out)
    else:
        print("\n✅ Không có sub ngoài danh sách.", file=out)


def main():
    # Ghi ra file text
    with open(OUT_FILE, "w", encoding="utf-8") as out:
        print(f"Thời gian: {datetime.now().isoformat()}", file=out)
        print(f"Thư mục:   {FILTERED_DIR}", file=out)
        print(f"File log:  {OUT_FILE}", file=out)

        for path in FILES:
            count_one(path, out)

        print("\n" + "=" * 70, file=out)
        print("Xong.", file=out)

    # In tóm tắt ra terminal — chỉ 1 dòng
    print(f"✅ Xong. Kết quả đã ghi vào: {OUT_FILE}")
    print(f"   Kích thước file: {OUT_FILE.stat().st_size / 1024:.1f} KB")
    print()
    print("Xem bằng:")
    print(f"  notepad {OUT_FILE}")
    print(f"  code {OUT_FILE}")


if __name__ == "__main__":
    main()