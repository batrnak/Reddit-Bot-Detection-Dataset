"""Backfill 12 tháng bằng downloader, Bronze và Silver hiện có."""

import argparse
from pathlib import Path
import re
import shutil


def year_months(end_month):
    if not re.fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", end_month):
        raise ValueError("Tháng kết thúc phải có dạng YYYY-MM")
    year, month = map(int, end_month.split("-"))
    end = year * 12 + month - 1
    return tuple(f"{index // 12:04d}-{index % 12 + 1:02d}" for index in range(end - 11, end + 1))


def _prepare_month(month, work_dir):
    from scripts.data_source import download, split_botflags

    local = work_dir / month
    split_dir = local / "split"
    split_dir.mkdir(parents=True, exist_ok=True)
    download.TORRENT_DIR = work_dir / "torrents"

    def filter_dump(code, kind, filter_fn, minimum):
        download.DATA_DIR = local / code
        filtered = local / f"{kind}_{month}.jsonl"
        raw = download.zst_path(month, code)
        if not filtered.exists():
            existing = download.out_path(month, kind)
            if (code != "RS" or download.strict_marker(month).is_file()) and download.is_output_valid(existing, minimum):
                print(f"  → Dùng file đã lọc {existing}")
                return existing
            assert shutil.which("aria2c") and shutil.which("zstd"), "Thiếu aria2c hoặc zstd"
            if code == "RS":
                assert download.get_ripgrep_path(), "Thiếu ripgrep"
            torrent = download.download_torrent(month)
            assert torrent is not None, f"Thiếu torrent {month} trong monthnote"
            # DATA_DIR riêng theo kind, file aria2 tạo cho kind còn lại chỉ là file phụ.
            download.download_specific(torrent, month, {code}, remove_unselected=True)
            assert raw.is_file(), f"Thiếu dump {raw}"
            size = filter_fn(raw, filtered, f"{kind}-{month}", minimum)
            assert size is not None and size >= minimum, f"Filter {raw.name} chưa hoàn tất"
        assert filtered.stat().st_size > 100, f"File filter bị rỗng: {filtered}"
        # Chỉ dọn raw của kind đã filter xong trong thư mục backfill.
        download.delete_zst_safe(month, code)
        return filtered

    posts = filter_dump("RS", "posts", download.filter_posts_strict, download.MIN_POSTS_MATCH)
    split_botflags.split_posts(
        posts, split_dir / f"posts_{month}_botflag.jsonl", split_dir / f"posts_{month}_normal.jsonl",
    )
    comments = filter_dump("RC", "comments", download.filter_comments_python, download.MIN_COMMENTS_MATCH)
    split_botflags.split_comments_normal(comments, split_dir / f"comments_{month}_normal.jsonl")
    return split_dir


def _backfill_month(month, work_dir):
    from scripts.bronze import ingest2bronze
    from scripts.silver import build_bot_users, build_comments, build_posts
    from scripts.silver.common import BRONZE_PREFIX, BUCKET, SILVER_PREFIX, _head

    builders = {"posts": build_posts, "comments": build_comments, "bot_users": build_bot_users}
    silver = {kind: f"{SILVER_PREFIX}/{kind}/data/{kind}_{month}.parquet" for kind in builders}
    silver_heads = {kind: _head(key) for kind, key in silver.items()}
    assert all(meta is None or meta["ContentLength"] > 0 for meta in silver_heads.values()), (
        f"{month}: Silver có file rỗng"
    )
    missing = [kind for kind, meta in silver_heads.items() if meta is None]
    if not missing:
        print(f"{month}: đủ 3 file Silver, bỏ qua")
        return

    year, mon = month.split("-")
    bronze = {
        f"{kind}_{month}_{label}.jsonl": f"{BRONZE_PREFIX}/{kind}/year={year}/month={mon}/{label}.jsonl"
        for kind, labels in ingest2bronze.REQUIRED_LABELS.items()
        for label in labels
    }
    heads = {name: _head(key) for name, key in bronze.items()}
    assert all(meta is None or meta["ContentLength"] > 100 for meta in heads.values()), (
        f"{month}: Bronze có file rỗng"
    )
    if any(meta is None for meta in heads.values()):
        split_dir = _prepare_month(month, work_dir)
        output = f"s3://{BUCKET}/{ingest2bronze.BRONZE_PREFIX}"
        assert {path.name for path in split_dir.glob("*.jsonl")} == bronze.keys(), (
            f"{month}: file split lệch danh sách đầu vào Bronze"
        )
        if any(meta is not None for meta in heads.values()):
            manifest = ingest2bronze._load_remote_manifest(output)
            for name, meta in heads.items():
                if meta is None:
                    continue
                entry = manifest.get(name, {})
                local = split_dir / name
                assert (entry.get("dest") == f"s3://{BUCKET}/{bronze[name]}"
                        and entry.get("bytes") == meta["ContentLength"] == local.stat().st_size
                        and entry.get("sha256") == ingest2bronze._sha256_of_file(local)), (
                    f"{month}: không xác nhận được {name} có sẵn trên Bronze; dừng để giữ object cũ"
                )
        result = ingest2bronze.main(local_dir=split_dir, output_dir=output)
        assert not result["n_failed"] and not result["unparsed"], f"{month}: Bronze chưa hoàn tất"
    for key in bronze.values():
        meta = _head(key)
        assert meta and meta["ContentLength"] > 100, f"Bronze thiếu dữ liệu: {key}"

    for kind in missing:
        builders[kind].main(months=[month])
        meta = _head(silver[kind])
        assert meta and meta["ContentLength"] > 0, f"Silver chưa ghi {silver[kind]}"
    print(f"{month}: đủ 3 file Silver")


def assert_silver_window(months):
    from scripts.gold.common import silver_keys

    for kind in ("posts", "comments", "bot_users"):
        actual = tuple(Path(key).name for key in silver_keys(kind))
        expected = tuple(f"{kind}_{month}.parquet" for month in months)
        assert actual == expected, f"Silver {kind} lệch cửa sổ {months[0]} đến {months[-1]}: {actual}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--end-month", default="2026-08")
    parser.add_argument("--month", help="Chỉ backfill một tháng trong cửa sổ để kiểm tra")
    parser.add_argument("--work-dir", required=True, type=Path, help="Thư mục riêng để tải và lọc dump")
    args = parser.parse_args()
    months = year_months(args.end_month)
    if args.month and args.month not in months:
        parser.error(f"--month phải nằm trong {months[0]} đến {months[-1]}")
    for month in (args.month,) if args.month else months:
        _backfill_month(month, args.work_dir.resolve())
    if args.month is None:
        assert_silver_window(months)
        print(f"Silver đủ {len(months)} tháng, từ {months[0]} đến {months[-1]}")


if __name__ == "__main__":
    main()
