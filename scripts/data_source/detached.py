# scripts/ingest/refilter_posts.py
"""
Filter lại RS_*.zst — chỉ chạy khi posts hiện tại bị nhiễm sub lạ.
Bug đã fix so với bản trước:
  - Bỏ --max-columns 0
  - Verify "unknown" không abort
  - Xoá .aria2 unconditional khi re-download
  - Check rg returncode trước zstd (SIGPIPE-safe)
  - Dry-run test trước khi chạy full
"""

import argparse
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

import requests

# ================== CẤU HÌNH ==================
WORK_DIR     = Path("C:/arctic")
TORRENT_DIR  = WORK_DIR / "torrents"
DATA_DIR     = WORK_DIR / "data"
FILTERED_DIR = Path("C:/arctic_filtered")

TARGET_SUBS = {
    "worldnews", "politics", "news", "geopolitics",
    "ukraine", "UkrainianConflict", "IsraelPalestine",
    "InternationalNews", "GlobalTalk", "UpliftingNews",
    "europe", "northkorea", "LessCredibleDefence",
    "Israel", "Palestine",
    "BotBouncer", "TheseFuckingAccounts", "RedditBotHunters",
}
TARGET_LOWER = {s.lower() for s in TARGET_SUBS}

MONTHS = ["2026-06", "2026-07", "2026-08"]
TORRENT_INFOHASH = {
    "2026-06": "3bac8bd352bbb74bbb23df4273cf3da5d66ee5a5",
    "2026-07": "e04a4fda12826ab1d181eef6512b36aca63c70ff",
    "2026-08": "f4848163a5fb650e4b15eee0202858447bca120a",
}
ACADEMIC_TORRENT_URL = "https://academictorrents.com/download/{infohash}.torrent"

CHUNK_SIZE     = 32 * 1024 * 1024
VERIFY_TIMEOUT = 3600

CORRUPTION_MARKERS = (
    "corruption", "unsupported format", "unknown frame",
    "truncated", "data corruption",
)


# ================== HELPER ==================
def zst_path(m):    return DATA_DIR / "reddit" / "submissions" / f"RS_{m}.zst"
def out_path(m):    return FILTERED_DIR / f"posts_{m}.jsonl"
def done_marker(m): return FILTERED_DIR / f"posts_{m}.jsonl.done"
def rg_bak(m):      return FILTERED_DIR / f"posts_{m}.jsonl.rg_bak"


def has_any_aria2() -> bool:
    """Torrent nhiều file dùng CHUNG 1 control (reddit.aria2)."""
    return DATA_DIR.exists() and any(DATA_DIR.rglob("*.aria2"))


def any_zst_present() -> bool:
    return DATA_DIR.exists() and any(DATA_DIR.rglob("*.zst"))


def delete_all_aria2():
    """Xoá hết .aria2 — gọi khi CHẮC CHẮN re-download từ đầu."""
    if not DATA_DIR.exists():
        return
    for ctrl in DATA_DIR.rglob("*.aria2"):
        ctrl.unlink(missing_ok=True)
        print(f"  🧹 Xoá {ctrl.name}")


def find_rg() -> str:
    rg = shutil.which("rg")
    if rg:
        return rg
    for c in [
        Path.home() / "scoop" / "shims" / "rg.exe",
        Path.home() / "scoop" / "apps" / "ripgrep" / "current" / "rg.exe",
        Path("C:/Program Files/ripgrep/rg.exe"),
    ]:
        if c.exists():
            return str(c)
    raise RuntimeError("Không tìm thấy rg. Cài: scoop install ripgrep")


# ================== VERIFY ==================
def verify_zst(zst: Path) -> str:
    """Trả về: 'ok' | 'corrupt' | 'unknown'."""
    print(f"  → Verify {zst.name}...")
    t0 = time.time()
    try:
        r = subprocess.run(
            ["zstd", "-t", "--long=31", str(zst)],
            capture_output=True, text=True, timeout=VERIFY_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        print(f"  ⏱️  Timeout {VERIFY_TIMEOUT}s — KHÔNG xoá file")
        return "unknown"
    except FileNotFoundError:
        print(f"  ❌ Không có zstd trong PATH")
        sys.exit(1)

    if r.returncode == 0:
        print(f"  ✅ OK ({time.time()-t0:.0f}s)")
        return "ok"

    err = ((r.stderr or "") + (r.stdout or "")).lower()
    for marker in CORRUPTION_MARKERS:
        if marker in err:
            print(f"  ❌ CORRUPT ({marker})")
            return "corrupt"

    print(f"  ⚠️  Verify fail không rõ: {(r.stderr or '')[:200]}")
    return "unknown"


# ================== TẢI ==================
def download_torrent(month: str) -> Path:
    TORRENT_DIR.mkdir(parents=True, exist_ok=True)
    dest = TORRENT_DIR / f"{month}.torrent"
    if dest.exists():
        return dest
    url = ACADEMIC_TORRENT_URL.format(infohash=TORRENT_INFOHASH[month])
    print(f"  → Tải torrent: {url}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    dest.write_bytes(r.content)
    return dest


def find_rs_index(torrent_path: Path, month: str) -> int | None:
    r = subprocess.run(
        ["aria2c", "--show-files", str(torrent_path)],
        capture_output=True, text=True, check=True,
    )
    for line in r.stdout.splitlines():
        if f"RS_{month}.zst" in line:
            parts = line.split("|")
            if parts:
                try:
                    return int(parts[0].strip())
                except ValueError:
                    pass
    return None


def download_rs(torrent_path: Path, month: str) -> Path | None:
    """Coị 'xong' = file tồn tại + không có .aria2 nào."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    zst = zst_path(month)

    if zst.exists() and not has_any_aria2():
        size_gb = zst.stat().st_size / (1024 ** 3)
        print(f"  → RS_{month}.zst có sẵn ({size_gb:.1f} GB), không .aria2")
        return zst

    rs_idx = find_rs_index(torrent_path, month)
    if rs_idx is None:
        print(f"  ❌ Không có RS_{month}.zst trong torrent")
        return None

    print(f"  → aria2c tải index={rs_idx}")
    cmd = [
        "aria2c",
        "--seed-time=0", "--continue=true",
        "--allow-overwrite=true", "--auto-file-renaming=false",
        "--max-connection-per-server=16", "--split=16",
        "--bt-max-peers=200", "--bt-request-peer-speed-limit=5M",
        "--enable-dht=true", "--enable-peer-exchange=true",
        "--file-allocation=none", "--summary-interval=30",
        "--check-integrity=true",
        f"--select-file={rs_idx}",
        "-d", str(DATA_DIR),
        str(torrent_path),
    ]
    subprocess.run(cmd, check=True)
    return zst_path(month)


# ================== FILTER ==================
def filter_posts(zst_file, out_file, label, min_kept):
    rg = find_rg()
    tmp = out_file.with_suffix(out_file.suffix + ".tmp")
    tmp.unlink(missing_ok=True)

    print(f"  → Filter {label}: {zst_file.name}")

    pat = r'"subreddit"\s*:\s*"(' + "|".join(TARGET_SUBS) + r')"'
    cmd_rg = [rg, "--no-filename", "--no-line-number", "-i", "-e", pat]

    p_zstd = subprocess.Popen(
        ["zstd", "-dc", "--long=31", str(zst_file)],
        stdout=subprocess.PIPE, bufsize=CHUNK_SIZE,
    )
    p_rg = subprocess.Popen(
        cmd_rg, stdin=p_zstd.stdout,
        stdout=subprocess.PIPE, bufsize=CHUNK_SIZE,
    )
    p_zstd.stdout.close()

    scanned = kept = dropped = parse_err = 0
    t0 = time.time()
    ok = False

    try:
        with open(tmp, "wb") as fout:
            for raw_line in p_rg.stdout:
                scanned += 1
                if scanned % 100_000 == 0:
                    elapsed = time.time() - t0
                    print(f"      [{label}] rg={scanned:,}, kept={kept:,} ({elapsed:.0f}s)")

                try:
                    obj = json.loads(raw_line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    parse_err += 1
                    continue
                if not isinstance(obj, dict):
                    continue

                sub = obj.get("subreddit")
                if isinstance(sub, str) and sub.lower() in TARGET_LOWER:
                    fout.write(raw_line)
                    if not raw_line.endswith(b"\n"):
                        fout.write(b"\n")
                    kept += 1
                else:
                    dropped += 1
        ok = True                       # FIX #1: đánh dấu thành công
    finally:
        if not ok:                      # FIX #1: kill nếu có exception
            print(f"    ⚠️  Exception giữa chừng — kill subprocess.")
            p_rg.kill()
            p_zstd.kill()
        p_rg.wait()
        p_zstd.wait()

    # FIX #2: check vô điều kiện
    if p_rg.returncode not in (0, 1, -9):   # -9 = SIGKILL do mình kill
        print(f"    ❌ rg exit {p_rg.returncode} — GIỮ {tmp.name}")
        return None
    if p_zstd.returncode not in (0, -9):
        print(f"    ❌ zstd exit {p_zstd.returncode} — GIỮ {tmp.name}")
        return None
    if not ok:
        print(f"    ❌ Exception khi ghi — GIỮ {tmp.name}")
        return None

    print(f"    📊 rg-passed={scanned:,}, kept={kept:,}, dropped={dropped:,}, err={parse_err}")

    if kept < min_kept:
        print(f"    ⚠️  kept {kept:,} < {min_kept:,}. GIỮ {tmp.name}, không ghi đè.")
        return None

    if out_file.exists() and not rg_bak_from(out_file).exists():
        out_file.rename(rg_bak_from(out_file))

    tmp.replace(out_file)
    print(f"    ✅ [{label}] ghi {out_file.name}")
    return kept

def rg_bak_from(out_file: Path) -> Path:
    return out_file.with_suffix(out_file.suffix + ".rg_bak")


# ================== MAIN ==================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-kept", type=int, default=5000)
    ap.add_argument("--months", nargs="+", default=MONTHS)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    FILTERED_DIR.mkdir(parents=True, exist_ok=True)
    WORK_DIR.mkdir(parents=True, exist_ok=True)

    for month in args.months:
        print(f"\n{'=' * 60}\n  {month}\n{'=' * 60}")
        try:
            if done_marker(month).exists() and not args.force:
                print(f"  → Đã có .done. Skip.")
                continue

            out = out_path(month)

            # Tải torrent metadata
            torrent = download_torrent(month)

            # Tải RS
            zst = download_rs(torrent, month)
            if zst is None or not zst.exists():
                print(f"  ❌ Không có RS_{month}.zst")
                continue

            # Verify
            v = verify_zst(zst)

            if v == "corrupt":
                print(f"  → Xoá {zst.name} + tất cả .aria2, re-download từ đầu.")
                zst.unlink(missing_ok=True)
                delete_all_aria2()           # unconditional, tránh lỗi resume
                zst = download_rs(torrent, month)
                if zst is None or not zst.exists():
                    print(f"  ❌ Re-download fail. Skip {month}.")
                    continue
                v2 = verify_zst(zst)
                if v2 == "corrupt":
                    print(f"  ❌ Vẫn corrupt. Skip {month}.")
                    continue
                # ok hoặc unknown → thử filter

            elif v == "unknown":
                print(f"  → Verify không rõ. Thử filter.")

            # Filter
            n = filter_posts(zst, out, f"POSTS-{month}", args.min_kept)
            if n is None:
                print(f"  ⚠️  Filter fail — GIỮ .zst để debug.")
                continue

            # Đánh dấu hoàn tất TRƯỚC khi xoá .zst
            done_marker(month).touch()
            size_gb = zst.stat().st_size / (1024 ** 3)
            zst.unlink()
            print(f"  🗑️  Xoá {zst.name} ({size_gb:.1f} GB)")
            if not any_zst_present():
                delete_all_aria2()

        except KeyboardInterrupt:
            print(f"\n⚠️  Ctrl+C — thoát an toàn.")
            raise
        except Exception as e:
            print(f"  ❌ Lỗi {month}: {type(e).__name__}: {e}")
            continue

    print(f"\n{'=' * 60}\n✅ HOÀN TẤT\n{'=' * 60}")
    for f in sorted(FILTERED_DIR.glob("posts_2026-*.jsonl")):
        if f.suffix == ".jsonl":
            print(f"  {f.name}  {f.stat().st_size/1024/1024:.2f} MB")


if __name__ == "__main__":
    main()