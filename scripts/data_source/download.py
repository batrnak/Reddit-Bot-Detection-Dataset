# scripts/ingest/crawl_flags.py
"""
Tải + filter Arctic Shift dumps cho 3 tháng.

Fix history (đã áp dụng):
  #1  .strict marker cho posts → không skip nhầm file cũ do rg tạo
  #2  Xoá .zst corrupt → xoá luôn .aria2 control (tránh resume sai)
  #3  download_specific: file ≥ 5GB vẫn tải nếu còn .aria2
  #4  filter_comments_python bắt UnicodeDecodeError (không chết giữa file)
  #5  filter_posts_strict: rg pre-filter + json.loads check top-level
  #6  download_specific: chỉ tải file cần (RS hoặc RC)
  #7  verify_zst trả 'ok'/'corrupt'/'unknown' — không xoá khi timeout
  #8  delete_raw_for: xoá theo từng kind, không đụng kind khác
  #9  finally: kill subprocess nếu exception → không treo
"""

import json
import re
import shutil
import subprocess
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
    "BotBouncer", 
}
TARGET_SUBS_LOWER = {s.lower() for s in TARGET_SUBS}

_SUB_BYTES_RE = re.compile(
    rb'"subreddit":\s*"(' + b"|".join(re.escape(s.encode("utf-8")) for s in TARGET_SUBS) + rb')"',
    re.IGNORECASE,
)

MONTHS = ["2026-0̀5"]

TORRENT_INFOHASH = {
    "2026-05": "55199eff9368cde1f5c1262dd7c1af09f7503ea5",
}
ACADEMIC_TORRENT_URL = "https://academictorrents.com/download/{infohash}.torrent"

MIN_FILE_SIZE      = 5 * 1024 ** 3
VERIFY_TIMEOUT     = 3600
CHUNK_SIZE         = 32 * 1024 * 1024
MIN_POSTS_MATCH    = 50_000
MIN_COMMENTS_MATCH = 500_000

CORRUPTION_MARKERS = (
    "corruption", "unsupported format", "unknown frame",
    "truncated", "data corruption",
)


# ================== HELPER ==================
def zst_path(month: str, kind: str) -> Path:
    """kind: 'RS' (submissions) | 'RC' (comments)."""
    sub = "submissions" if kind == "RS" else "comments"
    return DATA_DIR / "reddit" / sub / f"{kind}_{month}.zst"


def out_path(month: str, kind: str) -> Path:
    """kind: 'posts' | 'comments'."""
    return FILTERED_DIR / f"{kind}_{month}.jsonl"


def strict_marker(month: str) -> Path:
    """Marker file: posts_<month>.jsonl đã qua strict filter."""
    return FILTERED_DIR / f"posts_{month}.jsonl.strict"


def count_lines(path: Path) -> int:
    if not path.exists():
        return 0
    n = 0
    with open(path, "rb") as f:
        while chunk := f.read(4 * 1024 * 1024):
            n += chunk.count(b"\n")
    return n


def is_output_valid(path: Path, min_match: int) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return False
    return count_lines(path) >= min_match


def find_all_aria2() -> list[Path]:
    if not DATA_DIR.exists():
        return []
    return list(DATA_DIR.rglob("*.aria2"))


def get_ripgrep_path() -> str | None:
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
    return None


# ================== VERIFY ==================
def verify_zst(path: Path) -> str:
    """Trả về: 'ok' | 'corrupt' | 'unknown' (timeout hoặc lỗi không rõ)."""
    print(f"  → Verify {path.name}...")
    t0 = time.time()
    try:
        r = subprocess.run(
            ["zstd", "-t", "--long=31", str(path)],
            capture_output=True, text=True, timeout=VERIFY_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        print(f"  ⏱️  Timeout {VERIFY_TIMEOUT}s — KHÔNG xoá file")
        return "unknown"
    if r.returncode == 0:
        print(f"  ✅ OK ({time.time()-t0:.0f}s)")
        return "ok"

    err = ((r.stderr or "") + (r.stdout or "")).lower()
    for m in CORRUPTION_MARKERS:
        if m in err:
            print(f"  ❌ CORRUPT ({m})")
            return "corrupt"

    print(f"  ⚠️  Verify fail (không rõ nguyên nhân): {(r.stderr or '')[:200]}")
    return "unknown"


# ================== TẢI ==================
def download_torrent(month: str) -> Path | None:
    infohash = TORRENT_INFOHASH.get(month)
    if not infohash or len(infohash) != 40:
        print(f"  ❌ Info-hash {month} không hợp lệ.")
        return None

    TORRENT_DIR.mkdir(parents=True, exist_ok=True)
    dest = TORRENT_DIR / f"{month}.torrent"
    if dest.exists():
        print(f"  → Đã có {dest.name}")
        return dest

    url = ACADEMIC_TORRENT_URL.format(infohash=infohash)
    print(f"  → Tải torrent: {url}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    dest.write_bytes(r.content)
    return dest


def _find_index(torrent_path: Path, filename: str) -> int | None:
    """Tìm index của file trong torrent bằng aria2c --show-files."""
    r = subprocess.run(
        ["aria2c", "--show-files", str(torrent_path)],
        capture_output=True, text=True, check=True,
    )
    for line in r.stdout.splitlines():
        if filename in line:
            parts = line.split("|")
            if parts:
                try:
                    return int(parts[0].strip())
                except ValueError:
                    pass
    return None


def download_specific(torrent_path: Path, month: str, want: set[str]):
    """
    Chỉ tải file cần. want: subset của {'RS', 'RC'}.
    Fix #3: file ≥ 5GB vẫn tải nếu còn .aria2 (tải dở).
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    aria2_present = bool(find_all_aria2())
    to_download = []

    for kind in want:
        zst = zst_path(month, kind)
        already_full = (
            zst.exists()
            and zst.stat().st_size >= MIN_FILE_SIZE
            and not aria2_present          # FIX #3
        )
        if already_full:
            size_gb = zst.stat().st_size / (1024 ** 3)
            print(f"  → {zst.name} đã có ({size_gb:.1f} GB), không còn .aria2 → skip.")
            continue

        if zst.exists() and aria2_present:
            size_gb = zst.stat().st_size / (1024 ** 3)
            print(f"  → {zst.name} ({size_gb:.1f} GB) nhưng còn .aria2 → tải tiếp.")

        fname = f"{kind}_{month}.zst"
        idx = _find_index(torrent_path, fname)
        if idx is None:
            print(f"  ❌ Không tìm thấy {fname} trong torrent")
            continue
        to_download.append((kind, idx))

    if not to_download:
        print(f"  → Không cần tải gì.")
        return

    indices = ",".join(str(i) for _, i in to_download)
    kinds_str = "+".join(k for k, _ in to_download)
    print(f"  → aria2c tải {kinds_str} (index {indices})")

    cmd = [
        "aria2c",
        "--seed-time=0", "--continue=true",
        "--allow-overwrite=true", "--auto-file-renaming=false",
        "--max-connection-per-server=16", "--split=16",
        "--bt-max-peers=200", "--bt-request-peer-speed-limit=5M",
        "--enable-dht=true", "--enable-peer-exchange=true",
        "--file-allocation=none", "--summary-interval=30",
        "--check-integrity=true",
        f"--select-file={indices}",
        "-d", str(DATA_DIR),
        str(torrent_path),
    ]
    subprocess.run(cmd, check=True)


# ================== FILTER: POSTS (strict) ==================
def filter_posts_strict(zst_file: Path, out_file: Path, label: str, min_match: int) -> int | None:
    """
    rg pre-filter + json.loads check TOP-LEVEL subreddit.
    Fix bug nhiễm crosspost.
    """
    rg = get_ripgrep_path()
    if not rg:
        print(f"  ❌ Không có rg — không thể strict filter.")
        return None

    tmp = out_file.with_suffix(out_file.suffix + ".tmp")
    tmp.unlink(missing_ok=True)

    print(f"  → Filter {label}: {zst_file.name}  [rg + json check]")

    pattern = r'"subreddit"\s*:\s*"(' + "|".join(TARGET_SUBS) + r')"'
    cmd_rg = [rg, "--no-filename", "--no-line-number", "-i", "-e", pattern]

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
                if scanned % 500_000 == 0:
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
                if isinstance(sub, str) and sub.lower() in TARGET_SUBS_LOWER:
                    fout.write(raw_line)
                    if not raw_line.endswith(b"\n"):
                        fout.write(b"\n")
                    kept += 1
                else:
                    dropped += 1
        ok = True
    finally:
        if not ok:
            print(f"    ⚠️  Exception — kill subprocess.")
            p_rg.kill()
            p_zstd.kill()
        p_rg.wait()
        p_zstd.wait()

    # Check returncode vô điều kiện
    if p_rg.returncode not in (0, 1, -9):
        print(f"    ❌ rg exit {p_rg.returncode} — GIỮ {tmp.name}")
        return None
    if p_zstd.returncode not in (0, -9):
        print(f"    ❌ zstd exit {p_zstd.returncode} — GIỮ {tmp.name}")
        return None
    if not ok:
        print(f"    ❌ Exception khi ghi — GIỮ {tmp.name}")
        return None

    print(f"    📊 rg={scanned:,}, kept={kept:,}, dropped={dropped:,}, err={parse_err}")

    if kept < min_match:
        print(f"    ⚠️  kept {kept:,} < {min_match:,}. GIỮ {tmp.name}, không ghi đè.")
        return None

    tmp.replace(out_file)
    print(f"    ✅ [{label}] ghi {out_file.name}")
    return kept


# ================== FILTER: COMMENTS ==================
def filter_comments_python(zst_file: Path, out_file: Path, label: str, min_match: int) -> int | None:
    """Python chunk-based, fix #4: bắt UnicodeDecodeError."""
    tmp = out_file.with_suffix(out_file.suffix + ".tmp")
    tmp.unlink(missing_ok=True)

    print(f"  → Filter {label}: {zst_file.name}  [python-chunk]")

    p = subprocess.Popen(
        ["zstd", "-dc", "--long=31", str(zst_file)],
        stdout=subprocess.PIPE, bufsize=CHUNK_SIZE,
    )

    scanned = kept = parse_err = 0
    leftover = b""
    t0 = time.time()
    ok = False

    try:
        with open(tmp, "wb") as fout:
            while True:
                chunk = p.stdout.read(CHUNK_SIZE)
                if not chunk:
                    break
                chunk = leftover + chunk
                lines = chunk.split(b"\n")
                leftover = lines.pop()

                for raw_line in lines:
                    scanned += 1
                    if scanned % 5_000_000 == 0:
                        elapsed = time.time() - t0
                        print(f"      [{label}] quét {scanned:,}, kept={kept:,} ({elapsed:.0f}s)")

                    if b'"subreddit"' not in raw_line:
                        continue
                    if not _SUB_BYTES_RE.search(raw_line):
                        continue

                    try:
                        obj = json.loads(raw_line)
                    except (json.JSONDecodeError, UnicodeDecodeError):    # FIX #4
                        parse_err += 1
                        continue
                    if not isinstance(obj, dict):
                        continue

                    sub = obj.get("subreddit")
                    if isinstance(sub, str) and sub.lower() in TARGET_SUBS_LOWER:
                        fout.write(raw_line)
                        if not raw_line.endswith(b"\n"):
                            fout.write(b"\n")
                        kept += 1

            # Dòng cuối cùng
            if leftover and b'"subreddit"' in leftover and _SUB_BYTES_RE.search(leftover):
                try:
                    obj = json.loads(leftover)
                    if isinstance(obj, dict):
                        sub = obj.get("subreddit")
                        if isinstance(sub, str) and sub.lower() in TARGET_SUBS_LOWER:
                            fout.write(leftover)
                            if not leftover.endswith(b"\n"):
                                fout.write(b"\n")
                            kept += 1
                except (json.JSONDecodeError, UnicodeDecodeError):    # FIX #4
                    pass
        ok = True
    finally:
        if not ok:
            print(f"    ⚠️  Exception — kill zstd.")
            p.kill()
        p.wait()

    if p.returncode not in (0, -9):
        print(f"    ❌ zstd exit {p.returncode} — GIỮ {tmp.name}")
        return None
    if not ok:
        return None

    print(f"    📊 quét {scanned:,}, kept={kept:,}, err={parse_err}")

    if kept < min_match:
        print(f"    ⚠️  kept {kept:,} < {min_match:,}. GIỮ {tmp.name}.")
        return None

    tmp.replace(out_file)
    print(f"    ✅ [{label}] ghi {out_file.name}")
    return kept


# ================== XOÁ RAW ==================
def delete_zst_safe(month: str, kind: str):
    """Xoá 1 file .zst + xoá .aria2 nếu không còn .zst nào khác."""
    f = zst_path(month, kind)
    if not f.exists():
        return
    try:
        size_gb = f.stat().st_size / (1024 ** 3)
        f.unlink()
        print(f"  🗑️  Xoá {f.name} ({size_gb:.1f} GB)")
    except Exception as e:
        print(f"  ⚠️  Không xoá được {f.name}: {e}")
        return

    # Chỉ xoá .aria2 nếu không còn .zst nào cần control
    if not any(DATA_DIR.rglob("*.zst")):
        for ctrl in find_all_aria2():
            ctrl.unlink(missing_ok=True)
            print(f"  🧹 Xoá {ctrl.name}")


def delete_corrupt_zst(zst: Path):
    """Fix #2: xoá .zst hỏng + TẤT CẢ .aria2 (tránh resume sai)."""
    try:
        zst.unlink(missing_ok=True)
        print(f"  🗑️  Xoá {zst.name}")
    except Exception as e:
        print(f"  ⚠️  Không xoá được {zst.name}: {e}")

    for ctrl in find_all_aria2():
        ctrl.unlink(missing_ok=True)
        print(f"  🧹 Xoá control {ctrl.name}")


# ================== MAIN ==================
def main():
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    FILTERED_DIR.mkdir(parents=True, exist_ok=True)

    rg = get_ripgrep_path()
    if not rg:
        print("❌ Cần ripgrep. Cài: scoop install ripgrep")
        return

    print(f"Work dir     : {WORK_DIR}")
    print(f"Filtered dir : {FILTERED_DIR}")
    print(f"Months       : {', '.join(MONTHS)}")
    print(f"ripgrep      : {rg}\n")

    for month in MONTHS:
        print(f"\n{'=' * 60}\n  {month}\n{'=' * 60}")

        try:
            posts_out = out_path(month, "posts")
            comments_out = out_path(month, "comments")

            # FIX #1: posts_done chỉ khi có .strict marker
            posts_done = (
                strict_marker(month).exists()
                and is_output_valid(posts_out, MIN_POSTS_MATCH)
            )
            comments_done = is_output_valid(comments_out, MIN_COMMENTS_MATCH)

            if posts_done and comments_done:
                print(f"  → ✅ Cả 2 output đã hợp lệ. Skip.")
                if zst_path(month, "RS").exists() or zst_path(month, "RC").exists():
                    if zst_path(month, "RS").exists():
                        delete_zst_safe(month, "RS")
                    if zst_path(month, "RC").exists():
                        delete_zst_safe(month, "RC")
                continue

            # Xác định cần tải gì
            want = set()
            if not posts_done:
                want.add("RS")
            if not comments_done:
                want.add("RC")

            print(f"  → Cần tải: posts={'RS' if not posts_done else 'skip'}, "
                  f"comments={'RC' if not comments_done else 'skip'}")

            # Tải torrent metadata
            torrent = download_torrent(month)
            if torrent is None:
                continue

            # Tải chỉ file cần
            try:
                download_specific(torrent, month, want)
            except subprocess.CalledProcessError as e:
                print(f"  ⚠️  aria2c lỗi: {e}")
                continue

            # Verify từng file
            to_filter = {}
            for kind in want:
                zst = zst_path(month, kind)
                if not zst.exists():
                    print(f"  ❌ Thiếu {zst.name}")
                    continue

                v = verify_zst(zst)
                if v == "corrupt":
                    delete_corrupt_zst(zst)          # FIX #2
                    continue
                if v == "unknown":
                    print(f"  → Vẫn thử filter {zst.name}.")
                to_filter[kind] = zst

            # Filter
            posts_ok = posts_done
            comments_ok = comments_done

            if "RS" in to_filter and not posts_done:
                n = filter_posts_strict(
                    to_filter["RS"], posts_out,
                    f"POSTS-{month}", MIN_POSTS_MATCH,
                )
                if n is not None and n >= MIN_POSTS_MATCH:
                    posts_ok = True
                    strict_marker(month).touch()      # FIX #1
                    print(f"  ✅ Đánh dấu {strict_marker(month).name}")

            if "RC" in to_filter and not comments_done:
                n = filter_comments_python(
                    to_filter["RC"], comments_out,
                    f"COMMENTS-{month}", MIN_COMMENTS_MATCH,
                )
                if n is not None and n >= MIN_COMMENTS_MATCH:
                    comments_ok = True

            # Xoá raw chỉ cho kind đã thành công
            if posts_ok and not posts_done:
                delete_zst_safe(month, "RS")
            if comments_ok and not comments_done:
                delete_zst_safe(month, "RC")

            if not (posts_ok and comments_ok):
                print(f"\n  🛑 Chưa hoàn tất — giữ raw cho lần sau.")
                if not posts_ok:
                    print(f"     - posts chưa xong")
                if not comments_ok:
                    print(f"     - comments chưa xong")

        except KeyboardInterrupt:
            print(f"\n⚠️  Ctrl+C — thoát an toàn.")
            return
        except Exception as e:
            print(f"  ❌ Lỗi {month}: {type(e).__name__}: {e}")
            continue

    # Tổng kết
    print(f"\n{'=' * 60}\n✅ HOÀN TẤT\n{'=' * 60}")
    for f in sorted(FILTERED_DIR.glob("*.jsonl")):
        n = count_lines(f)
        print(f"  {f.name}  {f.stat().st_size/1024/1024:.2f} MB ({n:,} dòng)")


if __name__ == "__main__":
    main()