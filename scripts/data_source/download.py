# scripts/ingest/crawl_flags.py
"""
Tải + filter Arctic Shift dumps theo tháng, mặc định 2025-09 đến 2026-08.

Fix history (đã áp dụng):
  #1  .strict marker cho posts → không skip nhầm file cũ do rg tạo
  #2  Xoá .zst corrupt → xoá luôn .aria2 control (tránh resume sai)
  #3  download_specific: aria2 kiểm tra từng piece khi tải tiếp
  #4  filter_comments_python bắt UnicodeDecodeError (không chết giữa file)
  #5  filter_posts_strict: rg pre-filter + json.loads check top-level
  #6  download_specific: chỉ tải file cần (RS hoặc RC)
  #7  verify_zst trả 'ok'/'corrupt'/'unknown' — không xoá khi timeout
  #8  delete_raw_for: xoá theo từng kind, không đụng kind khác
  #9  finally: kill subprocess nếu exception → không treo
"""

import argparse
import ast
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

MONTHS = (
    "2025-09", "2025-10", "2025-11", "2025-12",
    "2026-01", "2026-02", "2026-03", "2026-04",
    "2026-05", "2026-06", "2026-07", "2026-08",
)


def _torrent_hashes():
    note = Path(__file__).with_name("monthnote")
    for statement in ast.parse(note.read_text(encoding="utf-8-sig")).body:
        if isinstance(statement, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "TORRENT_INFOHASH"
            for target in statement.targets
        ):
            return ast.literal_eval(statement.value)
    raise RuntimeError(f"Thiếu TORRENT_INFOHASH trong {note}")


TORRENT_INFOHASH = _torrent_hashes()
ACADEMIC_TORRENT_URL = "https://academictorrents.com/download/{infohash}.torrent"

VERIFY_TIMEOUT     = 3600
CHUNK_SIZE         = 32 * 1024 * 1024
MIN_POSTS_MATCH    = 50_000
MIN_COMMENTS_MATCH = 500_000
BT_STALL_TIMEOUT   = 300
BT_MAX_ATTEMPTS    = 3
BT_RETRYABLE_CODES = {2, 3, 4, 5, 6, 7, 19, 22, 29}

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


def download_specific(torrent_path: Path, month: str, want: set[str], *, remove_unselected=False):
    """
    Chỉ tải file cần. want: subset của {'RS', 'RC'}.
    Giữ raw và control file để aria2 kiểm tra và tiếp tục khi retry.
    """
    if not want:
        return
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    kinds = sorted(want)
    indices = []
    for kind in kinds:
        name = f"{kind}_{month}.zst"
        index = _find_index(torrent_path, name)
        if index is None:
            raise RuntimeError(f"Không tìm thấy {name} trong {torrent_path.name}")
        indices.append(str(index))

    kinds_str = "+".join(kinds)
    cmd = [
        "aria2c",
        "--seed-time=0", "--continue=true",
        "--allow-overwrite=true", "--auto-file-renaming=false",
        "--max-connection-per-server=4", "--split=4",
        "--bt-max-peers=200", "--bt-request-peer-speed-limit=5M",
        "--enable-dht=true", "--enable-peer-exchange=true",
        "--file-allocation=none", "--summary-interval=30",
        "--check-integrity=true", "--auto-save-interval=10",
        f"--bt-stop-timeout={BT_STALL_TIMEOUT}",
        f"--bt-remove-unselected-file={'true' if remove_unselected else 'false'}",
        f"--select-file={','.join(indices)}",
        "-d", str(DATA_DIR),
        str(torrent_path),
    ]
    for attempt in range(1, BT_MAX_ATTEMPTS + 1):
        print(f"  → aria2c tải {kinds_str} {month} (lần {attempt}/{BT_MAX_ATTEMPTS})", flush=True)
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as error:
            if error.returncode not in BT_RETRYABLE_CODES or attempt == BT_MAX_ATTEMPTS:
                raise RuntimeError(
                    f"Tải {kinds_str} {month} dừng (aria2c exit {error.returncode}). "
                    f"Giữ raw và control file trong {DATA_DIR}."
                ) from error
            print(f"  ⚠️ aria2c exit {error.returncode}. Thử tải tiếp sau 15 giây.", flush=True)
            time.sleep(15)
        else:
            return


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
def main(months=MONTHS):
    assert months, "Không có tháng để tải"
    assert all(re.fullmatch(r"[0-9a-fA-F]{40}", TORRENT_INFOHASH.get(month, "")) for month in months), (
        "Có tháng thiếu torrent info-hash hợp lệ trong monthnote"
    )
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    FILTERED_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Work dir     : {WORK_DIR}")
    print(f"Filtered dir : {FILTERED_DIR}")
    print(f"Months       : {', '.join(months)}")

    kinds = (
        ("RS", "posts", filter_posts_strict, MIN_POSTS_MATCH),
        ("RC", "comments", filter_comments_python, MIN_COMMENTS_MATCH),
    )
    for index, month in enumerate(months, start=1):
        print(f"\n{'=' * 60}\n  {month} ({index}/{len(months)})\n{'=' * 60}")
        for code, kind, filter_dump, minimum in kinds:
            output = out_path(month, kind)
            marker = strict_marker(month)
            if (code != "RS" or marker.is_file()) and is_output_valid(output, minimum):
                print(f"  → ✅ {output.name} đã hợp lệ. Skip.")
                continue

            if code == "RS":
                assert get_ripgrep_path(), "Thiếu ripgrep"
                marker.unlink(missing_ok=True)
            torrent = download_torrent(month)
            assert torrent is not None, f"Thiếu torrent {month}"
            download_specific(torrent, month, {code})
            raw = zst_path(month, code)
            assert raw.is_file(), f"Thiếu raw dump {raw}"

            status = verify_zst(raw)
            if status == "corrupt":
                delete_corrupt_zst(raw)
                raise RuntimeError(f"Dump hỏng: {raw.name}. Chạy lại để tải lại.")
            if status == "unknown":
                print(f"  → Vẫn thử filter {raw.name}.")

            kept = filter_dump(raw, output, f"{kind.upper()}-{month}", minimum)
            assert kept is not None and kept >= minimum, f"Filter {raw.name} chưa hoàn tất"
            if code == "RS":
                marker.touch()
            delete_zst_safe(month, code)

    # Tổng kết
    print(f"\n{'=' * 60}\n✅ HOÀN TẤT {len(months)} tháng\n{'=' * 60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--start-month", choices=MONTHS, default=MONTHS[0])
    parser.add_argument("--end-month", choices=MONTHS, default=MONTHS[-1])
    args = parser.parse_args()
    if args.start_month > args.end_month:
        parser.error("--start-month phải đứng trước hoặc bằng --end-month")
    main(tuple(month for month in MONTHS if args.start_month <= month <= args.end_month))
