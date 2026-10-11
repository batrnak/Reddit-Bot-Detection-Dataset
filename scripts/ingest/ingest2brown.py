# scripts/ingest/load_community_flags.py

import json
import hashlib
import re
from pathlib import Path
from datetime import datetime, timezone

from scripts.utils.storage import put_file, put_bytes

# ============================================================
# >>> ĐIỀN ĐƯỜNG DẪN THƯ MỤC CHỨA FILE JSONL VÀO ĐÂY <<<
FLAGS_DIR = Path("C:/arctic_filtered/split")
# ============================================================

# Pattern: {kind}_{YYYY-MM}_{label}.jsonl
NAME_RE = re.compile(r"^(posts|comments)_(\d{4}-\d{2})_(botflag|normal)\.jsonl$")


def _sha256_of_file(path: Path, chunk=8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while b := f.read(chunk):
            h.update(b)
    return h.hexdigest()


def main(local_dir=None, output_dir="", **ctx):
    """
    Copy NGUYÊN BẢN các file JSONL (đã tách botflag/normal) lên bronze S3.
    Partition: raw/{kind}/year={Y}/month={M}/{label}.jsonl
    Chạy được với 1 hay nhiều tháng: chỉ upload những gì có trong thư mục.
    """
    if not output_dir.startswith("s3://"):
        raise RuntimeError(f"output_dir phải dạng s3://..., nhận được: {output_dir!r}")
    output_dir = output_dir.rstrip("/")

    base = Path(local_dir or FLAGS_DIR)
    if not base.exists():
        raise RuntimeError(f"Không tìm thấy thư mục: {base}")

    files = sorted(base.glob("*.jsonl"))
    if not files:
        raise RuntimeError(f"Không có file .jsonl nào trong {base}")

    # ---------- Kiểm tra TRƯỚC khi upload ----------
    found = {tuple(m.groups()) for f in files if (m := NAME_RE.match(f.name))}
    pairs = {(k, ym) for k, ym, _ in found}
    incomplete = sorted(
        (k, ym, lbl)
        for (k, ym) in pairs
        for lbl in ("botflag", "normal")
        if (k, ym, lbl) not in found
    )
    if incomplete:
        raise RuntimeError(f"Thiếu nửa cặp label: {incomplete}")

    for f in files:
        if f.name.endswith("_botflag.jsonl") and f.stat().st_size == 0:
            raise RuntimeError(f"{f.name} rỗng, kiểm tra lại split_botflag")

    print(f"Tìm thấy {len(files)} file trong {base}")

    # ---------- Upload ----------
    manifest = {}
    unparsed = []

    for src in files:
        m = NAME_RE.match(src.name)
        if not m:
            print(f"  ⚠️  Không nhận diện: {src.name} → bỏ qua")
            unparsed.append(src.name)
            continue

        kind, ym, label = m.groups()
        year, month = ym.split("-")
        dest = f"{output_dir}/raw/{kind}/year={year}/month={month}/{label}.jsonl"

        put_file(dest, src)  # streaming, không load vào RAM

        size = src.stat().st_size
        manifest[src.name] = {
            "bytes": size,
            "sha256": _sha256_of_file(src),
            "source_path": str(src),
            "dest": dest,
            "kind": kind,
            "ym": ym,
            "label": label,
            "loaded_at": datetime.now(timezone.utc).isoformat(),
        }
        print(f"  ✅ {src.name} → {dest}  ({size/1024/1024:.1f} MB)")

    if not manifest:
        raise RuntimeError(f"Không có file nào nhận diện được trong {base}")

    # Manifest nhỏ → put_bytes, không cần file tạm trong thư mục nguồn
    months = sorted({info["ym"] for info in manifest.values()})
    manifest_name = f"_manifest_{months[0]}.json" if len(months) == 1 else f"_manifest_{months[0]}_to_{months[-1]}.json"
    put_bytes(
        f"{output_dir}/raw/{manifest_name}",
        json.dumps(
            {"manifest": manifest, "unparsed": unparsed},
            indent=2,
            ensure_ascii=False,
        ).encode("utf-8"),
    )

    print(f"\n📊 {len(manifest)} file đã upload ({', '.join(months)})")
    if unparsed:
        print(f"   ⚠️  Unparsed: {unparsed}")

    return {"n_files": len(manifest), "months": months, "unparsed": unparsed}