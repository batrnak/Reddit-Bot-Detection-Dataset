# scripts/ingest/load_community_flags.py
"""
Upload NGUYÊN BẢN các file JSONL đã tách lên bronze S3 (idempotent).

Đặc điểm:
  - KHÔNG xoá file local.
  - Đọc manifest cũ trên S3 → so sha256 → chỉ upload file mới/đã đổi.
  - File stale (comments_*_botflag.jsonl) tự động skip, không raise.
  - Nếu upload fail → giữ file local, không đánh dấu vào manifest.
"""

import json
import hashlib
import re
import os
import sys
from pathlib import Path
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from scripts.utils.storage import put_file, put_bytes


FLAGS_DIR      = Path("C:/arctic_filtered/split")
DEFAULT_BUCKET = "data-pipeline-datalake-537390611425"
BRONZE_PREFIX  = "bronze/arctic_shift"
BUCKET = os.environ.get("DATALAKE_BUCKET", DEFAULT_BUCKET)

# Pattern: {kind}_{YYYY-MM}_{label}.jsonl
NAME_RE = re.compile(r"^(posts|comments)_(\d{4}-\d{2})_(botflag|normal)\.jsonl$")

# Label bắt buộc theo kind:
#   posts    -> botflag + normal
#   comments -> chỉ normal (botflag đã bỏ ở split_botflag)
REQUIRED_LABELS = {
    "posts":    ("botflag", "normal"),
    "comments": ("normal",),
}


# ================== HELPER ==================

def _sha256_of_file(path: Path, chunk=8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while b := f.read(chunk):
            h.update(b)
    return h.hexdigest()


def _parse_s3_uri(uri: str):
    """s3://bucket/prefix → (bucket, prefix)"""
    if not uri.startswith("s3://"):
        raise ValueError(f"Không phải S3 URI: {uri}")
    bucket, _, prefix = uri[5:].partition("/")
    return bucket, prefix.rstrip("/")


def _load_remote_manifest(output_dir: str) -> dict:
    """
    Đọc manifest mới nhất trên S3 (nếu có).
    Trả về {filename: info} hoặc {} nếu chưa có / lỗi.
    """
    try:
        bucket, prefix = _parse_s3_uri(output_dir)
        s3 = boto3.client("s3")

        resp = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=f"{prefix}/raw/_manifest_",
        )
        if "Contents" not in resp or not resp["Contents"]:
            print("  ℹ️  Chưa có manifest cũ trên S3.")
            return {}

        latest = sorted(resp["Contents"], key=lambda x: x["LastModified"])[-1]
        print(f"  📖 Đọc manifest cũ: {latest['Key']}")
        data = s3.get_object(Bucket=bucket, Key=latest["Key"])["Body"].read()
        old = json.loads(data).get("manifest", {})
        print(f"     → {len(old)} entry cũ.")
        return old
    except ClientError as e:
        print(f"  ⚠️  Không đọc được manifest (ClientError): {e}")
        return {}
    except Exception as e:
        print(f"  ⚠️  Lỗi đọc manifest: {type(e).__name__}: {e}")
        return {}


# ================== MAIN ==================

def main(local_dir=None, output_dir="", **ctx):
    """
    Upload file JSONL lên bronze S3, idempotent theo sha256.
    Partition: raw/{kind}/year={Y}/month={M}/{label}.jsonl
    """
    if not output_dir:
        output_dir = f"s3://{BUCKET}/{BRONZE_PREFIX}"

    if not output_dir.startswith("s3://"):
        raise RuntimeError(f"output_dir phải dạng s3://..., nhận được: {output_dir!r}")
    output_dir = output_dir.rstrip("/")

    base = Path(local_dir or FLAGS_DIR)
    if not base.exists():
        raise RuntimeError(f"Không tìm thấy thư mục: {base}")

    files_all = sorted(base.glob("*.jsonl"))
    if not files_all:
        raise RuntimeError(f"Không có file .jsonl nào trong {base}")

    # ---------- Lọc stale (comments_*_botflag.jsonl) ----------
    stale = [
        f for f in files_all
        if f.name.startswith("comments_") and f.name.endswith("_botflag.jsonl")
    ]
    if stale:
        print(f"  ℹ️  Bỏ qua {len(stale)} file stale: {[f.name for f in stale]}")
        files = [f for f in files_all if f not in stale]
    else:
        files = files_all

    # ---------- Kiểm tra completeness ----------
    found = {tuple(m.groups()) for f in files if (m := NAME_RE.match(f.name))}
    pairs = {(k, ym) for k, ym, _ in found}

    incomplete = sorted(
        (k, ym, lbl)
        for (k, ym) in pairs
        for lbl in REQUIRED_LABELS[k]
        if (k, ym, lbl) not in found
    )
    if incomplete:
        raise RuntimeError(f"Thiếu file bắt buộc: {incomplete}")

    # Không cho file rỗng
    for f in files:
        if f.stat().st_size == 0:
            raise RuntimeError(f"{f.name} rỗng, kiểm tra lại split_botflag")

    # ---------- Header ----------
    print(f"Bucket    : {BUCKET}")
    print(f"Prefix    : {BRONZE_PREFIX}")
    print(f"Local dir : {base}")
    print(f"Output    : {output_dir}")
    print(f"Local     : {len(files)} file (bỏ {len(stale)} stale)\n")

    # ---------- Đọc manifest cũ ----------
    old_manifest = _load_remote_manifest(output_dir)
    print()

    # ---------- Upload idempotent ----------
    manifest = dict(old_manifest)
    unparsed   = []
    n_uploaded = 0
    n_skipped  = 0
    n_failed   = 0

    for src in files:
        m = NAME_RE.match(src.name)
        if not m:
            print(f"  ⚠️  Không nhận diện: {src.name} → bỏ qua")
            unparsed.append(src.name)
            continue

        kind, ym, label = m.groups()
        year, month = ym.split("-")
        dest = f"{output_dir}/raw/{kind}/year={year}/month={month}/{label}.jsonl"

        size = src.stat().st_size
        local_sha = _sha256_of_file(src)

        # So sánh với manifest cũ
        old = old_manifest.get(src.name)
        if old and old.get("sha256") == local_sha and old.get("bytes") == size:
            print(f"  ⏭️  {src.name} không đổi → skip ({size/1024/1024:.1f} MB)")
            manifest[src.name] = old          # giữ nguyên entry cũ
            n_skipped += 1
            continue

        # Upload
        try:
            put_file(dest, src)
        except Exception as e:
            print(f"  ❌ {src.name} upload fail: {type(e).__name__}: {e}")
            n_failed += 1
            continue

        manifest[src.name] = {
            "bytes":       size,
            "sha256":      local_sha,
            "source_path": str(src),
            "dest":        dest,
            "kind":        kind,
            "ym":          ym,
            "label":       label,
            "loaded_at":   datetime.now(timezone.utc).isoformat(),
        }
        tag = "đổi" if old else "mới"
        print(f"  ✅ {src.name} → {dest}  ({size/1024/1024:.1f} MB, {tag})")
        n_uploaded += 1

    if not manifest:
        raise RuntimeError("Không có file nào upload được.")

    # ---------- Manifest ----------
    months = sorted({info["ym"] for info in manifest.values()})
    manifest_name = (
        f"_manifest_{months[0]}.json"
        if len(months) == 1
        else f"_manifest_{months[0]}_to_{months[-1]}.json"
    )
    put_bytes(
        f"{output_dir}/raw/{manifest_name}",
        json.dumps(
            {
                "manifest":       manifest,
                "unparsed":       unparsed,
                "stale_skipped":  [f.name for f in stale],
            },
            indent=2,
            ensure_ascii=False,
        ).encode("utf-8"),
    )

    print(f"\n📊 Upload: {n_uploaded} mới/đổi | {n_skipped} skip | {n_failed} fail")
    print(f"📄 Manifest: {output_dir}/raw/{manifest_name}")
    if unparsed:
        print(f"   ⚠️  Unparsed: {unparsed}")
    if stale:
        print(f"   ℹ️  Stale skipped: {[f.name for f in stale]}")
    if n_failed:
        raise RuntimeError(
            f"{n_failed} file upload fail. Manifest đã ghi, "
            f"chạy lại sẽ chỉ upload phần thiếu."
        )
    return {
        "n_files":       len(manifest),
        "n_uploaded":    n_uploaded,
        "n_skipped":     n_skipped,
        "n_failed":      n_failed,
        "months":        months,
        "unparsed":      unparsed,
        "stale_skipped": [f.name for f in stale],
    }


# ================== ENTRY POINT ==================

if __name__ == "__main__":
    # Override qua CLI (tùy chọn):
    #   python -m scripts.ingest.load_community_flags s3://other-bucket/path
    OUTPUT_DIR = sys.argv[1] if len(sys.argv) >= 2 else f"s3://{BUCKET}/{BRONZE_PREFIX}"

    print("=" * 60)
    print("Ingest Community Flags → Bronze S3 (idempotent)")
    print("=" * 60)
    print(f"Output: {OUTPUT_DIR}\n")

    result = main(output_dir=OUTPUT_DIR)

    print("\n" + "=" * 60)
    print("✅ HOÀN TẤT")
    print("=" * 60)
    print(result)