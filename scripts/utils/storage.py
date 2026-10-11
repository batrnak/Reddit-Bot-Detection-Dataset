import boto3
import shutil 
from pathlib import Path

def put_bytes(dest: str, data: bytes):
    if dest.startswith("s3://"):
        bucket, key = dest[5:].split("/", 1)
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=data)
    else:
        p = Path(dest)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

def get_bytes(src: str) -> bytes:
    if src.startswith("s3://"):
        bucket, key = src[5:].split("/", 1)
        return boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    return Path(src).read_bytes()

def put_file(dest: str, src: Path):
    if dest.startswith("s3://"):
        bucket, key = dest[5:].split("/", 1)
        boto3.client("s3").upload_file(str(src), bucket, key)
    else:
        p = Path(dest)
        p.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, p)  