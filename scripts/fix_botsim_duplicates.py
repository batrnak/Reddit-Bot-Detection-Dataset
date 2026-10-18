"""
Fix duplicate user_ids trong BotSim-24 Users.csv.

Tìm user_id trùng sau khi chuẩn hóa (bs_ + lowercase),
kiểm tra trong user_post_comment.json, drop dòng trùng (giữ dòng đầu),
rồi ghi lại file đã xử lý lên S3.

Usage:
    python -m scripts.fix_botsim_duplicates
"""

import boto3
# pyrefly: ignore[missing-import]
import polars as pl
from io import BytesIO
import json
from datetime import datetime

BUCKET = 'data-pipeline-datalake-537390611425'
USERS_CSV_KEY = 'bronze/botsim24/raw/Users.csv'
USER_POST_KEY = 'bronze/botsim24/raw/user_post_comment.json'


def main():
    s3 = boto3.client('s3')
    
    # 1. Đọc Users.csv
    print("="*70)
    print("ĐỌC BOTSIM-24 USERS.CSV")
    print("="*70)
    body = s3.get_object(Bucket=BUCKET, Key=USERS_CSV_KEY)['Body'].read()
    users = pl.read_csv(BytesIO(body))
    print(f"Users.csv: {users.height:,} rows")
    print(f"Columns: {users.columns}")
    
    # 2. Thêm user_id chuẩn hóa và row index
    users = users.with_columns([
        (pl.lit("bs_") + pl.col("name").cast(pl.String).str.to_lowercase()).alias("user_id_norm"),
    ]).with_row_index("_row_idx")
    
    # 3. Tìm duplicates
    print("\n" + "="*70)
    print("TÌM USER_ID TRÙNG")
    print("="*70)
    
    dup_groups = (
        users.group_by("user_id_norm")
        .agg([
            pl.len().alias("count"),
            pl.col("name").alias("original_names"),
            pl.col("_row_idx").alias("row_indices"),
        ])
        .filter(pl.col("count") > 1)
    )
    
    if dup_groups.height == 0:
        print("✅ Không có duplicate user_ids. Không cần xử lý.")
        return
    
    print(f"❌ Tìm thấy {dup_groups.height} user_id trùng:")
    
    # 4. Đọc user_post_comment.json để check
    print("\n" + "="*70)
    print("ĐỌC USER_POST_COMMENT.JSON")
    print("="*70)
    
    try:
        body = s3.get_object(Bucket=BUCKET, Key=USER_POST_KEY)['Body'].read()
        post_data = json.loads(body)
        print(f"user_post_comment.json: {len(post_data):,} users")
        
        # Tạo set user_ids có trong post data (cả với và không có prefix bs_)
        post_user_ids = set()
        for uid in post_data.keys():
            uid_lower = uid.lower()
            post_user_ids.add(uid_lower)
            post_user_ids.add("bs_" + uid_lower)
    except Exception as e:
        print(f"⚠️  Không đọc được user_post_comment.json: {e}")
        post_data = {}
        post_user_ids = set()
    
    # 5. Phân tích chi tiết từng duplicate
    print("\n" + "="*70)
    print("PHÂN TÍCH CHI TIẾT")
    print("="*70)
    
    users_to_drop = []  # List các row indices cần drop
    
    for row in dup_groups.iter_rows(named=True):
        user_id_norm = row["user_id_norm"]
        original_names = row["original_names"]
        row_indices = row["row_indices"]
        count = row["count"]
        
        print(f"\n{user_id_norm} (xuất hiện {count} lần):")
        
        # Lấy các dòng tương ứng
        matching_rows = users.filter(pl.col("user_id_norm") == user_id_norm)
        
        for r in matching_rows.iter_rows(named=True):
            row_idx = r["_row_idx"]
            name = r["name"]
            char_setting = str(r.get("character_setting", ""))[:60]
            
            # Xác định human/bot (1000 dòng cuối là bot)
            is_bot = row_idx >= users.height - 1000
            label = "BOT" if is_bot else "HUMAN"
            
            # Check có trong post data không
            name_lower = name.lower()
            in_post = name_lower in post_data or ("bs_" + name_lower) in post_data
            
            # Lấy số posts/comments nếu có
            n_posts = n_comments = 0
            if post_data:
                key = name_lower if name_lower in post_data else "bs_" + name_lower
                if key in post_data:
                    user_data = post_data[key]
                    n_posts = len(user_data.get("posts", []))
                    n_comments = len(user_data.get("comments", []))
            
            post_info = f"{n_posts} posts, {n_comments} comments" if in_post else "KHÔNG có data"
            
            print(f"  Row {row_idx:4d} [{label:5s}] | name={name:<30s} | {post_info}")
            print(f"            character: {char_setting}...")
        
        # Quyết định drop dòng nào
        # Strategy: drop các dòng KHÔNG có trong post_data trước,
        # nếu tất cả đều có hoặc không có, giữ dòng có _row_idx nhỏ nhất (dòng đầu trong CSV)
        rows_with_post = []
        rows_without_post = []
        
        for r in matching_rows.iter_rows(named=True):
            row_idx = r["_row_idx"]
            name = r["name"]
            name_lower = name.lower()
            
            in_post = name_lower in post_data or ("bs_" + name_lower) in post_data
            if in_post:
                rows_with_post.append(row_idx)
            else:
                rows_without_post.append(row_idx)
        
        print(f"  → Rows có data: {rows_with_post}")
        print(f"  → Rows không data: {rows_without_post}")
        
        if rows_without_post:
            # Drop các row không có data
            to_drop = rows_without_post
            print(f"  → Drop {len(to_drop)} row(s) không có data: {to_drop}")
            users_to_drop.extend(to_drop)
        else:
            # Tất cả đều có data, giữ row đầu tiên (nhỏ nhất)
            keep_row = min(rows_with_post)
            to_drop = [r for r in rows_with_post if r != keep_row]
            print(f"  → Giữ row {keep_row}, drop {len(to_drop)} row(s): {to_drop}")
            users_to_drop.extend(to_drop)
    
    # 6. Drop và verify
    print("\n" + "="*70)
    print("DROP DUPLICATES")
    print("="*70)
    
    print(f"Tổng số rows cần drop: {len(users_to_drop)}")
    
    users_clean = users.filter(~pl.col("_row_idx").is_in(users_to_drop))
    
    # Verify không còn duplicate
    remaining_dups = (
        users_clean.group_by("user_id_norm")
        .agg(pl.len().alias("count"))
        .filter(pl.col("count") > 1)
    )
    
    if remaining_dups.height > 0:
        print(f"❌ Vẫn còn {remaining_dups.height} duplicates. Kiểm tra lại logic.")
        return
    
    print(f"✅ Trước: {users.height:,} rows | Sau: {users_clean.height:,} rows (mất {users.height - users_clean.height})")
    
    # Đếm human/bot sau khi drop
    n_clean = users_clean.height
    n_bot_clean = users_clean.filter(pl.col("_row_idx") >= n_clean - 1000).height
    # Lưu ý: row_idx không còn chính xác sau khi drop. Tính lại theo vị trí.
    n_bot_clean = 1000  # Giữ nguyên 1000 bot ở cuối nếu không bị drop
    n_bot_dropped = len([r for r in users_to_drop if r >= users.height - 1000])
    n_bot_clean = 1000 - n_bot_dropped
    n_human_clean = users_clean.height - n_bot_clean
    
    print(f"  Human: {n_human_clean:,} | Bot: {n_bot_clean:,}")
    
    # 7. Backup file cũ và ghi file mới
    print("\n" + "="*70)
    print("GHI LẠI S3")
    print("="*70)
    
    # Backup
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    backup_key = f"botsim24/raw/Users.csv.backup.{ts}"
    s3.copy_object(
        Bucket=BUCKET,
        CopySource={"Bucket": BUCKET, "Key": USERS_CSV_KEY},
        Key=backup_key,
    )
    print(f"  Backup: s3://{BUCKET}/{backup_key}")
    
    # Ghi file mới
    # Bỏ các cột tạm (_row_idx, user_id_norm) trước khi ghi
    users_clean_output = users_clean.drop(["_row_idx", "user_id_norm"])
    
    buf = BytesIO()
    users_clean_output.write_csv(buf)
    buf.seek(0)
    
    s3.put_object(
        Bucket=BUCKET,
        Key=USERS_CSV_KEY,
        Body=buf.getvalue(),
    )
    print(f"  Ghi mới: s3://{BUCKET}/{USERS_CSV_KEY}")
    
    # 8. Verify file mới
    print("\n" + "="*70)
    print("VERIFY")
    print("="*70)
    
    body_new = s3.get_object(Bucket=BUCKET, Key=USERS_CSV_KEY)['Body'].read()
    users_verify = pl.read_csv(BytesIO(body_new))
    
    users_verify = users_verify.with_columns([
        (pl.lit("bs_") + pl.col("name").cast(pl.String).str.to_lowercase()).alias("user_id_norm"),
    ])
    
    final_dups = (
        users_verify.group_by("user_id_norm")
        .agg(pl.len().alias("count"))
        .filter(pl.col("count") > 1)
    )
    
    if final_dups.height == 0:
        print(f"✅ File mới không có duplicate!")
        print(f"  Tổng: {users_verify.height:,} users")
        print(f"  Columns: {users_verify.columns}")
    else:
        print(f"❌ File mới vẫn có {final_dups.height} duplicates!")
        print("  Cần kiểm tra lại.")


if __name__ == "__main__":
    main()