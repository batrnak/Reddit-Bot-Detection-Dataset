# Reddit Bot Detection

## Dataset có nhãn theo nguồn trong 12 tháng

Cửa sổ mặc định là **09/2025–08/2026**. Backfill dùng `scripts/data_source/download.py` với đúng 15 subreddit hiện tại và r/BotBouncer để lấy flags. Backfill bỏ qua tháng đã đủ ba file Silver. Chạy một tiến trình backfill mỗi lần, từ repo root, với AWS credentials và các công cụ `aria2c`, `zstd`, `rg` đã cài. Chừa chỗ cho cả hai file trong torrent của tháng đang tải, các file JSONL tích lũy và file tạm. Trên Windows, aria2 có thể tạo thêm file phụ cho phần chung của hai dump và dọn file đó sau khi tải xong.

Backfill dùng lại file đã lọc hợp lệ trong `C:\arctic_filtered` nếu có.

**1. Backfill Bronze và Silver.** Có thể chạy thử riêng 05/2026 bằng cách thêm `--month 2026-05`. Chạy lệnh đầy đủ sau đó để kiểm tra đủ 12 tháng. Script xử lý từng tháng, tải RS rồi RC và dọn từng raw sau khi filter thành công. File đã lọc và đã split được giữ ở work dir để chạy lại.

```powershell
$env:PYTHONIOENCODING = "utf-8"
python -m scripts.data_source.backfill_year --end-month 2026-08 --work-dir C:\arctic_year_test
```

**2. Rebuild bảng Gold duy nhất cần cho luồng này.** Chạy sau khi backfill báo đủ 12 tháng. Bảng giữ `node_idx` và split theo identity.

```powershell
python -m scripts.gold.build_users
```

**3. Lấy nhãn theo nguồn và xuất hai file.** Script kiểm tra lại 12 tháng Silver, đối chiếu event counts với `gold.users` và dò author flair qua Arctic Shift API. Nó dùng `r/science`, `r/AskDocs`, `r/Selfie` và 15 subreddit đã kiểm tra trong `exports/expand_verified_users_2026.py`. Cache từng nguồn nằm trong thư mục evidence để tiếp tục nếu API gặp lỗi. Dùng `--destination` khác khi cần một version mới.

```powershell
python -m scripts.gold.export_verified_year --end-month 2026-08
```

Khi chạy thành công, `exports/dataset_verified_2025-09_2026-08_v1/` chứa đúng `Users.csv` và `user_post_comment.json`. Thư mục bên cạnh có hậu tố `_evidence` giữ bằng chứng flair và `report.json` với counts theo nguồn, lớp và split. Mỗi account trong hai file có một nhãn. Trong JSON, `comment_1` trả lời bài đăng (`t3_`) và `comment_2` trả lời bình luận (`t1_`, có thể sâu hơn cấp 2, `level=null`).

- **`0 = Bot`** — BotBouncer half A, có `banned`, không có `organic/service` và có ít nhất 4 events trước `flagged_at`. Half B chỉ được đếm cho tập proxy evaluation.
- **`1 = User`** — Account có verified flair được ghi nhận trong cửa sổ và có ít nhất 4 events trong 15 subreddit. Loại account có trong `bot_users`, kể cả `pending`, và loại các identity có bằng chứng xung đột.
- Giữ tất cả account đủ điều kiện. Team dùng `scripts.gold.common.stable_bucket(user_id, SALT_SPLIT, 100)` để giữ split cũ (`<70` train, `70–84` val, `85–99` test). Cân bằng lớp nếu cần ở bước train. BotSim chỉ cung cấp danh sách identity heldout cần loại, nên vẫn cần bản `data/raw/botsim24/Users.csv` đã verify nếu bản S3 thiếu hai bot.

Luồng này chỉ build `gold.users` rồi export nhãn theo nguồn. Verified flair cho biết chủ account đã qua quy trình xác minh của subreddit. BotBouncer phản ánh các account nguồn đó phát hiện. Quy mô từng lớp trong 12 tháng cần đo sau khi chạy. Gate 12 tháng kiểm tra các file Parquet thực tế trên Silver. Manifest Silver hiện tại ghi các tháng của lần build gần nhất.

## Kết quả thí nghiệm trước đây

Các kết quả labeler, weak labels và benchmark trước đây được ghi trong `AGENTS.md`. Mã nguồn thí nghiệm đã rút khỏi luồng chạy được sao lưu local trong `exports/legacy_sources/`.

Dataset đã xuất, cache gán nhãn, kết quả thử nghiệm và báo cáo nằm trong `exports/` theo quy tắc `.gitignore`. Hai module kiểm tra verified flair ở ngay cấp đầu của `exports/` là mã nguồn của luồng hiện hành.
