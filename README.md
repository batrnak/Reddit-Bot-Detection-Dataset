# Reddit Bot Detection Dataset

Dự án xây dựng dataset phân loại tài khoản Reddit thành **Bot** và **User**. Mỗi mẫu gồm một tài khoản, lịch sử bài đăng và bình luận, cùng nhãn dựa trên bằng chứng từ BotBouncer hoặc verified author flair. Dataset phục vụ huấn luyện và đánh giá mô hình ở cấp tài khoản.

Cửa sổ mặc định là **12 tháng, từ 09/2025 đến hết 08/2026**. Dữ liệu hoạt động đến từ 15 subreddit về tin tức và chính trị. Chọn tháng kết thúc bằng `--end-month`.

## Nguồn dữ liệu và quy tắc gán nhãn

- **Arctic Shift** cung cấp dump bài đăng và bình luận theo tháng. Danh sách 15 subreddit nằm trong [cấu hình lọc](scripts/data_source/download.py). [Danh sách dump Arctic Shift](https://github.com/ArthurHeitmann/arctic_shift/blob/master/download_links.md)
- **BotBouncer** cung cấp cờ ở cấp tài khoản. Nhãn **`0 = Bot`** lấy từ half A, với cờ `banned`, không có `organic` hoặc `service`, và ít nhất 4 sự kiện trước `flagged_at`. Half B được giữ riêng cho đánh giá. [r/BotBouncer](https://www.reddit.com/r/BotBouncer/)
- **Verified author flair** cung cấp tín hiệu cho nhãn **`1 = User`**. Flair phải được ghi nhận trong cửa sổ dữ liệu, tài khoản phải có ít nhất 4 sự kiện ở các subreddit mục tiêu và không có trong `bot_users`. Các nguồn gồm r/AskDocs, r/science và những cộng đồng được định nghĩa trong hai module kiểm tra. [Nguồn flair chính](exports/verified_user_probe_2026.py) · [Nguồn bổ sung](exports/expand_verified_users_2026.py)
- **BotSim-24** là bộ tham chiếu định dạng và nguồn xác định các identity held-out. Exporter loại các identity held-out, kể cả tên Reddit tương ứng của tài khoản human trong BotSim. [BotSim-24 Dataset](https://github.com/QQQQQQBY/BotSim/tree/main/BotSim-24-Dataset)

Các tài khoản có bằng chứng xung đột hoặc chưa đủ điều kiện gán nhãn nằm ngoài dataset bàn giao. Verified flair cung cấp bằng chứng về chủ tài khoản. Độ tin cậy của nhãn và khả năng phát hiện bot từ LLM generator mới cần được đánh giá riêng.

## Pipeline

![Sequence diagram từ backfill Bronze và Silver, qua gold.users, đến export Users.csv và user_post_comment.json với nhãn từ BotBouncer và verified flair](docs/pipeline.svg)

1. **Bronze.** Tải và lọc dump theo subreddit. Tách posts BotBouncer thành luồng cờ riêng, rồi upload JSONL lên S3 theo tháng.
2. **Silver.** Chuẩn hóa thành ba nhóm Parquet `posts`, `comments` và `bot_users`. Mỗi nhóm có một file cho mỗi tháng UTC.
3. **Gold.** Tính lại `gold.users` từ Silver, đếm hoạt động và giữ `node_idx` ổn định cho tài khoản cũ. `user_id` dùng tên Reddit viết thường. Split theo hash của `user_id` với tỷ lệ mục tiêu **70/15/15** cho train, validation và test. Tỷ lệ này áp dụng trước bước chọn nhãn.
4. **Export.** Đối chiếu số hoạt động với Gold, áp dụng quy tắc gán nhãn và cutoff, rồi kiểm tra ID, nhãn và số hoạt động khớp giữa hai file trước khi xuất.

## Định dạng dataset

- `Users.csv` có một dòng cho mỗi tài khoản, gồm `user_id`, số bài đăng, số bình luận, subreddit và cột `label`.
- `user_post_comment.json` dùng cùng `user_id` làm key. Mỗi tài khoản có `posts`, `comment_1` và `comment_2`. `comment_1` trả lời bài đăng (`t3_`). `comment_2` trả lời bình luận (`t1_`) và có thể sâu hơn cấp 2, nên `level` để `null`.
- `report.json` trong thư mục `_evidence` bên cạnh dataset ghi cửa sổ dữ liệu, số mẫu theo lớp và split, số BotBouncer B dùng được cho đánh giá, cùng thống kê nguồn flair. Thư mục này cũng chứa cache bằng chứng theo nguồn.

Ví dụ rút gọn cho một tài khoản có 1 bài đăng, 3 bình luận và nhãn User:

```csv
user_id,submission_num,comment_num,label
user_demo,1,3,1
```

```json
{
  "user_demo": {
    "posts": [{"submission_id": "p01", "posts": "Bài mẫu", "subreddit": "news"}],
    "comment_1": [
      {"comment_id": "c01", "parent_id": "t3_p01", "comment_body": "Bình luận mẫu"},
      {"comment_id": "c02", "parent_id": "t3_p01"}
    ],
    "comment_2": [{"comment_id": "c03", "parent_id": "t1_c01", "level": null}]
  }
}
```

Hai file chứa các tài khoản đủ điều kiện từ cả ba split. Khi dùng dataset, lấy split từ `gold.users` hoặc tính lại bằng `stable_bucket(user_id, SALT_SPLIT, 100)`. Bucket `0–69` là train, `70–84` là validation và `85–99` là test. [Cấu hình split](scripts/gold/common.py) · [Cách gán split](scripts/gold/build_users.py)

## Chuẩn bị và chạy

Chạy từ thư mục gốc của repo. Môi trường cần Python, quyền đọc và ghi trên bucket S3 đã cấu hình, cùng `aria2c`, `zstd` và `rg` cho bước tải và lọc. [Cấu hình S3](scripts/silver/common.py) · [Hạ tầng AWS](terraform/)

```powershell
python -m pip install -r requirements.txt
$env:PYTHONIOENCODING = "utf-8"
```

Chuẩn bị bản `Users.csv` chính thức của BotSim-24 tại `data/raw/botsim24/Users.csv` để exporter có bản dự phòng. Exporter kiểm tra checksum của bản S3 và bản local trước khi dùng. [BotSim-24 Dataset](https://github.com/QQQQQQBY/BotSim/tree/main/BotSim-24-Dataset)

**1. Backfill Bronze và Silver.**

```powershell
python -u -m scripts.data_source.backfill_year --end-month 2026-08 --work-dir C:\arctic_year_test
```

**2. Dựng `gold.users` sau khi backfill báo đủ 12 tháng.**

```powershell
python -m scripts.gold.build_users
```

**3. Chọn nhãn và xuất dataset.**

```powershell
python -m scripts.gold.export_verified_year --end-month 2026-08
```

- **Backfill từng tháng.** Giữ nguyên `--work-dir` khi chạy lại để tận dụng file đã tải hoặc lọc. Tháng đã đủ ba file Silver được bỏ qua. Dùng `--month YYYY-MM` để chạy thử một tháng. Các tháng cần tải phải có trong [danh sách torrent](scripts/data_source/monthnote).
- **Kiểm tra cửa sổ.** Backfill toàn kỳ và exporter yêu cầu mỗi nhóm Silver có đúng 12 file Parquet theo cửa sổ đã chọn.
- **Dung lượng local.** Chọn ổ có chỗ cho dump của tháng đang tải, file phụ của torrent và JSONL tích lũy. Thư mục tạm cũng cần chỗ để xử lý JSONL và Parquet tải từ S3.
- **Xuất theo version.** Lệnh trên tạo `exports/dataset_verified_2025-09_2026-08_v1/`. Dùng `--destination` với đường dẫn mới để xuất version tiếp theo. Dataset, cache bằng chứng và báo cáo là output local theo [quy tắc Git](.gitignore).

## Cấu trúc repo

- [`scripts/data_source/`](scripts/data_source/) tải dump, lọc subreddit và điều phối backfill.
- [`scripts/bronze/`](scripts/bronze/) upload JSONL lên S3.
- [`scripts/silver/`](scripts/silver/) chuẩn hóa dữ liệu theo tháng và ghi Parquet.
- [`scripts/gold/`](scripts/gold/) dựng bảng tài khoản, chọn dataset theo nguồn và ghi hai file bàn giao.
- [`exports/`](exports/) chứa hai module verified flair ở cấp đầu và các thư mục output local.
- [`terraform/`](terraform/) định nghĩa hạ tầng AWS và Glue Data Catalog cho các bảng truy vấn bằng Athena.

## Kết quả đã đối chiếu

Trong giai đoạn **06–08/2026**, Silver có **94.272 bài đăng** và **5.668.129 bình luận**. Rà soát theo nguồn xác định **142 tài khoản Bot** và **236 tài khoản User** đủ điều kiện trên cửa sổ này. Backfill tháng **09/2025** cũng đã được kiểm tra qua Bronze và Silver. Số lượng của cửa sổ 12 tháng được thống kê sau khi export trong `report.json`.

Các quy tắc về identity, cutoff, held-out và kết quả thí nghiệm trước đây được ghi trong [AGENTS.md](AGENTS.md). Mã nguồn dùng giấy phép [MIT](LICENSE).
