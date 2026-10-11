# terraform/glue.tf
# Glue Data Catalog cho SILVER layer (Arctic Shift)
#
# NOTE về kiểu dữ liệu:
#   - Pandas/pyarrow mặc định ghi int64 → Athena cần "bigint" (không phải "int")
#   - Python script dùng datetime với tz=UTC → Parquet lưu timestamp[ns, tz=UTC]
#     → khai "timestamp" (Athena v3 hiển thị theo UTC). Nếu lỗi thử "timestamptz".
#   - ingest_month là cột THƯỜNG (ghi trong file), không phải partition.
#     → giữ trong columns, KHÔNG dùng partition_keys.

# ================== DATABASE ==================
resource "aws_glue_catalog_database" "silver_arctic_shift" {
  name        = "silver_arctic_shift"
  description = "Silver layer - Arctic Shift cleaned data"
}

# ================== LOCALS ==================
locals {
  silver_base = "s3://${aws_s3_bucket.datalake.id}/silver/arctic_shift"

  # Schema cho từng bảng — dùng cho dynamic "columns"
  posts_columns = [
    { name = "submission_id", type = "string"    },
    { name = "author_name",   type = "string"    },
    { name = "title",         type = "string"    },
    { name = "selftext",      type = "string"    },
    { name = "score",         type = "bigint"    },
    { name = "num_comments",  type = "bigint"    },
    { name = "upvote_ratio",  type = "double"    },
    { name = "created_utc",   type = "timestamp" },
    { name = "subreddit",     type = "string"    },
    { name = "url",           type = "string"    },
    { name = "permalink",     type = "string"    },
    { name = "ingest_month",  type = "string"    },
  ]

  comments_columns = [
    { name = "comment_id",   type = "string"    },
    { name = "author_name",  type = "string"    },
    { name = "body",         type = "string"    },
    { name = "score",        type = "bigint"    },
    { name = "link_id",      type = "string"    },
    { name = "parent_id",    type = "string"    },
    { name = "created_utc",  type = "timestamp" },
    { name = "subreddit",    type = "string"    },
    { name = "permalink",    type = "string"    },
    { name = "ingest_month", type = "string"    },
  ]

  bot_users_columns = [
    { name = "username",         type = "string"    },
    { name = "source_subreddit", type = "string"    },
    { name = "source_post_id",   type = "string"    },
    { name = "flair",            type = "string"    },
    { name = "flagged_at",       type = "timestamp" },
    { name = "ingest_month",     type = "string"    },
  ]
}

# ================== TABLE: posts ==================
# Output của scripts/silver/build_posts.py
resource "aws_glue_catalog_table" "silver_posts" {
  name          = "posts"
  database_name = aws_glue_catalog_database.silver_arctic_shift.name
  table_type    = "EXTERNAL_TABLE"
  description   = "Reddit submissions (posts) cleaned"

  parameters = {
    "classification"      = "parquet"
    "parquet.compression" = "SNAPPY"
    "EXTERNAL"            = "TRUE"
  }

  storage_descriptor {
    location      = "${local.silver_base}/posts/data/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    dynamic "columns" {
      for_each = local.posts_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

# ================== TABLE: comments ==================
# Output của scripts/silver/build_comments.py
resource "aws_glue_catalog_table" "silver_comments" {
  name          = "comments"
  database_name = aws_glue_catalog_database.silver_arctic_shift.name
  table_type    = "EXTERNAL_TABLE"
  description   = "Reddit comments cleaned"

  parameters = {
    "classification"      = "parquet"
    "parquet.compression" = "SNAPPY"
    "EXTERNAL"            = "TRUE"
  }

  storage_descriptor {
    location      = "${local.silver_base}/comments/data/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    dynamic "columns" {
      for_each = local.comments_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

# ================== TABLE: bot_users ==================
# Output của scripts/silver/build_bot_users.py
resource "aws_glue_catalog_table" "silver_bot_users" {
  name          = "bot_users"
  database_name = aws_glue_catalog_database.silver_arctic_shift.name
  table_type    = "EXTERNAL_TABLE"
  description   = "Reddit usernames flagged as bots"

  parameters = {
    "classification"      = "parquet"
    "parquet.compression" = "SNAPPY"
    "EXTERNAL"            = "TRUE"
  }

  storage_descriptor {
    location      = "${local.silver_base}/bot_users/data/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    dynamic "columns" {
      for_each = local.bot_users_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}
