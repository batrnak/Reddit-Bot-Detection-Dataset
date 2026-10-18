# terraform/gold_schema.tf
# Gold layer: schema tối thiểu để gán nhãn (6 bảng).
#
# Quy ước kiểu (khớp pandas/pyarrow):
#   - số nguyên -> bigint (pandas ghi int64; khai "int" sẽ lỗi HIVE_BAD_DATA trên Athena)
#   - timestamp -> phải ghi parquet ở độ phân giải micro giây (coerce_timestamps="us")
#   - cột bắt đầu bằng lf_ chỉ dành cho LF, KHÔNG đưa vào model

resource "aws_glue_catalog_database" "gold" {
  name        = "gold"
  description = "Gold layer - features, graph, labels"
}

locals {
  gold_base = "s3://${aws_s3_bucket.datalake.id}/gold"

  gold_tables = {
    users = {
      description = "Tap user chung, node_idx co dinh, split co dinh"
      columns = [
        { name = "user_id",  type = "string"  },
        { name = "node_idx", type = "bigint"  },
        { name = "n_events", type = "bigint"  },
        { name = "eligible", type = "boolean" },
        { name = "split",    type = "string"  }, # train | val | test, chia theo hash user_id
      ]
    }

    user_features = {
      description = "Feature cho model (khong co prefix lf_) va feature chi cho LF (prefix lf_)"
      columns = [
        { name = "user_id",           type = "string"    },
        { name = "feature_cutoff_ts", type = "timestamp" },
        { name = "has_enough_data",   type = "boolean"   },
        # --- feature cho model ---
        { name = "log_events",   type = "double" },
        { name = "n_subs",       type = "bigint" },
        { name = "hour_entropy", type = "double" },
        { name = "iat_median",   type = "double" },
        { name = "iat_cv",       type = "double" },
        { name = "burstiness",   type = "double" },
        { name = "len_median",   type = "double" },
        { name = "dup_ratio",    type = "double" },
        { name = "link_ratio",   type = "double" },
        # --- chi cho LF, KHONG vao model ---
        { name = "lf_name_autogen", type = "boolean" },
        { name = "lf_dup_count",    type = "bigint"  },
        { name = "lf_burst_count",  type = "bigint"  },
        { name = "lf_coord_score",  type = "double"  },
      ]
    }

    graph_edges = {
      description = "Canh do thi. edge_role = propagation chi cho coord_*"
      columns = [
        { name = "src_idx",    type = "bigint"    },
        { name = "dst_idx",    type = "bigint"    },
        { name = "edge_type",  type = "string"    },
        { name = "edge_role",  type = "string"    },
        { name = "weight_idf", type = "double"    },
        { name = "first_ts",   type = "timestamp" }, # su kien dau tien tao ra canh
        { name = "last_ts",    type = "timestamp" },
      ]
    }   

    lf_votes = {
      description = "Phieu cua tung labeling function, dang long"
      columns = [
        { name = "user_id", type = "string" },
        { name = "lf_name", type = "string" },
        { name = "vote",    type = "bigint" }, # -1 abstain | 0 nguoi | 1 bot
      ]
    }

    weak_labels = {
      description = "Nhan yeu sau label model. p_bot dung de xep hang, khong phai xac suat"
      columns = [
        { name = "user_id",      type = "string" },
        { name = "p_bot",        type = "double" },
        { name = "label",        type = "bigint" }, # 0 nguoi | 1 bot | -1 khong chac
        { name = "n_lfs_voted",  type = "bigint" },
        { name = "label_source", type = "string" }, # vd: label_model_v1
      ]
    }

    eval_users = {
      description = "Danh sach loai tru khoi LF va huan luyen"
      columns = [
        { name = "user_id",   type = "string" },
        { name = "eval_role", type = "string" }, # botbouncer_B | botsim_heldout | injected
        { name = "eval_label", type = "bigint" },
      ]
    }
  }
}

resource "aws_glue_catalog_table" "gold" {
  for_each = local.gold_tables

  name          = each.key
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"
  description   = each.value.description

  parameters = {
    "classification"      = "parquet"
    "parquet.compression" = "SNAPPY"
    "EXTERNAL"            = "TRUE"
  }

  storage_descriptor {
    location      = "${local.gold_base}/${each.key}/"
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
      for_each = each.value.columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

output "glue_database_gold" {
  value = aws_glue_catalog_database.gold.name
}

output "glue_tables_gold" {
  value = keys(aws_glue_catalog_table.gold)
}