# terraform/gold_schema.tf
# Gold layer: schema de gan nhan + huan luyen (8 bang).
#
# Quy uoc kieu (khop polars/pyarrow):
#   - so nguyen -> bigint (khai "int" se loi HIVE_BAD_DATA tren Athena)
#   - timestamp -> ghi parquet o do phan giai micro giay
#   - hash noi dung (UInt64 cua polars) phai doi sang Int64 truoc khi ghi: .reinterpret(signed=True)
#   - cot bat dau bang lf_ chi danh cho LF, KHONG dua vao model

resource "aws_glue_catalog_database" "gold" {
  name        = "gold"
  description = "Gold layer - features, graph, labels"
}

locals {
  gold_base = "s3://${aws_s3_bucket.datalake.id}/gold"

  # Feature cho model: dung chung cho user_features (Reddit) va botsim_users (BotSim)
  # de hai ben luon cung ten cot, cung kieu.
  model_feature_cols = [
    { name = "has_enough_data", type = "boolean" },
    { name = "log_events",      type = "double"  },
    { name = "n_subs",          type = "bigint"  },
    { name = "hour_entropy",    type = "double"  },
    { name = "iat_median",      type = "double"  },
    { name = "iat_cv",          type = "double"  },
    { name = "burstiness",      type = "double"  },
    { name = "len_median",      type = "double"  },
    { name = "dup_ratio",       type = "double"  },
    { name = "link_ratio",      type = "double"  },
  ]

  gold_tables = {
    users = {
      description = "Tap user Reddit chung, node_idx co dinh, split co dinh"
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
      columns = concat(
        [
          { name = "user_id",           type = "string"    },
          { name = "feature_cutoff_ts", type = "timestamp" },
        ],
        local.model_feature_cols,
        [
          # --- chi cho LF, KHONG vao model ---
          { name = "lf_name_autogen", type = "boolean" },
          { name = "lf_dup_count",    type = "bigint"  },
          { name = "lf_burst_count",  type = "bigint"  },
          { name = "lf_coord_score",  type = "double"  },
        ]
      )
    }

    botsim_users = {
      description = "BotSim-24 voi nhan that. user_id co prefix bs_. Feature cung ten cot voi user_features"
      columns = concat(
        [
          { name = "user_id", type = "string" },
          { name = "is_bot",  type = "bigint" }, # 1 = bot (1000 dong cuoi cua Users.csv), 0 = nguoi
          { name = "split",   type = "string" }, # train | heldout (cung phep chia botsim_bucket voi eval_users)
        ],
        local.model_feature_cols
      )
    }

    user_events = {
      description = "Chuoi su kien cua user cho mo hinh chuoi. Voi user bi flag chi gom su kien truoc flagged_at"
      columns = [
        { name = "user_id",          type = "string"    }, # Reddit hoac bs_<ten> cho BotSim
        { name = "ts",               type = "timestamp" },
        { name = "event_type",       type = "string"    }, # post | comment
        { name = "item_id",          type = "string"    }, # submission_id / comment_id, de doi chieu voi silver
        { name = "subreddit",        type = "string"    },
        { name = "text_len",         type = "bigint"    },
        { name = "text_hash",        type = "bigint"    }, # Int64, null neu qua ngan hoac [removed]
        { name = "has_external_url", type = "boolean"   },
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
      description = "Phieu cua tung labeling function, dang long. Moi (user_id, lf_name) toi da 1 dong"
      columns = [
        { name = "user_id",  type = "string" },
        { name = "lf_name",  type = "string" },
        { name = "vote",     type = "bigint" }, # -1 abstain | 0 nguoi | 1 bot
        { name = "score",    type = "double" }, # diem lien tuc neu LF co, de trong neu la luat thuan
        { name = "evidence", type = "string" }, # ly do ngan, chi de debug
      ]
    }

    weak_labels = {
      description = "Bang chung positive va unknown; evidence_score chi dung de xep hang"
      columns = [
        { name = "user_id",      type = "string" },
        { name = "n_lfs_vote",    type = "bigint" },
        { name = "lf_mask",        type = "bigint" },
        { name = "evidence_score", type = "double" },
        { name = "label_status",   type = "string" },
        { name = "y_pu",           type = "bigint" }, # 1 hoac NULL (unlabeled)
      ]
    }

    eval_users = {
      description = "Tap danh gia, KHONG dung de huan luyen. eval_label: 1 bot, 0 nguoi"
      columns = [
        { name = "user_id",    type = "string" },
        { name = "eval_role",  type = "string" }, # botbouncer_B | botsim_heldout | injected
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

