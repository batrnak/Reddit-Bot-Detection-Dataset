output "s3_bucket_name" {
  description = "Name of S3 Data Lake Bucket"
  value       = aws_s3_bucket.datalake.bucket
}

output "s3_bucket_arn" {
  description = "ARN of S3 Data Lake Bucket"
  value       = aws_s3_bucket.datalake.arn
}
output "iam_access_key_id" {
  value     = aws_iam_access_key.airflow_local.id
  sensitive = true
}

output "iam_secret_access_key" {
  value     = aws_iam_access_key.airflow_local.secret
  sensitive = true
}

output "glue_database_silver" {
  value = aws_glue_catalog_database.silver_arctic_shift.name
}

output "glue_tables_silver" {
  value = [
    aws_glue_catalog_table.silver_posts.name,
    aws_glue_catalog_table.silver_comments.name,
    aws_glue_catalog_table.silver_bot_users.name,
  ]
}

output "glue_database_gold" {
  value = aws_glue_catalog_database.gold.name
}

output "glue_tables_gold" {
  value = keys(aws_glue_catalog_table.gold)
}