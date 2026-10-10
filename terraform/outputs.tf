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