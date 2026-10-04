output "s3_bucket_name" {
  description = "Name of S3 Data Lake Bucket"
  value       = aws_s3_bucket.datalake.bucket
}

output "s3_bucket_arn" {
  description = "ARN of S3 Data Lake Bucket"
  value       = aws_s3_bucket.datalake.arn
}
