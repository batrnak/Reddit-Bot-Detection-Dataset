resource "aws_iam_user" "airflow_local" {
  name = "airflow-local-user"
}

resource "aws_iam_access_key" "airflow_local" {
  user = aws_iam_user.airflow_local.name
}

resource "aws_iam_user_policy" "airflow_local_s3" {
  name = "airflow-local-s3"
  user = aws_iam_user.airflow_local.name
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:*"]
      Resource = [
        aws_s3_bucket.datalake.arn,
        "${aws_s3_bucket.datalake.arn}/*"
      ]
    }]
  })
}