resource "aws_s3_bucket" "raw" {
  bucket = "${local.project}-raw-data"

  tags = {
    Name        = "${local.project}-raw-data"
    Environment = local.environment
    Purpose     = "Landing zone for incoming raw data"
  }
}


resource "aws_s3_bucket_public_access_block" "raw" {
  bucket = aws_s3_bucket.raw.id
  block_public_acls=true
  block_public_policy=true
  ignore_public_acls=true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = var.enable_s3_versioning ? "Enabled" : "Suspended"
  }
}

resource "aws_s3_bucket" "processed" {
  bucket = "${local.project}-processed-data"

  tags={
    Name="${local.project}-processed-data"
    Environment = local.environment
    Purpose = "Stores cleaned and transformed data"
  }
}

resource "aws_s3_bucket_public_access_block" "name" {
  bucket = aws_s3_bucket.processed.id
  block_public_acls       = true
  block_public_policy = true
  ignore_public_acls = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "processed" {
  bucket = aws_s3_bucket.processed.id
  versioning_configuration {
    status = var.enable_s3_versioning ? "Enabled" : "Suspended"
  }
}


output "raw_bucket_name" {
  description = "Name of the raw data S3 bucket"
  value = aws_s3_bucket.raw.bucket
}

output "processed_bucket_name" {
  description = "Name of the processed data S3 bucket"
  value       = aws_s3_bucket.processed.bucket
}


