terraform {
  required_providers {
    aws      = { source = "hashicorp/aws", version = "~> 5.0" }
    external = { source = "hashicorp/external", version = "~> 2.3" }
    archive  = { source = "hashicorp/archive", version = "~> 2.4" }
  }
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  endpoints {
    iam    = "http://localhost:4566"
    lambda = "http://localhost:4566"
    sqs    = "http://localhost:4566" # Enabled SQS Endpoint hook inside LocalStack
  }
}

variable "lambda_environment" {
  type      = map(string)
  sensitive = true
  default   = {}
}

# 1. Create a native dead-letter SQS queue inside your emulated local cloud stack
resource "aws_sqs_queue" "lambda_dlq" {
  name = "demo-processing-dlq"
}

# 2. Compile your code and bundle dependencies safely into a single index.mjs file
data "external" "esbuild_trigger" {
  program = ["sh", "-c", "cd lambda && mkdir -p dist && npx esbuild index.js --bundle --platform=node --target=node18 --format=esm --external:pg-native --banner:js=\"import { createRequire } from 'module'; const require = createRequire(import.meta.url);\" --outfile=dist/index.mjs > /dev/null && echo '{\"status\":\"done\"}'"]
}

data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/lambda/dist"
  output_path = "${path.module}/lambda.zip"
  depends_on  = [data.external.esbuild_trigger]
}

# 3. Create the basic execution role for the Lambda function
resource "aws_iam_role" "lambda_role" {
  name               = "demo-lambda-role"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{ Action = "sts:AssumeRole", Effect = "Allow", Principal = { Service = "://amazonaws.com" } }] })
}

# 4. Attach an inline policy allowing the Lambda function to ship records to your DLQ SQS Queue
resource "aws_iam_role_policy" "lambda_sqs_policy" {
  name = "demo-lambda-sqs-policy"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = aws_sqs_queue.lambda_dlq.arn
      }
    ]
  })
}

# 5. Deploy the self-contained package artifact into LocalStack
resource "aws_lambda_function" "kafka_processor" {
  function_name    = "kafka-processor"
  role             = aws_iam_role.lambda_role.arn
  handler          = "index.handler"
  runtime          = "nodejs18.x"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  # NATIVE CLOUD PLATFORM ROUTING CONFIGURATION
  dead_letter_config {
    target_arn = aws_sqs_queue.lambda_dlq.arn
  }

  environment {
    variables = var.lambda_environment
  }
}

