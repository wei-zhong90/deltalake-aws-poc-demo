resource "aws_glue_job" "phase_1" {
  name        = "raw_process_python"
  description = "The initial ETL job that will create delta table from the raw data"
  role_arn    = module.glue_role.arn

  worker_type       = "G.1X"
  number_of_workers = 5
  glue_version      = "3.0"

  max_retries = 0

  connections = [aws_glue_connection.kafka.name]

  execution_property {
    max_concurrent_runs = 20
  }

  command {
    name            = "gluestreaming"
    script_location = "s3://${aws_s3_bucket.jar_bucket.bucket}/scripts/raw_phase_1.py"
    python_version  = 3
  }

  default_arguments = {
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--bootstrap_servers"                = module.kafka.bootstrap_brokers_iam
    "--bucket_name"                      = aws_s3_bucket.data_bucket.bucket
    "--topic"                            = var.kafka_test_topic
    "--extra-jars"                       = "s3://${aws_s3_bucket.jar_bucket.bucket}/delta-core_2.12-1.0.0.jar,s3://${aws_s3_bucket.jar_bucket.bucket}/aws-msk-iam-auth-1.1.0-all.jar"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.jar_bucket.bucket}/delta-core_2.12-1.0.0.jar"
    "--TempDir"                          = "s3://${aws_s3_bucket.jar_bucket.bucket}/tmp/"
    "--enable-metrics"                   = ""
    "--enable-glue-datacatalog"          = ""
  }
}

resource "aws_glue_job" "phase_1_topic_2" {
  name        = "raw_process_python_topic_2"
  description = "The initial ETL job that will create delta table from the raw data from topic 2"
  role_arn    = module.glue_role.arn

  worker_type       = "G.1X"
  number_of_workers = 5
  glue_version      = "3.0"

  max_retries = 0

  connections = [aws_glue_connection.kafka.name]

  execution_property {
    max_concurrent_runs = 20
  }

  command {
    name            = "gluestreaming"
    script_location = "s3://${aws_s3_bucket.jar_bucket.bucket}/scripts/raw_phase_1_topic_2.py"
    python_version  = 3
  }

  default_arguments = {
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--bootstrap_servers"                = module.kafka.bootstrap_brokers_iam
    "--bucket_name"                      = aws_s3_bucket.data_bucket.bucket
    "--topic"                            = var.kafka_test_topic_2
    "--extra-jars"                       = "s3://${aws_s3_bucket.jar_bucket.bucket}/delta-core_2.12-1.0.0.jar,s3://${aws_s3_bucket.jar_bucket.bucket}/aws-msk-iam-auth-1.1.0-all.jar"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.jar_bucket.bucket}/delta-core_2.12-1.0.0.jar"
    "--TempDir"                          = "s3://${aws_s3_bucket.jar_bucket.bucket}/tmp/"
    "--enable-metrics"                   = ""
    "--enable-glue-datacatalog"          = ""
  }
}

resource "aws_glue_connection" "kafka" {
  connection_type = "KAFKA"
  connection_properties = {
    KAFKA_BOOTSTRAP_SERVERS = module.kafka.bootstrap_brokers_iam
    KAFKA_SSL_ENABLED       = true
  }

  name = "kafka-connect"

  physical_connection_requirements {
    availability_zone      = data.aws_subnet.selected.availability_zone
    security_group_id_list = var.create_new_vpc ? [aws_default_security_group.default[0].id] : var.security_group_ids
    subnet_id              = var.create_new_vpc ? module.application_subnets.private_subnet_ids[0] : var.subnet_ids[0]
  }
}

data "aws_subnet" "selected" {
  id = var.create_new_vpc ? module.application_subnets.private_subnet_ids[0] : var.subnet_ids[0]
}

resource "aws_glue_job" "phase_2" {
  name         = "upsert_process_scala"
  description  = "The second phase etl that will do the upsert"
  role_arn     = module.glue_role.arn
  glue_version = "3.0"

  worker_type       = "G.1X"
  number_of_workers = 5

  command {
    name            = "gluestreaming"
    script_location = "s3://${aws_s3_bucket.jar_bucket.bucket}/scripts/upsert_phase_2.scala"
  }

  execution_property {
    max_concurrent_runs = 20
  }

  default_arguments = {
    "--job-language"                     = "scala"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--bucket_name"                      = aws_s3_bucket.data_bucket.bucket
    "--extra-jars"                       = "s3://${aws_s3_bucket.jar_bucket.bucket}/delta-core_2.12-1.0.0.jar"
    "--TempDir"                          = "s3://${aws_s3_bucket.jar_bucket.bucket}/tmp/"
    "--enable-metrics"                   = ""
    "--enable-glue-datacatalog"          = ""
    "--job-bookmark-option"              = "job-bookmark-enable"
  }
}

data "aws_iam_policy_document" "s3_full_access" {
  statement {
    sid    = "FullAccess"
    effect = "Allow"
    resources = [
      aws_s3_bucket.jar_bucket.arn,
      format("%s/*", aws_s3_bucket.jar_bucket.arn),
      aws_s3_bucket.data_bucket.arn, format("%s/*",
      aws_s3_bucket.data_bucket.arn)
    ]

    actions = [
      "s3:*"
    ]
  }
}

data "aws_iam_policy_document" "kafka_full_access" {
  statement {
    sid    = "kafkaAccess1"
    effect = "Allow"
    resources = [
      module.kafka.cluster_arn
    ]

    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:AlterCluster",
      "kafka-cluster:DescribeCluster"
    ]
  }

  statement {
    sid    = "kafkaAccess2"
    effect = "Allow"
    resources = [
      "*"
    ]

    actions = [
      "kafka-cluster:*Topic*",
      "kafka-cluster:WriteData",
      "kafka-cluster:ReadData"
    ]
  }

  statement {
    sid    = "kafkaAccess3"
    effect = "Allow"
    resources = [
      "*"
    ]

    actions = [
      "kafka-cluster:AlterGroup",
      "kafka-cluster:DescribeGroup"
    ]
  }
}

data "aws_iam_policy_document" "glue_full_access" {
  statement {
    sid    = "GlueFullAccess"
    effect = "Allow"
    resources = [
      "*"
    ]

    actions = [
      "glue:*",
      "ec2:*"
    ]
  }
}

module "glue_role" {
  source = "cloudposse/iam-role/aws"
  # Cloud Posse recommends pinning every module to a specific version
  version = "0.16.0"

  namespace = var.namespace
  stage     = var.stage
  name      = "glue_s3_admin"

  policy_description = "Allow S3 FullAccess"
  role_description   = "glue role with full access to s3 resource"

  principals = {
    Service = ["glue.amazonaws.com"]
  }

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/AdministratorAccess"
  ]
}

resource "aws_s3_bucket" "jar_bucket" {
  bucket_prefix = "central-jar-bucket-"
  force_destroy = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "default-encryption" {
  bucket = aws_s3_bucket.jar_bucket.bucket

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_object" "delta" {
  bucket = aws_s3_bucket.jar_bucket.bucket
  key    = "delta-core_2.12-1.0.0.jar"
  source = "../delta_core/delta-core_2.12-1.0.0.jar"

  etag = filemd5("../delta_core/delta-core_2.12-1.0.0.jar")
}

resource "aws_s3_bucket_object" "msk_iam" {
  bucket = aws_s3_bucket.jar_bucket.bucket
  key    = "aws-msk-iam-auth-1.1.0-all.jar"
  source = "../delta_core/aws-msk-iam-auth-1.1.0-all.jar"

  etag = filemd5("../delta_core/aws-msk-iam-auth-1.1.0-all.jar")
}

resource "aws_s3_bucket_object" "phase1_script" {
  bucket = aws_s3_bucket.jar_bucket.bucket
  key    = "scripts/raw_phase_1.py"
  source = "../spark_scripts/raw_phase_1.py"

  etag = filemd5("../spark_scripts/raw_phase_1.py")
}

resource "aws_s3_bucket_object" "phase1_script_topic_2" {
  bucket = aws_s3_bucket.jar_bucket.bucket
  key    = "scripts/raw_phase_1_topic_2.py"
  source = "../spark_scripts/raw_phase_1_topic_2.py"

  etag = filemd5("../spark_scripts/raw_phase_1_topic_2.py")
}

resource "aws_s3_bucket_object" "phase2_script" {
  bucket = aws_s3_bucket.jar_bucket.bucket
  key    = "scripts/upsert_phase_2.scala"
  source = "../spark_scripts/upsert_phase_2.scala"

  etag = filemd5("../spark_scripts/upsert_phase_2.scala")
}

resource "aws_s3_bucket" "data_bucket" {
  bucket_prefix = "delta-test-data-bucket-"
  force_destroy = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "default-encryption-data" {
  bucket = aws_s3_bucket.data_bucket.bucket

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
  }
}