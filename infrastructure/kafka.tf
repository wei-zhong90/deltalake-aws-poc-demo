resource "aws_msk_cluster" "kafka" {
  cluster_name           = "app"
  kafka_version          = "2.6.2"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    ebs_volume_size = 100
    client_subnets  = var.create_new_vpc ? module.application_subnets.private_subnet_ids : var.subnet_ids
    security_groups = var.create_new_vpc ? [aws_default_security_group.default[0].id] : var.security_group_ids
  }

  client_authentication {
    sasl {
      iam = true
    }
    unauthenticated = true
  }

  configuration_info {
    arn      = aws_msk_configuration.kafka.arn
    revision = aws_msk_configuration.kafka.latest_revision
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  open_monitoring {
    prometheus {
      jmx_exporter {
        enabled_in_broker = true
      }
      node_exporter {
        enabled_in_broker = true
      }
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.kafka.name
      }
    }
  }

  timeouts {
    create = "1h"
    update = "1h"
    delete = "30m"
  }

  # lifecycle {
  #   ignore_changes = [
  #     # Ignore changes to tags, e.g. because a management agent
  #     # updates these based on some ruleset managed elsewhere.
  #     client_authentication,
  #   ]
  # }

  tags = {
    namespace = var.namespace
  }
}

resource "aws_cloudwatch_log_group" "kafka" {
  name = "msk_broker_logs"
}

resource "aws_msk_configuration" "kafka" {
  kafka_versions = ["2.6.2"]
  name           = "kafka"

  server_properties = <<PROPERTIES
auto.create.topics.enable = true
delete.topic.enable = true
PROPERTIES
}
