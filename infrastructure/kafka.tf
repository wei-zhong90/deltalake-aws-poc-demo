module "kafka" {
  source = "cloudposse/msk-apache-kafka-cluster/aws"
  # Cloud Posse recommends pinning every module to a specific version
  version = "0.8.4"

  namespace               = var.namespace
  stage                   = var.stage
  name                    = "app"
  vpc_id                  = var.create_new_vpc ? module.vpc.vpc_id : var.vpc_id
  subnet_ids              = var.create_new_vpc ? module.application_subnets.private_subnet_ids : var.subnet_ids
  kafka_version           = "2.6.2"
  number_of_broker_nodes  = var.create_new_vpc ? length(module.application_subnets.private_subnet_ids) : length(var.subnet_ids)
  broker_instance_type    = var.broker_instance_type
  client_sasl_iam_enabled = true
  client_broker           = "TLS"
  client_tls_auth_enabled = false
  create_security_group   = false

  properties = {
    "auto.create.topics.enable" = true,
    "delete.topic.enable"       = true
  }

  # security groups to put on the cluster itself
  associated_security_group_ids = var.create_new_vpc ? [aws_default_security_group.default[0].id] : var.security_group_ids
}