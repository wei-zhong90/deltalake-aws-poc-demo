module "vpc" {
  source                          = "cloudposse/vpc/aws"
  version                         = "0.28.1"
  namespace                       = var.namespace
  stage                           = var.stage
  name                            = "kafka-stream"
  enabled                         = var.create_new_vpc
  cidr_block                      = var.vpc_cidr
  default_security_group_deny_all = false
}

module "application_subnets" {
  source = "cloudposse/dynamic-subnets/aws"
  # Cloud Posse recommends pinning every module to a specific version
  version            = "0.39.8"
  namespace          = var.namespace
  stage              = var.stage
  name               = "app"
  enabled            = var.create_new_vpc
  availability_zones = data.aws_availability_zones.available.names
  vpc_id             = module.vpc.vpc_id
  igw_id             = module.vpc.igw_id
  cidr_block         = var.vpc_cidr
}

resource "aws_default_security_group" "default" {
  count  = var.create_new_vpc ? 1 : 0
  vpc_id = module.vpc.vpc_id

  ingress {
    protocol  = -1
    self      = true
    from_port = 0
    to_port   = 0
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "ssh_access" {
  count                  = var.create_new_vpc ? 1 : 0
  name_prefix            = "workspace_access_"
  vpc_id                 = module.vpc.vpc_id
  revoke_rules_on_delete = true

  ingress {
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    from_port   = 22
    to_port     = 22
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}