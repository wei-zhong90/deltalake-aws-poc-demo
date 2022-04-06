module "workspace" {
  source = "cloudposse/ec2-instance/aws"
  # Cloud Posse recommends pinning every module to a specific version
  version                     = "0.42.0"
  ssh_key_pair                = aws_key_pair.deployer.id
  instance_type               = var.instance_type
  vpc_id                      = var.create_new_vpc ? module.vpc.vpc_id : var.vpc_id
  security_groups             = var.create_new_vpc ? [aws_default_security_group.default[0].id, aws_security_group.ssh_access[0].id] : var.security_group_ids
  subnet                      = var.create_new_vpc ? module.application_subnets.public_subnet_ids[0] : var.subnet_ids[0]
  name                        = "workspace"
  namespace                   = var.namespace
  stage                       = var.stage
  ami                         = data.aws_ami.amazon-2.id
  ami_owner                   = data.aws_ami.amazon-2.owner_id
  security_group_enabled      = false
  root_volume_size            = 20
  user_data_base64            = filebase64("./script/user_data.sh")
  associate_public_ip_address = true
}

resource "aws_key_pair" "deployer" {
  key_name   = "msk_workspace_key"
  public_key = var.public_key
}