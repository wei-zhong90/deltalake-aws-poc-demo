output "ssh_keypair_name" {
  value = aws_key_pair.deployer.id
}

output "ec2_public_ip" {
  value = module.workspace.public_ip
}

output "kafka_iam_connection_string" {
  value = module.kafka.bootstrap_brokers_iam
}