output "ssh_keypair_name" {
  value = aws_key_pair.deployer.id
}

output "ec2_public_ip" {
  value = module.workspace.public_ip
}

output "kafka_iam_connection_string" {
  value = aws_msk_cluster.kafka.bootstrap_brokers_sasl_iam
}

output "kafka_tls_connection_string" {
  value = aws_msk_cluster.kafka.bootstrap_brokers_tls
}

output "data_bucket_name" {
  value = aws_s3_bucket.data_bucket.bucket
}