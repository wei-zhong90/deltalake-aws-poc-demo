variable "region" {
  type        = string
  description = "the region for deployment"
}

variable "namespace" {
  type    = string
  default = "msk"
}

variable "stage" {
  type    = string
  default = "dev"
}

variable "create_new_vpc" {
  type        = bool
  description = "whether it will create new vpc"
  default     = true
}

variable "vpc_cidr" {
  type        = string
  description = "the cidr block for the new vpc"
  default     = "10.0.0.0/16"
}

variable "broker_instance_type" {
  type    = string
  default = "kafka.t3.small"
}

variable "vpc_id" {
  type    = string
  default = ""
}

variable "subnet_ids" {
  type    = list(string)
  default = []
}

variable "security_group_ids" {
  type    = list(string)
  default = []
}

variable "instance_type" {
  type    = string
  default = "t3.small"
}

variable "public_key" {
  type        = string
  description = "The rsa key pair for logging in workspace instance"
}

variable "kafka_test_topic" {
  type    = string
  default = "deltatest"
}