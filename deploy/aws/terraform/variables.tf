variable "region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "EC2 instance type. t3.small fits Kafka (tuned heap) + gateway + Caddy for casual 2-person play; bump to t3.medium if you see OOMs under real play."
  type        = string
  default     = "t3.small"
}
