output "instance_id" {
  description = "EC2 instance ID (use with: aws ssm start-session --target <id>)"
  value       = aws_instance.gateway.id
}

output "public_ip" {
  description = "Elastic IP — point the domain's apex A record here"
  value       = aws_eip.gateway.public_ip
}

output "gha_deploy_role_arn" {
  description = "Set as the AWS_DEPLOY_ROLE_ARN GitHub Actions repo variable"
  value       = aws_iam_role.gha_deploy.arn
}
