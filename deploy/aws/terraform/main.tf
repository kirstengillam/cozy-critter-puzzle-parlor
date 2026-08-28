terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Bucket/key/region/locking are supplied at `terraform init` time via
  # init.sh's -backend-config flags, not here — the bucket name embeds
  # the AWS account id, which we don't want committed to git. See
  # init.sh for why.
  backend "s3" {}
}

provider "aws" {
  region = var.region
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }

  # us-east-1e doesn't support t3.small in this account; restrict to AZs that do.
  filter {
    name   = "availability-zone"
    values = ["us-east-1a", "us-east-1b", "us-east-1c", "us-east-1d", "us-east-1f"]
  }
}

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    # "al2023-ami-2*" (not "al2023-ami-*") deliberately excludes the
    # ECS-optimized variant (al2023-ami-ecs-hvm-*), which ships with an
    # ecs-agent container that crash-loops with no ECS cluster to join.
    name   = "name"
    values = ["al2023-ami-2*-x86_64"]
  }
  filter {
    name   = "architecture"
    values = ["x86_64"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_security_group" "gateway" {
  name        = "cozy-critter-gateway"
  description = "Cozy Critter: allow HTTP/HTTPS from the internet only"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "HTTP (ACME challenge + redirect)"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "HTTPS"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Project = "cozy-critter-puzzle-parlor"
  }
}

resource "aws_iam_role" "ssm" {
  name = "cozy-critter-ec2-ssm"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })

  tags = {
    Project = "cozy-critter-puzzle-parlor"
  }
}

resource "aws_iam_role_policy_attachment" "ssm_core" {
  role       = aws_iam_role.ssm.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "ssm" {
  name = "cozy-critter-ec2-ssm"
  role = aws_iam_role.ssm.name
}

resource "aws_instance" "gateway" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.instance_type
  subnet_id              = data.aws_subnets.default.ids[0]
  vpc_security_group_ids = [aws_security_group.gateway.id]
  iam_instance_profile   = aws_iam_instance_profile.ssm.name

  root_block_device {
    volume_type = "gp3"
    volume_size = 30
  }

  user_data = templatefile("${path.module}/user_data.sh.tpl", {})

  tags = {
    Name    = "cozy-critter-gateway"
    Project = "cozy-critter-puzzle-parlor"
  }

  lifecycle {
    # data.aws_ami.al2023 re-resolves to whatever's newest at plan time, so
    # without this every plan against the already-running instance wants
    # to replace it on a newer AL2023 patch AMI. Pin to whatever it's
    # actually running; bump deliberately (terraform taint or a manual
    # apply with this removed) rather than accidentally on every apply.
    ignore_changes = [ami]
  }
}

resource "aws_eip" "gateway" {
  instance = aws_instance.gateway.id
  domain   = "vpc"

  tags = {
    Project = "cozy-critter-puzzle-parlor"
  }
}

# GitHub Actions CD role: lets the "deploy" job in .github/workflows/ci.yml
# trigger deploy/aws/update.sh's commands via SSM, without any long-lived
# AWS credentials sitting in GitHub Secrets. Looked up by url (not arn) so
# this file doesn't need the account id — the provider itself is shared
# across this AWS account's projects, not owned by this one.
data "aws_iam_openid_connect_provider" "github" {
  url = "https://token.actions.githubusercontent.com"
}

data "aws_region" "current" {}

resource "aws_iam_role" "gha_deploy" {
  name = "cozy-critter-gha-deploy"

  # Scoped to this repo's main branch only — a workflow run from a fork,
  # a PR branch, or any other repo can't assume this role. The real
  # scoping is repository/ref (StringEquals, exact) rather than sub:
  # GitHub appends numeric owner/repo IDs to sub
  # (repo:owner@id/name@id:ref:...) once an account or repo has ever been
  # renamed, specifically so a renamed-away name can't inherit an old
  # trust relationship — repository/ref stay plain strings regardless.
  # AWS separately *requires* a sub (or job_workflow_ref) condition on
  # any GitHub OIDC trust policy, so sub is here too (StringLike, with a
  # wildcard tolerating that optional @id suffix) purely to satisfy that
  # rule — repository/ref above is what's actually doing the scoping.
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRoleWithWebIdentity"
      Effect    = "Allow"
      Principal = { Federated = data.aws_iam_openid_connect_provider.github.arn }
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud"        = "sts.amazonaws.com"
          "token.actions.githubusercontent.com:repository" = "kirstengillam/cozy-critter-puzzle-parlor"
          "token.actions.githubusercontent.com:ref"        = "refs/heads/main"
        }
        StringLike = {
          "token.actions.githubusercontent.com:sub" = "repo:kirstengillam*/cozy-critter-puzzle-parlor*:ref:refs/heads/main"
        }
      }
    }]
  })

  tags = {
    Project = "cozy-critter-puzzle-parlor"
  }
}

resource "aws_iam_role_policy" "gha_deploy_ssm" {
  name = "ssm-deploy"
  role = aws_iam_role.gha_deploy.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "SendDeployCommand"
        Effect = "Allow"
        Action = "ssm:SendCommand"
        Resource = [
          aws_instance.gateway.arn,
          "arn:aws:ssm:${data.aws_region.current.name}::document/AWS-RunShellScript",
        ]
      },
      {
        # Addressed by command id, not instance ARN, so this one can't be
        # scoped any tighter than "*".
        Sid      = "PollDeployStatus"
        Effect   = "Allow"
        Action   = "ssm:GetCommandInvocation"
        Resource = "*"
      },
    ]
  })
}
