provider "aws" {
  region = "us-east-1"
}

variable "image_tag" {}

resource "null_resource" "delete_existing_sg" {
  provisioner "local-exec" {
    command = <<EOT
      EXISTING_SG_ID=$(aws ec2 describe-security-groups --filters Name=group-name,Values=flask_sg --query "SecurityGroups[0].GroupId" --output text 2>/dev/null)
      if [[ "$EXISTING_SG_ID" != "None" ]]; then
        aws ec2 delete-security-group --group-id $EXISTING_SG_ID
      fi
    EOT
  }
}

resource "aws_security_group" "flask_sg" {
  depends_on  = [null_resource.delete_existing_sg]
  name        = "flask_sg"
  description = "Allow inbound traffic to Flask app"

  ingress {
    from_port   = 8765
    to_port     = 8765
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "flask_server" {
  ami           = "ami-04b4f1a9cf54c11d0"
  instance_type = "t2.micro"
  key_name      = "my_key"

  vpc_security_group_ids = [aws_security_group.flask_sg.id]

  depends_on = [aws_security_group.flask_sg]

  user_data = <<-EOF
            #!/bin/bash
            set -e

            echo "Updating system packages..."
            sudo apt-get update -y
            sudo apt-get install -y ca-certificates curl git docker.io

            echo "Starting Docker service..."
            sudo systemctl start docker
            sudo systemctl enable docker

            echo "Pulling new Docker image: vampconnoisseur/flask-server:${var.image_tag}"
            sudo docker pull vampconnoisseur/flask-server:${var.image_tag}

            echo "Running new container..."
            sudo docker run -d --name flask-container -p 8765:8765 vampconnoisseur/flask-server:${var.image_tag}
  EOF

  tags = {
    Name = "flask_server"
  }
}

output "public_ip" {
  value = aws_instance.flask_server.public_ip
}

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.46.0"
    }
  }
}
