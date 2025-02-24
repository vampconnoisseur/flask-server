provider "aws" {
  region = "us-east-1"
}

variable "image_tag" {}

data "aws_instances" "existing_flask_server" {
  filter {
    name   = "tag:Name"
    values = ["flask_server"]
  }
}

resource "aws_security_group" "flask_sg" {
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

  lifecycle {
    ignore_changes = [user_data]
  }
}

resource "null_resource" "update_flask_server" {
  count = length(data.aws_instances.existing_flask_server.ids) > 0 ? 1 : 0

  triggers = {
    always_run  = timestamp()
    instance_id = data.aws_instances.existing_flask_server.ids[0]
    image_tag   = var.image_tag
  }

  connection {
    type        = "ssh"
    user        = "ubuntu"
    private_key = file("./my_key.pem")
    host        = data.aws_instances.existing_flask_server.public_ips[0]
  }

  provisioner "remote-exec" {
    inline = [
      "echo 'Updating Flask server container...'",

      "if ! command -v docker &> /dev/null; then sudo apt-get update -y && sudo apt-get install -y docker.io; sudo systemctl start docker; sudo systemctl enable docker; fi",

      "if sudo docker ps -a --format '{{.Names}}' | grep -q '^flask-container$'; then sudo docker stop flask-container && sudo docker rm flask-container; fi",

      "sudo docker pull vampconnoisseur/flask-server:${var.image_tag}",

      "sudo docker run -d --name flask-container -p 8765:8765 vampconnoisseur/flask-server:${var.image_tag}"
    ]
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

  backend "s3" {}
}
