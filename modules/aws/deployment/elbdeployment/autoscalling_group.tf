data "aws_subnet_ids" "subnets" {
  vpc_id = var.vpc_id
}

data "aws_subnet" "subnet_values" {
  for_each = data.aws_subnet_ids.subnets.ids
  id       = each.value
}

data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_security_group" "elb_security_group" {
  name = var.elb_security_group_name
  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    from_port = 80
    to_port = 8080
    protocol = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_elb" "autoscalling_group_elb" {
  name = var.autoscalling_group_elb_name
  security_groups = [aws_security_group.elb_security_group.id]
  availability_zones = data.aws_availability_zones.available.names
  health_check {
    healthy_threshold = 2
    unhealthy_threshold = 2
    timeout = 3
    interval = 30
    target = "HTTP:8080/ignite?cmd=version"
  }
  listener {
    lb_port = 80
    lb_protocol = "http"
    instance_port = "8080"
    instance_protocol = "http"
  }
}

resource "aws_security_group" "ec2_security_group" {
  name = var.ec2_security_group
  ingress {
    from_port = 8080
    to_port = 8080
    protocol = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    from_port = 47500
    protocol = "tcp"
    to_port = 47600
    self = true
  }
  ingress {
    from_port = 47100
    protocol = "tcp"
    to_port = 47200
    self = true
  }
  egress {
    from_port = 0
    protocol = "-1"
    to_port = 0
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_iam_role" "instances_role" {
  name = var.instances_role
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "ec2_elb_policy" {
  name = var.ec2_elb_policy
  role = aws_iam_role.instances_role.id

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "ec2:Describe*",
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": "elasticloadbalancing:Describe*",
      "Resource": "*"
    }
  ]
}
EOF
}

resource "aws_iam_instance_profile" "ec2_elb_profile" {
  name = var.ec2_elb_profile
  role = aws_iam_role.instances_role.id
}

resource "template_file" "load_balancer_ignite_config" {
  template = file("./elbconfiguration.xml")
}

resource"aws_launch_configuration" "launch_configuration" {
  name = var.launch_configuration_name
  image_id = var.image_id
  instance_type = var.instance_type
  security_groups = [aws_security_group.ec2_security_group.id]
  key_name = "paparis"
  iam_instance_profile = aws_iam_instance_profile.ec2_elb_profile.id
  user_data = <<-EOF
              #!/bin/bash
              yum install java unzip -y
              curl https://www-eu.apache.org/dist/ignite/2.7.6/apache-ignite-2.7.6-bin.zip -o apache-ignite.zip
              unzip apache-ignite.zip -d /opt/apache-ignite
              cd /opt/apache-ignite/apache-ignite-2.7.6-bin/
              cp -r libs/optional/ignite-rest-http/ libs/ignite-rest-http/
              cp -r libs/optional/ignite-aws/ libs/ignite-aws/
              echo ${base64encode(template_file.load_balancer_ignite_config.rendered)}|base64 --decode > examples/config/loadbalancer.xml
              ./bin/ignite.sh ./examples/config/loadbalancer.xml > ignite.log
              EOF
}

resource "aws_autoscaling_group" "elb_based_autoscallin_group" {
  name = var.auto_scalling_group_name
  max_size = 3
  min_size = 1
  health_check_grace_period = 300
  health_check_type = "ELB"
  desired_capacity = 2
  force_delete = true
  vpc_zone_identifier = [for s in data.aws_subnet.subnet_values: s.id]
  load_balancers = [aws_elb.autoscalling_group_elb.name]
  launch_configuration = aws_launch_configuration.launch_configuration.name

  lifecycle {
    create_before_destroy = true
  }
}


