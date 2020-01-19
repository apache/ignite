variable "vpc_id" {
  type = string
  default = "vpc-8efaaceb"
}

variable "launch_configuration_name" {
  type = string
  default = "ignite_launch_configuration"
}

variable "auto_scalling_group_name" {
  type = string
  default = "ignite_autoscalling_group"
}

variable "image_id" {
  type = string
  default =  "ami-0713f98de93617bb4"
}

variable "instance_type" {
  type = string
  default = "t2.micro"
}

variable "autoscalling_group_elb_name" {
  type = string
  default = "igniteelbbasedautoscallinggroup"
}

variable "elb_security_group_name" {
  type = string
  default = "ignite_elb_security_group"
}

variable "ec2_security_group" {
  type = string
  default = "ignite_security_group"
}

variable "instances_role" {
  type = string
  default = "ignite_elb_node_role"
}

variable "ec2_elb_policy" {
  type = string
  default = "ignite_ec2_elb_policy"
}

variable "ec2_elb_profile" {
  type = string
  default = "ignite_ec2_elb_profile"
}