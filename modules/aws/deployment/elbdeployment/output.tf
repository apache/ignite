output "subnet_names" {
  value = [for s in data.aws_subnet.subnet_values: s.id]
}

