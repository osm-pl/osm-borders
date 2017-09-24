/*  add autoscaling later */
variable "name" {}

resource  "aws_dynamodb_table" "${name}" {
  name = "${name}"
  read_capacity = 1
  write_capacity = 1
  hash_key = "key"
  attribute {
    name = "key"
    type = "S"
  }
}

