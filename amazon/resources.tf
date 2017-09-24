provider "aws" {
  region = "eu-west-1"
}

data "aws_iam_policy_document" "osm_apps_lambda_policy_document_ro" {
  statement {
    sid = "1"
    effect = "Allow"
    actions = ["dynamodb:GetItem"]
    resources = [
      "${osm_prg_gminy_dict.arn}",
      "${osm_prg_wojewodztwa_dict.arn}",
    ]
  }
}

data "aws_iam_policy_document" "osm_apps_lambda_policy_document_rw" {
  statement {
    sid = "1"
    effect = "Allow"
    actions = [
      "dynamodb:BatchWriteItem",
      "dynamodb:CreateTable",
      "dynamodb:DeleteTable",
      "dynamodb:GetItem",
      "dynamodb:PutItem",
    ]
    resources = [
      "${osm_prg_gminy_dict.arn}",
      "${osm_prg_wojewodztwa_dict.arn}",
    ]
  }   
}

resource "aws_iam_role" "osm_apps_lambda_iam" {
  name = "osm_apps_lambda_iam"
  policy = "${data.aws_iam_policy_document.osm_apps_lambda_policy_document_ro.json}"
}


resource "aws_iam_user" "osm_borders_dynamo_rw" {
  name = "osm_borders_lambda"
  path = "/osm/osm-borders/"
}

resource "aws_iam_user_policy" "osm_borders_dynamo_rw_policy" {
  name = "osm_borders_dynamo_rw_policy"
  user = "${osm_borders_dynamo_rw}"
  policy = "${data.aws_iam_policy_document.osm_apps_lambda_policy_document_rw.json}"
}

resource "aws_iam_access_key" "osm_borders_lambda_access_key" {
  user    = "${aws_iam_user.osm_borderS_lambda.name}"
}

/*
  access key is here:
  access_key = ${aws_iam_access_key.osm_borders_lambda_access_key.id}
  secret = ${aws_iam_access_key.osm_borders_lambda_access_key.secret}
*/

resource "aws_lambda_function" "osm_borders_lambda" {
  filename      = "payload.zip"
  function_name = "??"
  role          = "${aws_iam_role.osm_apps_lambda_ian.arn}"
  handler       = "??"
  source_code_hash "${base64sha256(file("payload.zip"))}"
	memory_size		= 128
  runtime       = "python3.6"

  environment {
    variables {
      foo   = "bar"
    }
  }
  depends_on = ["build_lambda"]
}

module "osm_prg_gminy_dict" {
  source = "dynamo_cache"
  name = "osm_prg_gminy_dict"
}

module "osm_prg_wojewodztwa_dict" {
  source = "dynamo_cache"
  name = "osm_prg_wojewodztwa_dict"
}

resorce "null_resource" "build_lambda" {
  provisioner "local-exec" {
    command = "?"
  }
}
