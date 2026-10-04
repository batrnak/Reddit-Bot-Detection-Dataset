variable "project" {
  type    = string
  default = "data-pipeline"
}

variable "region" {
  type    = string
  default = "ap-southeast-1" 
}

variable "account_id" {
  type    = string
  default = "537390611425" 
}

variable "monthly_budget" {
  type    = string
  default = "30" 
}


variable "alert_email" {
  type = string
  default = "batrna2006@gmail.com"
}