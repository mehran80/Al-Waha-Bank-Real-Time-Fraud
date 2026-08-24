variable "project_name" {
  type = string
  description = "The name of the project."
}

variable "environment" {
  type = string
  description = "The environment of the project."
}

variable "location" {
    type = string
    description = "The location of the project."
}

variable "tags"{
    type = map(string)
    description = "The tags of the project."
}

variable "subscription_id" {
  type = string
  description = "The subscription ID for the Azure provider."
}