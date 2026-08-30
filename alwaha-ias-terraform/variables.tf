variable "project_name" {
  type = string
  description = "The name of the project."
}

variable "environment" {
  type = list(string)
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

variable "databricks_account_id" {
  description = "Account id for databricks"
  type = string
}

variable "azure_tenant_id" {
  type = string
  description = "tenant id"
  
}

variable "azure_client_id" {
  description = "Azure Service Principal Client ID"
  type        = string
}

variable "azure_client_secret" {
  description = "Azure Service Principal Client Secret"
  type        = string
  sensitive   = true
}

variable "databricks_token" {
  description = "Databricks Personal Access Token"
  type        = string
  sensitive   = true
}