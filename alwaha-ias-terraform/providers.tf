terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
        source = "hashicorp/azurerm"
        version = "~> 4.0.0"
    }

    databricks = {
        source = "databricks/databricks"
        version = "~> 1.50.0"
    }
  }
}


provider "azurerm" {
    subscription_id = var.subscription_id
    client_id       = var.azure_client_id
    client_secret   = var.azure_client_secret
    tenant_id       = var.azure_tenant_id
    features {
        key_vault{
            purge_soft_delete_on_destroy = true
        }
    }
}