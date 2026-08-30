data "azurerm_client_config" "current" {}

#-------------------------------------
# AZURE INFRASTRUCTURE (AZURE PORTAL)
#-------------------------------------

#1. Resource Group
resource "azurerm_resource_group" "rg"{
    name = lower("${var.project_name}-${var.environment[0]}-rg")
    location = var.location
    tags = var.tags
}

#2. ADLS GEN2 STOAGE ACCOUNT
resource "azurerm_storage_account" "adls" {
    name = lower(replace("${var.project_name}${var.environment[0]}001", "-", ""))
    resource_group_name = azurerm_resource_group.rg.name
    location = azurerm_resource_group.rg.location
    account_tier = "Standard"
    account_replication_type = "LRS"
    is_hns_enabled = true
}

#3. AZURE STORAGE CONTAINER

resource "azurerm_storage_container" "adls_container" {
    for_each = toset((["landing", "bronze", "silver", "gold", "monitoring"]))
    name  = each.value
    storage_account_name = azurerm_storage_account.adls.name
    container_access_type = "private"
}

#4. AZURE KEY VAULT
resource "azurerm_key_vault" "akv" {
    name = lower("${replace(var.project_name, "-", "")}-${var.environment[0]}")
    location = azurerm_resource_group.rg.location
    resource_group_name = azurerm_resource_group.rg.name
    tenant_id = data.azurerm_client_config.current.tenant_id
    sku_name = "standard"
}

#5. AZURE DATA FACTORY
resource "azurerm_data_factory" "adf" {
    name = lower("adf-${var.project_name}-${var.environment[0]}-001")
    location = azurerm_resource_group.rg.location
    resource_group_name = azurerm_resource_group.rg.name

    identity {
        type = "SystemAssigned"
    }

    github_configuration {
        account_name       = "mehran80"
        branch_name        = "main"
        git_url             = "https://github.com"
        publishing_enabled = true
        repository_name    = "Al-Waha-Bank-Real-Time-Fraud"
        root_folder        = "/adf"
    }
}

#6. AZURE ADF KEY VAULT 
resource "azurerm_role_assignment" "adf_to_keyvault" {
    scope = azurerm_key_vault.akv.id
    role_definition_name = "Key Vault Secrets User"
    principal_id = azurerm_data_factory.adf.identity[0].principal_id
}


#7. RBAC ROLE ASSIGNMENT FOR AZURE DATA FACTORY
resource "azurerm_role_assignment" "adf_to_storage" {
    scope = azurerm_storage_account.adls.id
    role_definition_name = "Storage Blob Data Contributor"
    principal_id = azurerm_data_factory.adf.identity[0].principal_id
}

#8. ACCESS CONNECTOR FOR AZURE DATABRICKS WORKSPACE
resource "azurerm_databricks_access_connector" "db_access_connector" {
    name = lower("dbac-${var.project_name}-${var.environment[0]}-001")
    resource_group_name = azurerm_resource_group.rg.name
    location = azurerm_resource_group.rg.location

    identity {
        type = "SystemAssigned"
    }
}

#9. AZURE DATABRICKS ACCESS CONNECTOR TO STORAGE ACCOUNT
resource "azurerm_role_assignment" "db_access_connector_to_storage" {
    scope = azurerm_storage_account.adls.id
    role_definition_name = "Storage Blob Data Contributor"
    principal_id = azurerm_databricks_access_connector.db_access_connector.identity[0].principal_id
}


#10. AZURE DATABRICKS WORKSPACE
resource "azurerm_databricks_workspace" "db_ws" {
    name = lower("dbw-${var.project_name}-${var.environment[0]}-001")
    location = azurerm_resource_group.rg.location
    resource_group_name = azurerm_resource_group.rg.name
    sku = "premium"
}

#11. RBAC ROLE ASSIGNMENT FOR AZURE DATABRICKS WORKSPACE

# resource "azurerm_role_assignment" "adf_to_databricks" {
#   scope                = azurerm_databricks_workspace.db_ws.id
#   role_definition_name = "Contributor"
#   principal_id         = azurerm_data_factory.adf.identity[0].principal_id
# }


#-------------------------------------------------
# DATABRICKS INFRASTRUCTURE (DATABRICKS PORTAL)
#-------------------------------------------------

provider "databricks" {
    host  = azurerm_databricks_workspace.db_ws.workspace_url
    token = var.databricks_token
}

provider "databricks" {
    alias           = "account"
    host            = "https://accounts.azuredatabricks.net"
    account_id      = var.databricks_account_id
    azure_tenant_id = data.azurerm_client_config.current.tenant_id
    azure_client_id     = var.azure_client_id
    azure_client_secret = var.azure_client_secret
}


#-------------------------------------------------
# DATABRICKS ACCOUNT-LEVEL GROUPS
#-------------------------------------------------

resource "databricks_group" "analyst" {
    provider = databricks.account
    display_name = "analyst"
  
}

resource "databricks_group" "compliance_officer" {
    provider = databricks.account
    display_name = "compliance_officer"
  
}

resource "databricks_group" "data_engineers" {
    provider =  databricks.account
    display_name =  "data_engineers"
  
}

resource "databricks_group" "pipeline_service" {
    provider = databricks.account
    display_name = "pipeline_service"
  
}

#-------------------------------------------------
# DATABRICKS MWS PERMISSION ASSIGNMENT
#-------------------------------------------------

resource "databricks_mws_permission_assignment" "analyst_to_workspace" {
    provider = databricks.account
    workspace_id = azurerm_databricks_workspace.db_ws.workspace_id
    principal_id = databricks_group.analyst.id
    permissions = ["USER"]
  
}

resource "databricks_mws_permission_assignment" "compliance_to_workspace" {
    provider = databricks.account
    workspace_id =  azurerm_databricks_workspace.db_ws.workspace_id
    principal_id = databricks_group.compliance_officer.id
    permissions = ["USER"]
  
}

resource "databricks_mws_permission_assignment" "data_engineers_to_workspace" {
    provider = databricks.account
    workspace_id = azurerm_databricks_workspace.db_ws.workspace_id
    principal_id = databricks_group.data_engineers.id
    permissions = ["USER"]
}

resource "databricks_mws_permission_assignment" "pipeline_service_to_workspace" {
    provider = databricks.account
    workspace_id = azurerm_databricks_workspace.db_ws.workspace_id
    principal_id =  databricks_group.pipeline_service.id
    permissions = ["USER"]
  
}

#-------------------------------------------------
# ADDING USERS IN GROUPS
#-------------------------------------------------

data "databricks_user" "ali_baloch" {
    provider = databricks.account
    user_name = "mehran8023@gmail.com"
  
}

data "databricks_service_principal" "adf_alwaha_service_principal" {
    provider = databricks.account
    application_id = "5c696b18-3112-42e5-95d4-9294ac7b22d3"
  
}



resource "databricks_group_member" "ali_in_engineers" {
    provider = databricks.account
    group_id = databricks_group.data_engineers.id
    member_id = data.databricks_user.ali_baloch.id
  
}

resource "databricks_group_member" "adf_in_pipeline" {
    provider = databricks.account
    group_id = databricks_group.pipeline_service.id
    member_id = data.databricks_service_principal.adf_alwaha_service_principal.id
}



#1. STORAGE CREDENTIAL
resource "databricks_storage_credential" "storage_credential" {
    name = lower(replace("${var.project_name}_${var.environment[0]}_storage_credential", "-", "_"))
    comment = "Storage Credential using Access Connector Managed Identity"
    azure_managed_identity {
        access_connector_id = azurerm_databricks_access_connector.db_access_connector.id
    }
    depends_on = [azurerm_role_assignment.db_access_connector_to_storage]
}

#3. EXTERNAL LOCATION
resource "databricks_external_location" "external_location" {
    for_each = toset((["landing", "bronze", "silver", "gold", "monitoring"]))
    name = "ext_loc_${each.value}"
    url = "abfss://${each.value}@${azurerm_storage_account.adls.name}.dfs.core.windows.net/"
    credential_name = databricks_storage_credential.storage_credential.id
    comment = "External Location for ${each.key} container"
}

#4. UNITY CATALOG
resource "databricks_catalog" "catalog"{
    name = lower(replace("${var.project_name}_${var.environment[0]}_001", "-", "_"))
    comment = "Unity Catalog for ${var.project_name} ${var.environment[0]}"
    storage_root = "abfss://landing@${azurerm_storage_account.adls.name}.dfs.core.windows.net/"
}

#2. SCHEMA
resource "databricks_schema" "schema" {
    for_each = toset((["landing","bronze", "silver", "gold", "monitoring", "governance"]))
    catalog_name = databricks_catalog.catalog.name
    name = each.value

    storage_root = each.key == "governance" ? null : "abfss://${each.value}@${azurerm_storage_account.adls.name}.dfs.core.windows.net/"

    lifecycle {
        ignore_changes = [storage_root]
    }
    
}

#-------------------------------------------------
# DATABRICKS GROUP PERMISSIONA GRANTS
#-------------------------------------------------

resource "databricks_grants" "catalog_grants" {
  catalog = databricks_catalog.catalog.name

  grant {
    principal  = "account users"
    privileges = ["USE_CATALOG"]
  }
}

resource "databricks_grants" "schema_grants" {
    for_each = databricks_schema.schema
    schema   = each.value.id

    depends_on = [
        databricks_grants.catalog_grants
    ]

    dynamic "grant" {
        for_each = contains(["gold"], each.key) ? [1] : []
        content {
          principal  = databricks_group.analyst.display_name
          privileges = ["SELECT"]
        }
    }

    dynamic "grant" {
        for_each = contains(["silver", "gold"], each.key) ? [1] : []
        content {
          principal  = databricks_group.compliance_officer.display_name
          privileges = ["SELECT"]
        }
    }

    dynamic "grant" {
        for_each = contains(["landing","bronze","silver", "gold", "monitoring", "governance"], each.key) ? [1] : []
        content {
          principal  = databricks_group.data_engineers.display_name
          privileges = ["SELECT", "MODIFY", "CREATE_TABLE", "CREATE_FUNCTION", "USE_SCHEMA"]
        }
    }

    dynamic "grant" {
        for_each = contains(["landing","bronze", "silver", "gold", "monitoring", "governance"], each.key) ? [1] : []
        content {
          principal  = databricks_group.pipeline_service.display_name
          privileges = ["SELECT", "MODIFY", "CREATE_TABLE"]
        }
    }
}

#-------------------------------------------------------
#     Unity  Catalog Permissions
#-------------------------------------------------------
resource "databricks_grants" "externel_location_grant" {
    for_each = toset(["landing", "bronze", "silver", "gold", "monitoring"])
    external_location = databricks_external_location.external_location[each.key].id

    depends_on = [
        databricks_grants.schema_grants
    ]

    grant {
      principal = databricks_group.data_engineers.display_name
      privileges = ["READ_FILES", "WRITE_FILES", "CREATE_EXTERNAL_TABLE", "CREATE_EXTERNAL_VOLUME"]
    }

    grant {
      principal = databricks_group.pipeline_service.display_name
      privileges = ["READ_FILES", "WRITE_FILES", "CREATE_EXTERNAL_TABLE"]
    }
  
}

resource "databricks_grants" "storage_credential_grant" {
    storage_credential = databricks_storage_credential.storage_credential.id

    depends_on = [
        databricks_grants.schema_grants
    ]

    grant {
      principal = databricks_group.data_engineers.display_name
      privileges = ["CREATE_EXTERNAL_LOCATION", "READ_FILES", "WRITE_FILES"]
    }
  
}

#-------------------------------------------------------
#                 LINKED SERVICES 
#-------------------------------------------------------

#1. LINKED SERVICE FOR AZURE KEY VAULT
resource "azurerm_data_factory_linked_service_key_vault" "ls_kv"{
    name = "ls_key_vault"
    data_factory_id = azurerm_data_factory.adf.id
    key_vault_id = azurerm_key_vault.akv.id
}

#2. LINKED SERVICE FOR AZURE STORAGE ACCOUNT
resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "ls_adls"{
    name = "ls_adls"
    data_factory_id = azurerm_data_factory.adf.id
    url = azurerm_storage_account.adls.primary_dfs_endpoint
    use_managed_identity = true
}

#3. LINKED SERVICE FOR AZURE DATABRICKS
resource "azurerm_data_factory_linked_service_azure_databricks" "ls_adb"{
    name = "ls_adb"
    data_factory_id = azurerm_data_factory.adf.id
    adb_domain = "https://${azurerm_databricks_workspace.db_ws.workspace_url}"
    msi_work_space_resource_id = azurerm_databricks_workspace.db_ws.id

    new_cluster_config {
      node_type = "Standard_DS3_v2"
      cluster_version = "14.3.x-scala2.12"
      min_number_of_workers = 1
      max_number_of_workers = 1
    }
}