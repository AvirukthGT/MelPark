# =============================================================================
# 1. FOUNDATIONAL INFRASTRUCTURE
# =============================================================================

# Resource Group: The container for all project assets
resource "azurerm_resource_group" "rg" {
  name     = "${var.project_name}-rg"
  location = var.location
}

# Data Lake Storage Gen2: Hierarchical Namespace (HNS) must be enabled
resource "azurerm_storage_account" "storage" {
  name                     = "${var.project_name}storageacct"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true 
  
  # Best Practice: Force HTTPS and set TLS version
  https_traffic_only_enabled = true
  min_tls_version            = "TLS1_2"
}

# Containers for the Medallion Architecture
resource "azurerm_storage_data_lake_gen2_filesystem" "layers" {
  for_each           = toset(["bronze", "silver", "gold"])
  name               = each.key
  storage_account_id = azurerm_storage_account.storage.id
}

# =============================================================================
# 2. SECURITY & SECRETS MANAGEMENT
# =============================================================================

# Get current Azure client config for IDs
data "azurerm_client_config" "current" {}

# Azure Key Vault: Centralized secret management
resource "azurerm_key_vault" "vault" {
  name                = "${var.project_name}-kv-unique" 
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  # Access Policy for the deploying user
  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    secret_permissions = ["Get", "List", "Set", "Delete", "Purge"]
  }
}

# Store the Storage Account Key as a secret
resource "azurerm_key_vault_secret" "storage_key" {
  name         = "storage-access-key"
  value        = azurerm_storage_account.storage.primary_access_key
  key_vault_id = azurerm_key_vault.vault.id
}

# =============================================================================
# 3. ANALYTICS PLATFORM (DATABRICKS)
# =============================================================================

# Databricks Workspace
resource "azurerm_databricks_workspace" "databricks" {
  name                = "${var.project_name}-databricks"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "premium" # Required for Unity Catalog and Secret Scopes
}

# Databricks Secret Scope: Link Databricks to Azure Key Vault
resource "databricks_secret_scope" "kv_scope" {
  name = "azure-key-vault"

  keyvault_metadata {
    resource_id = azurerm_key_vault.vault.id
    dns_name    = azurerm_key_vault.vault.vault_uri
  }
}

# Single Node Cluster for pipeline execution
resource "databricks_cluster" "melpark_cluster" {
  cluster_name            = "melpark-pipeline-cluster"
  spark_version           = "14.3.x-scala2.12" 
  node_type_id            = data.databricks_node_type.smallest.id
  autotermination_minutes = 20 

  spark_conf = {
    "spark.databricks.cluster.profile" : "singleNode"
    "spark.master" : "local[*]"
  }

  custom_tags = {
    "ResourceClass" = "SingleNode"
  }
}