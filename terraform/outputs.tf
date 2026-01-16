output "resource_group_name" {
  value       = azurerm_resource_group.rg.name
  description = "The name of the created Resource Group"
}

output "storage_account_name" {
  value       = azurerm_storage_account.storage.name
  description = "The name of the Data Lake Storage account"
}

output "databricks_workspace_url" {
  value       = azurerm_databricks_workspace.databricks.workspace_url
  description = "The URL for the Databricks workspace"
}

output "key_vault_uri" {
  value       = azurerm_key_vault.vault.vault_uri
  description = "The URI of the Key Vault for secret management"
}