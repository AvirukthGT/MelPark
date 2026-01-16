.PHONY: terraform-init terraform-apply dbt-deps dbt-debug dbt-run dbt-test dbt-full-refresh clean help

# Variables
DBT_PROFILE_DIR := .
PROJECT_NAME := melpark

# --- Terraform Commands ---
# Initialize Terraform and download providers
terraform-init:
	cd terraform && terraform init

# Deploy your Azure infrastructure
terraform-apply:
	cd terraform && terraform apply -auto-approve

# Destroy the infrastructure (Use with caution!)
terraform-destroy:
	cd terraform && terraform destroy -auto-approve

# --- dbt Transformation Commands ---
# Install dbt packages (like dbt_utils)
dbt-deps:
	dbt deps

# Test the connection to Databricks
dbt-debug:
	dbt debug --profiles-dir $(DBT_PROFILE_DIR)

# Run all models to build the Gold layer
dbt-run:
	dbt run --profiles-dir $(DBT_PROFILE_DIR)

# Run data quality tests
dbt-test:
	dbt test --profiles-dir $(DBT_PROFILE_DIR)

# Rebuild everything from scratch (drops and recreates tables)
dbt-full-refresh:
	dbt run --full-refresh --profiles-dir $(DBT_PROFILE_DIR)

# --- Utility Commands ---
# Clean up temporary Python and dbt artifacts
clean:
	rm -rf target/
	rm -rf dbt_packages/
	find . -type d -name "__pycache__" -exec rm -rf {} +

# Display available commands
help:
	@echo "Available commands:"
	@echo "  make terraform-init    - Initialize Terraform"
	@echo "  make terraform-apply   - Deploy Azure resources"
	@echo "  make dbt-run           - Run dbt transformations"
	@echo "  make dbt-test          - Run data quality tests"
	@echo "  make clean             - Remove build artifacts"