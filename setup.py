import setuptools

setuptools.setup(
    name="melpark-pipeline",
    version="0.1.0",
    description="ETL pipeline for Melbourne Parking Data on Azure Databricks",
    author="krokadil",
    # Packages to include (automatically finds folders with __init__.py)
    packages=setuptools.find_packages(),
    # Core libraries required to run your project
    install_requires=[
        "dbt-databricks>=1.8.0",  # Optimized dbt adapter for Databricks
        "azure-storage-blob>=12.0.0",  # Required for raw storage access
        "azure-identity",  # Needed for secure Vault/Storage auth
        "pyspark==3.5.0",  # Matches your Databricks cluster version
        "pandas>=2.0.0",  # Useful for local testing and Python models
    ],
    python_requires=">=3.8",
)
