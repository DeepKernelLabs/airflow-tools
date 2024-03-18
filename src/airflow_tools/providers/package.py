from typing import Any, Dict

from airflow_tools._version import __version__


def get_provider_info() -> Dict[str, Any]:
    """Return provider metadata to Airflow"""
    return {
        "package-name": "airflow-tools",
        "name": "DKL Airflow Tools",
        "description": "Apache Airflow Providers containing Operators, Sensors and tools for ELT",
        "versions": __version__,
        # Optional.
        "connection-types": [
            {
                "connection-type": "azure_databricks_sql",
                "hook-class-name": "airflow_tools.providers.azure.hooks.azure_databricks.AzureDatabricksSqlHook",
            },
            {
                "connection-type": "azure_databricks_volume",
                "hook-class-name": "airflow_tools.providers.azure.hooks.azure_databricks.AzureDatabricksVolumeHook",
            },
            {
                "connection-type": "azure_file_share_sp",
                "hook-class-name": "airflow_tools.providers.azure.hooks.azure_file_share.AzureFileShareServicePrincipalHook",
            },
        ],
        "extra-links": [],
    }
