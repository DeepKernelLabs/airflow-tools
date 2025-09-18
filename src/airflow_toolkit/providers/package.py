from typing import Any, Dict

from airflow_toolkit._version import __version__


def get_provider_info() -> Dict[str, Any]:
    """Return provider metadata to Airflow"""
    return {
        "package-name": "airflow-toolkit",
        "name": "DKL Airflow Toolkit",
        "description": "Apache Airflow Providers containing Operators, Sensors and toolkit for ELT",
        "versions": __version__,
        # Optional.
        "connection-types": [
            {
                "connection-type": "azure_databricks_sql",
                "hook-class-name": "airflow_toolkit.providers.azure.hooks.azure_databricks.AzureDatabricksSqlHook",
            },
            {
                "connection-type": "azure_databricks_volume",
                "hook-class-name": "airflow_toolkit.providers.azure.hooks.azure_databricks.AzureDatabricksVolumeHook",
            },
            {
                "connection-type": "azure_file_share_sp",
                "hook-class-name": "airflow_toolkit.providers.azure.hooks.azure_file_share.AzureFileShareServicePrincipalHook",
            },
        ],
        "extra-links": [],
    }
