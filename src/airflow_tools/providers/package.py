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
        "connection-types": [],
        "extra-links": [],
    }
