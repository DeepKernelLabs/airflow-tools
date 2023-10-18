import logging
from io import BytesIO

from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

DataLakeConnection = WasbHook
logger = logging.getLogger(__file__)


class DataLakeFacade:
    """Provides a consistent interface over different Data Lakes (S3/Blob Storage etc.)"""

    def __init__(self, conn: DataLakeConnection):
        self.conn = conn

    def write(self, data: str | bytes | BytesIO, path: str):
        match self.conn.conn_type:
            case "wasb":
                container_name, blob_name = _get_container_and_blob_name(path)
                if isinstance(data, str):
                    self.conn.load_string(data, container_name, blob_name)
                    return

                if isinstance(data, bytes):
                    data = BytesIO(data)
                logger.info(
                    f'Writing to wasb container "{container_name}" and blob "{blob_name}"'
                )
                self.conn.load_file(data, container_name, blob_name)
            case _:
                raise NotImplementedError(
                    f"Data Lake type {self.conn.conn_type} does not support write"
                )


def _get_container_and_blob_name(path: str) -> tuple[str, str]:
    parts = path.split("/")
    return parts[0], "/".join(parts[1:])
