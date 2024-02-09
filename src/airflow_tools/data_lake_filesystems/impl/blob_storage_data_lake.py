import logging
from io import BytesIO

from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

from airflow_tools.data_lake_filesystems.data_lake_protocol import DataLakeProtocol

logger = logging.getLogger(__file__)


class BlobStorageDataLake(DataLakeProtocol):
    def __init__(self, hook: WasbHook):
        self.hook = hook

    def write(self, data: str | bytes | BytesIO, path: str):
        container_name, blob_name = _get_container_and_blob_name(path)
        if isinstance(data, str):
            self.conn.load_string(data, container_name, blob_name)
            return

        if isinstance(data, BytesIO):
            data = data.getvalue()
        logger.info(
            f'Writing to wasb container "{container_name}" and blob "{blob_name}"'
        )
        self.conn.upload(container_name=container_name, blob_name=blob_name, data=data)

    def delete_prefix(self, prefix: str):
        container_name, blob_prefix = _get_container_and_blob_name(prefix)
        self.conn.delete_file(
            container_name, blob_prefix, is_prefix=True, ignore_if_missing=True
        )


def _get_container_and_blob_name(path: str) -> tuple[str, str]:
    parts = path.split("/")
    return parts[0], "/".join(parts[1:])
