import logging
from io import BytesIO

from airflow_tools.filesystems.filesystem_protocol import FilesystemProtocol
from airflow_tools.providers.azure.hooks.azure_databricks import (
    AzureDatabricksVolumeHook,
)

logger = logging.getLogger(__file__)


class AzureDatabricksVolumeFilesystem(FilesystemProtocol):
    def __init__(self, hook: AzureDatabricksVolumeHook):
        self.hook = hook

    def read(self, path: str) -> bytes:
        return self.hook.get_conn().files.download(path).contents

    def write(self, data: str | bytes | BytesIO, path: str):
        if isinstance(data, str):
            data = data.encode()
        elif isinstance(data, BytesIO):
            data = data.getvalue()
        self.hook.get_conn().files.upload(path, data)

    def delete_prefix(self, prefix: str):
        conn = self.hook.get_conn()
        for entry in conn.list_directory_contents(prefix):
            if entry.is_directory:
                self.delete_prefix(entry.path)
            else:
                conn.files.delete(entry.path)
        conn.files.delete_directory(prefix)

    def list_files(self, prefix: str) -> list[str]:
        conn = self.hook.get_conn()
        return [
            entry.path
            for entry in conn.list_directory_contents(prefix)
            if not entry.is_directory
        ]
