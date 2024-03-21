import logging
from io import BytesIO

from azure.storage.fileshare._models import DirectoryProperties

from airflow_tools.filesystems.filesystem_protocol import FilesystemProtocol
from airflow_tools.providers.azure.hooks.azure_file_share import (
    AzureFileShareServicePrincipalHook,
)

logger = logging.getLogger(__file__)


class AzureFileShareFilesystem(FilesystemProtocol):
    def __init__(self, hook: AzureFileShareServicePrincipalHook):
        self.hook = hook

    def read(self, path: str) -> bytes:
        return self.hook.get_conn().get_file_client(path).download_file().readall()

    def write(self, data: str | bytes | BytesIO, path: str):
        if isinstance(data, str):
            data = data.encode()
        elif isinstance(data, BytesIO):
            data = data.getvalue()
        self.hook.get_conn().get_file_client(path).upload_file(data)

    def delete_prefix(self, prefix: str):
        conn = self.hook.get_conn()
        for item in conn.list_directories_and_files(prefix):
            if isinstance(item, DirectoryProperties):
                self.delete_prefix(f'{prefix}/{item.name}')
            else:
                conn.get_file_client(f'{prefix}/{item.name}').delete_file()
        conn.get_directory_client(prefix).delete_directory()

    def list_files(self, prefix: str) -> list[str]:
        conn = self.hook.get_conn()
        return [
            f'{prefix}/{item.name}'
            for item in conn.list_directories_and_files(prefix)
            if not item.is_directory
        ]
        
    def check_prefix(self, prefix: str) -> bool:
        conn = self.hook.get_conn()
        return bool(conn.list_directories_and_files(prefix))
