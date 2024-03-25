import logging
from io import BytesIO

from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

from airflow_tools.filesystems.filesystem_protocol import FilesystemProtocol

logger = logging.getLogger(__file__)


class BlobStorageFilesystem(FilesystemProtocol):
    def __init__(self, hook: WasbHook):
        self.hook = hook

    def read(self, path: str) -> bytes:
        container_name, blob_name = _get_container_and_blob_name(path)
        stream = self.hook.download(container_name=container_name, blob_name=blob_name)
        return stream.readall().encode()

    def write(self, data: str | bytes | BytesIO, path: str):
        container_name, blob_name = _get_container_and_blob_name(path)
        if isinstance(data, str):
            self.hook.load_string(data, container_name, blob_name)
            return

        if isinstance(data, BytesIO):
            data = data.getvalue()
        logger.info(
            f'Writing to wasb container "{container_name}" and blob "{blob_name}"'
        )
        self.hook.upload(container_name=container_name, blob_name=blob_name, data=data)

    def delete_file(self, path: str):
        container_name, blob_name = _get_container_and_blob_name(path)
        self.hook.delete_file(
            container_name, blob_name, is_prefix=False, ignore_if_missing=True
        )

    def create_prefix(self, prefix: str):
        container_name, blob_name = _get_container_and_blob_name(f'{prefix}/')
        self.hook.upload(container_name, blob_name, data=b"")

    def delete_prefix(self, prefix: str):
        container_name, blob_prefix = _get_container_and_blob_name(prefix)
        self.hook.delete_file(
            container_name, blob_prefix, is_prefix=True, ignore_if_missing=True
        )

    def check_file(self, path: str) -> bool:
        container_name, blob_name = _get_container_and_blob_name(path)
        return self.hook.check_for_blob(container_name, blob_name)

    def check_prefix(self, prefix: str) -> bool:
        container_name, blob_prefix = _get_container_and_blob_name(prefix)
        return self.hook.check_for_prefix(container_name, blob_prefix)

    def list_files(self, prefix: str) -> list[str]:
        container_name, blob_prefix = _get_container_and_blob_name(prefix)
        return self.hook.get_blobs_list(container_name, blob_prefix)


def _get_container_and_blob_name(path: str) -> tuple[str, str]:
    parts = path.split("/")
    return parts[0], "/".join(parts[1:])
