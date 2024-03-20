import shutil
from io import BytesIO
from pathlib import Path

from airflow.hooks.filesystem import FSHook

from airflow_tools.filesystems.filesystem_protocol import FilesystemProtocol


class LocalFilesystem(FilesystemProtocol):
    def __init__(self, hook: FSHook):
        self.hook = hook

    def read(self, path: str) -> bytes:
        return (Path(self.hook.get_path()) / path).read_bytes()

    def write(self, data: str | bytes | BytesIO, path: str):
        if isinstance(data, str):
            data = data.encode()
        elif isinstance(data, BytesIO):
            data = data.getvalue()
        (Path(self.hook.get_path()) / path).write_bytes(data)

    def delete_prefix(self, prefix: str):
        path_to_delete = Path(self.hook.get_path()) / prefix
        shutil.rmtree(str(path_to_delete))

    def list_files(self, prefix: str) -> list[str]:
        path_to_list = Path(self.hook.get_path()) / prefix
        return [str(file) for file in path_to_list.glob("*") if file.is_file()]
