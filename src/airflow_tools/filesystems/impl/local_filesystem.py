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

    def delete_file(self, path: str):
        path_to_delete = Path(self.hook.get_path()) / path.lstrip('/')
        path_to_delete.unlink()

    def create_prefix(self, prefix: str):
        path_to_create = Path(self.hook.get_path()) / prefix.lstrip('/')
        path_to_create.mkdir(parents=True, exist_ok=True)

    def delete_prefix(self, prefix: str):
        path_to_delete = Path(self.hook.get_path()) / prefix.lstrip('/')
        shutil.rmtree(str(path_to_delete))

    def check_file(self, path: str) -> bool:
        path_to_check = Path(self.hook.get_path()) / path.lstrip('/')
        return path_to_check.exists() and path_to_check.is_file()

    def check_prefix(self, prefix: str) -> bool:
        path_to_check = Path(self.hook.get_path()) / prefix.lstrip('/')
        return path_to_check.exists() and path_to_check.is_dir()

    def list_files(self, prefix: str) -> list[str]:
        path_to_list = Path(self.hook.get_path()) / prefix.lstrip('/')
        return [str(file) for file in path_to_list.glob("*") if file.is_file()]
