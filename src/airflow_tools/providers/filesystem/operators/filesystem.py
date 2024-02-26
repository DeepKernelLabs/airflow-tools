from typing import Optional, Protocol

from airflow.models import BaseOperator


class Transformation(Protocol):
    def __call__(self, data: bytes, filename: str, context: dict) -> bytes:
        ...


class FilesystemToFilesystem(BaseOperator):
    """
    Copies a file from a filesystem to another filesystem.
    """

    template_fields = ('file_paths', 'destination_path')

    def __init__(
        self,
        source_fs_hook: str,
        destination_fs_hook: str,
        source_file_path: str,
        destination_path: str,
        data_transformation: Optional[Transformation] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.source_fs_hook = source_fs_hook
        self.destination_fs_hook = destination_fs_hook
        self.source_file_path = source_file_path
        self.destination_path = destination_path
        self.data_transformation = data_transformation

    def execute(self, context):
        file_name = self.source_file_path.split('/')[-1]
        data = self.source_fs_hook.read(self.source_file_path)
        if self.data_transformation:
            data = self.data_transformation(data, file_name, context)
        full_destination_path = (
            self.destination_path + file_name
            if self.destination_path.endswith('/')
            else self.destination_path
        )
        self.destination_fs_hook.write(data, full_destination_path)
