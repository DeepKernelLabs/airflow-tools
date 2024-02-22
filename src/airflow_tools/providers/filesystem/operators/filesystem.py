from typing import List

from airflow.models import BaseOperator


class FilesystemToFilesystem(BaseOperator):
    """
    Copies a file from a filesystem to another filesystem.
    """

    template_fields = ('file_paths', 'destination_path')

    def __init__(
        self,
        source_fs_hook: str,
        destination_fs_hook: str,
        file_paths: List[str],
        destination_path: str,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.source_fs_hook = source_fs_hook
        self.destination_fs_hook = destination_fs_hook
        self.file_paths = file_paths
        self.destination_path = destination_path

    def execute(self, context):
        for file_path in self.file_paths:
            data = self.source_fs_hook.read(file_path)

            file_name = file_path.split('/')[-1]
            self.destination_fs_hook.write(data, self.destination_path + file_name)
