from typing import Optional, Protocol

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.common.sql.hooks.sql import DbApiHook

from airflow_tools.filesystems.filesystem_factory import FilesystemFactory


class Transformation(Protocol):
    def __call__(self, data: bytes, filename: str, context: dict) -> bytes:
        ...


class FilesystemToFilesystem(BaseOperator):
    """
    Copies a file from a filesystem to another filesystem.
    """

    template_fields = ('source_file_path', 'destination_path')

    def __init__(
        self,
        source_fs_conn_id: str,
        destination_fs_conn_id: str,
        source_path: str,
        destination_path: str,
        data_transformation: Optional[Transformation] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.source_fs_conn_id = source_fs_conn_id
        self.destination_fs_conn_id = destination_fs_conn_id
        self.source_path = source_path
        self.destination_path = destination_path
        self.data_transformation = data_transformation

    def execute(self, context):
        source_fs_hook = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(self.source_fs_conn_id),
        )
        destination_fs_hook = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(self.destination_fs_conn_id),
        )

        for file_path in source_fs_hook.list_files(self.source_path):
            file_name = file_path.split('/')[-1]
            data = source_fs_hook.read(file_path)
            if self.data_transformation:
                data = self.data_transformation(data, file_name, context)
            full_destination_path = (
                self.destination_path + file_name
                if self.destination_path.endswith('/')
                else self.destination_path
            )
            destination_fs_hook.write(data, full_destination_path)


class SQLToFilesystem(BaseOperator):
    """
    Copies data from a SQL query to a filesystem.
    """

    template_fields = (
        'sql',
        'destination_path',
    )

    def __init__(
        self,
        source_sql_conn_id: str,
        destination_fs_conn_id: str,
        sql: str,
        destination_path: str,
        batch_size: Optional[int] = 100000,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.source_sql_conn_id = source_sql_conn_id
        self.destination_fs_conn_id = destination_fs_conn_id
        self.sql = sql
        self.destination_path = destination_path
        self.batch_size = batch_size
        self.files = []

    def execute(self, context):
        source_sql_hook: DbApiHook = BaseHook.get_connection(
            self.source_sql_conn_id
        ).get_hook()

        destination_fs_hook = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(self.destination_fs_conn_id),
        )

        self.files.clear()
        for i, df in enumerate(
            source_sql_hook.get_pandas_df_by_chunks(
                sql=self.sql, chunksize=self.batch_size
            ),
            start=1,
        ):
            full_file_path = f"{self.destination_path.rstrip('/')}/part{i:04}.parquet"
            destination_fs_hook.write(
                df.to_parquet(index=False, engine='pyarrow'), full_file_path
            )
            self.files.append(full_file_path)
