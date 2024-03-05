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
        source_file_path: str,
        destination_path: str,
        data_transformation: Optional[Transformation] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.source_fs_conn_id = source_fs_conn_id
        self.destination_fs_conn_id = destination_fs_conn_id
        self.source_file_path = source_file_path
        self.destination_path = destination_path
        self.data_transformation = data_transformation

    def execute(self, context):
        source_fs_hook = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(self.source_fs_conn_id),
        )
        destination_fs_hook = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(self.destination_fs_conn_id),
        )

        file_name = self.source_file_path.split('/')[-1]
        data = source_fs_hook.read(self.source_file_path)
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
        'source_name',
        'source_schema_name',
        'source_table_name',
        'destination_path',
    )

    def __init__(
        self,
        source_sql_conn_id: str,
        destination_fs_conn_id: str,
        source_name: str,
        source_schema_name: str,
        source_table_name: str,
        destination_path: str,
        batch_size: Optional[int] = 100000,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.source_sql_conn_id = source_sql_conn_id
        self.destination_fs_conn_id = destination_fs_conn_id
        self.source_name = source_name
        self.source_schema_name = source_schema_name
        self.source_table_name = source_table_name
        self.destination_path = destination_path
        self.batch_size = batch_size

    def execute(self, context):
        source_sql_hook: DbApiHook = BaseHook.get_connection(
            self.source_sql_conn_id
        ).get_hook()
        source_sql_hook.__schema__ = self.source_schema_name

        destination_fs_hook = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(self.destination_fs_conn_id),
        )

        query = f'SELECT * FROM {self.source_table_name};'
        for i, df in enumerate(
            source_sql_hook.get_pandas_df_by_chunks(
                sql=query, chunksize=self.batch_size
            ),
            start=1,
        ):
            generated_path = f"{self.source_name.lower()}/{self.source_table_name.lower()}/part{i:04}.parquet"
            full_file_path = f"{self.destination_path.rstrip('/')}/{generated_path}"
            destination_fs_hook.write(df.to_parquet(index=False), full_file_path)
