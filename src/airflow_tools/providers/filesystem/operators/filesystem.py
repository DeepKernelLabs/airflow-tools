from io import BytesIO
from typing import TYPE_CHECKING, Optional, Protocol

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.common.sql.hooks.sql import DbApiHook

from airflow_tools.filesystems.filesystem_factory import FilesystemFactory

if TYPE_CHECKING:
    from airflow.utils.context import Context

import logging

logger = logging.getLogger(__file__)


class Transformation(Protocol):
    def __call__(self, data: bytes, filename: str, context: dict) -> bytes:
        ...


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
        batch_size: int | None = None,
        create_file_on_success: str | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.source_sql_conn_id = source_sql_conn_id
        self.destination_fs_conn_id = destination_fs_conn_id
        self.sql = sql
        self.destination_path = destination_path
        self.batch_size = batch_size
        self.create_file_on_success = create_file_on_success
        self.files = []

    def execute(self, context):
        source_sql_hook: DbApiHook = BaseHook.get_connection(
            self.source_sql_conn_id
        ).get_hook()

        destination_fs_hook = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(self.destination_fs_conn_id),
        )

        self.files.clear()
        dfs = (
            [source_sql_hook.get_pandas_df(sql=self.sql)]
            if not self.batch_size
            else source_sql_hook.get_pandas_df_by_chunks(
                sql=self.sql, chunksize=self.batch_size
            )
        )
        for i, df in enumerate(dfs, start=1):
            full_file_path = f"{self.destination_path.rstrip('/')}/part{i:04}.parquet"
            destination_fs_hook.write(df.to_parquet(index=False), full_file_path)
            self.files.append(full_file_path)

        if self.create_file_on_success is not None and isinstance(self.create_file_on_success, str):
            success_file_path = f"{self.destination_path.rstrip('/')}/{self.create_file_on_success}"
            destination_fs_hook.write(BytesIO(), success_file_path)


class FilesystemToFilesystem(BaseOperator):
    """
    Copies a file from a filesystem to another filesystem.
    """

    template_fields = ('source_path', 'destination_path')

    def __init__(
        self,
        source_fs_conn_id: str,
        destination_fs_conn_id: str,
        source_path: str,
        destination_path: str,
        data_transformation: Optional[Transformation] = None,
        create_file_on_success: str | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.source_fs_conn_id = source_fs_conn_id
        self.destination_fs_conn_id = destination_fs_conn_id
        self.source_path = source_path
        self.destination_path = destination_path
        self.data_transformation = data_transformation
        self.create_file_on_success = create_file_on_success

    def execute(self, context):
        source_fs_hook = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(self.source_fs_conn_id),
        )
        destination_fs_hook = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(self.destination_fs_conn_id),
        )

        files = (
            source_fs_hook.list_files(self.source_path)
            if source_fs_hook.check_prefix(self.source_path)
            else [self.source_path]
        )
        for file_path in files:
            logger.info(f'Trying to copy {file_path} to {self.destination_path}')
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

        if self.create_file_on_success is not None and isinstance(self.create_file_on_success, str):
            success_file_path = '/'.join(self.destination_path.split('/')[:-1]) + '/' + self.create_file_on_success
            destination_fs_hook.write(BytesIO(), success_file_path)


class FilesystemDeleteOperator(BaseOperator):
    template_fields = ('filesystem_path',)

    def __init__(self, filesystem_conn_id: str, filesystem_path: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filesystem_conn_id = filesystem_conn_id
        self.filesystem_path = filesystem_path

    def execute(self, context: 'Context'):
        filesystem_protocol = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(self.filesystem_conn_id),
        )
        logger.info(f'Trying to delete: {self.filesystem_path}')
        filesystem_protocol.delete_prefix(self.filesystem_path)


class FilesystemCheckOperator(BaseOperator):
    template_fields = ('filesystem_path',)

    def __init__(
        self,
        filesystem_conn_id: str,
        filesystem_path: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.filesystem_conn_id = filesystem_conn_id
        self.filesystem_path = filesystem_path

    def execute(self, context: 'Context'):
        filesystem_protocol = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(self.filesystem_conn_id),
        )

        logger.info(f'Trying to check: {self.filesystem_path}')
        return (
            filesystem_protocol.check_prefix(self.filesystem_path)
            if self.filesystem_path.endswith('/')
            else filesystem_protocol.check_file(self.filesystem_path)
        )
