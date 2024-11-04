import io
import logging
import sys
import typing

import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from sqlalchemy import create_engine

from airflow_tools.filesystems.filesystem_factory import FilesystemFactory

if sys.version_info < (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

logger = logging.getLogger(__name__)


class FilesystemToDatabaseOperator(BaseOperator):
    """
    This operator will copy a file from a filesystem to a database table.
    """

    template_fields = (
        'db_table',
        'db_schema',
        'filesystem_conn_id',
        'database_conn_id',
        'filesystem_path',
        'metadata',
    )

    def __init__(
        self,
        filesystem_conn_id: str,
        database_conn_id: str,
        filesystem_path: str,
        db_table: str,
        db_schema: typing.Optional[str] = None,
        source_format: typing.Optional[Literal['csv', 'json', 'parquet']] = 'csv',
        source_format_options: typing.Optional[typing.Dict] = None,
        table_aggregation_type: typing.Optional[
            Literal['append', 'fail', 'replace']
        ] = 'append',
        metadata: typing.Optional[typing.Dict[str, str]] = None,
        *args,
        **kwargs,
    ) -> None:

        super().__init__(*args, **kwargs)

        self.filesystem_conn_id = filesystem_conn_id
        self.database_conn_id = database_conn_id
        self.filesystem_path = filesystem_path
        self.db_table = db_table
        self.db_schema = db_schema
        self.source_format = source_format
        self.source_format_options = source_format_options
        self.table_aggregation_type = table_aggregation_type
        self.metadata = metadata or {'_DS': '{{ ds }}'}

    def execute(self, context):
        logger.info('Create conn for filesystem')
        filesystem = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(self.filesystem_conn_id),
        )

        engine = create_engine(
            BaseHook.get_connection(self.database_conn_id).get_hook().get_uri()
        )

        for blob in filesystem.list_files(prefix=self.filesystem_path):

            if not blob.endswith(
                (f'.{self.source_format}', f'.{self.source_format}.gz')
            ):
                logger.warning(f'Blob {blob} is not in the right format. Skipping...')
                continue

            logger.info(f'Read file {blob} and convert to pandas')
            raw_content = io.BytesIO(filesystem.read(blob))

            df = self.raw_content_to_pandas(path_or_buf=raw_content)

            for key, value in self.metadata.items():
                df[key] = value
            df['_LOADED_FROM'] = blob

            df.to_sql(
                name=self.db_table,
                schema=self.db_schema,
                con=engine,
                if_exists=self.table_aggregation_type,
                index=False,
            )

    def raw_content_to_pandas(self, path_or_buf: typing.Union[str, bytes, io.StringIO]):
        options = self.source_format_options or {}

        match self.source_format:
            case 'csv':
                return pd.read_csv(path_or_buf, **options)
            case 'json':
                return pd.read_json(path_or_buf, **options)
            case 'parquet':
                return pd.read_parquet(path_or_buf, **options)
            case _:
                raise ValueError(f'Unknown source format {self.source_format}')
