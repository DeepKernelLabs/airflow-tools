from typing import TYPE_CHECKING

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator

from airflow_tools.data_lake_facade import DataLakeFacade

if TYPE_CHECKING:
    from airflow.utils.context import Context

import logging

logger = logging.getLogger(__file__)


class DataLakeDeleteOperator(BaseOperator):
    template_fields = ('data_lake_path',)

    def __init__(self, data_lake_conn_id: str, data_lake_path: str, *args, **kwargs):
        logger.warning('DataLakeDeleteOperator is deprecated. Use FilesystemDeleteOperator instead.')
        super().__init__(*args, **kwargs)
        self.data_lake_conn_id = data_lake_conn_id
        self.data_lake_path = data_lake_path

    def execute(self, context: 'Context'):
        data_lake_conn = BaseHook.get_connection(self.data_lake_conn_id)
        data_lake_facade = DataLakeFacade(
            conn=data_lake_conn.get_hook(),
        )
        data_lake_facade.delete_prefix(self.data_lake_path)


class DataLakeCheckOperator(BaseOperator):
    template_fields = ('data_lake_path',)

    def __init__(
        self,
        data_lake_conn_id: str,
        data_lake_path: str,
        check_specific_filename: None | str = None,
        *args,
        **kwargs,
    ):
        logger.warning('DataLakeCheckOperator is deprecated. Use FilesytemCheckOperator instead.')
        super().__init__(*args, **kwargs)
        self.data_lake_conn_id = data_lake_conn_id
        self.data_lake_path = data_lake_path
        self.check_specific_filename = check_specific_filename

    def execute(self, context: 'Context'):
        data_lake_conn = BaseHook.get_connection(self.data_lake_conn_id)
        data_lake_facade = DataLakeFacade(
            conn=data_lake_conn.get_hook(),
        )
        logger.info(f'Checking {self.data_lake_path}')
        prefix_flag = data_lake_facade.check_prefix(self.data_lake_path)
        logger.info(f'Prefix flag: {prefix_flag}')
        if self.check_specific_filename:
            logger.info(f'Checking {self.check_specific_filename}')
            specific_file_flag = data_lake_facade.check_prefix(
                f'{self.data_lake_path}{self.check_specific_filename}'
            )
            logger.info(f'Specific file flag: {specific_file_flag}')
            return prefix_flag and specific_file_flag
        return prefix_flag
