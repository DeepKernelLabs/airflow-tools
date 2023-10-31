import logging
import time
from typing import Any

from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.context import Context
from jinja2 import Template

from airflow_tools.data_lake_facade import DataLakeFacade

logger = logging.getLogger(__name__)


class SkipOrReplacePartition(BaseOperator):
    """If a __SUCCESS__ file exists in the partition it will skip it unless the DAG is triggered with the parameter replace=true"""

    template_fields = ('data_lake_partition_path',)

    def __init__(
        self,
        data_lake_conn_id: str,
        data_warehouse_conn_id: str,
        data_lake_partition_path: str,
        data_warehouse_schema: str,
        data_warehouse_table: str,
        partition_id_field: str = 'ds',
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.data_lake_conn_id = data_lake_conn_id
        self.data_warehouse_conn_id = data_warehouse_conn_id
        self.data_lake_partition_path = data_lake_partition_path
        self.data_warehouse_schema = data_warehouse_schema
        self.data_warehouse_table = data_warehouse_table
        self.partition_id_field = partition_id_field

    def execute(self, context: Context) -> Any:
        replace: bool = context['params'].get('replace', False)
        data_lake_conn = BaseHook.get_connection(self.data_lake_conn_id)
        data_lake_facade = DataLakeFacade(
            conn=data_lake_conn.get_hook(),
        )

        success_exists = data_lake_facade.exists(
            f'{self.data_lake_partition_path.rstrip("/")}/__SUCCESS__'
        )
        if not success_exists:
            logger.info('Success file not found. Replacing partition if exists...')
            replace = True

        if replace:
            data_lake_facade.delete(self.data_lake_partition_path)
            self._delete_partition_snowflake_if_exists(context)
        else:
            assert success_exists
            raise AirflowSkipException('Partition already exists in Data Lake')

    def _delete_partition_snowflake_if_exists(self, context: Context):
        params = {
            'table': self.data_warehouse_table,
            'partition_id_field': self.partition_id_field,
        }
        sql = Template(
            'DELETE FROM {{ table }} WHERE {{ partition_id_field }} = {{ ds }}'
        ).render({**context, **params})
        SQLExecuteQueryOperator(
            task_id=f'delete_partition_warehouse_{time.time()}',
            conn_id=self.data_warehouse_conn_id,
            hook_params={
                'schema': self.data_warehouse_schema,
            },
            sql=sql,
        ).execute(context)
