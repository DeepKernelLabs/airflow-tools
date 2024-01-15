from typing import TYPE_CHECKING

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator

from airflow_tools.data_lake_facade import DataLakeFacade

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DataLakeDeleteOperator(BaseOperator):
    template_fields = ('data_lake_path',)

    def __init__(self, data_lake_conn_id: str, data_lake_path: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_lake_conn_id = data_lake_conn_id
        self.data_lake_path = data_lake_path

    def execute(self, context: 'Context'):
        data_lake_conn = BaseHook.get_connection(self.data_lake_conn_id)
        data_lake_facade = DataLakeFacade(
            conn=data_lake_conn.get_hook(),
        )
        data_lake_facade.delete_prefix(self.data_lake_path)
