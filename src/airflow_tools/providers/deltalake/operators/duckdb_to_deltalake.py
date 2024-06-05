import sys
import logging

from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook

from deltalake import write_deltalake
from duckdb_provider.hooks.duckdb_hook import DuckDBHook

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

logger = logging.getLogger(__name__)


class DuckdbToDeltalakeOperator(BaseOperator):
    template_fields = ('source_query', 'table_path')

    def __init__(
        self,
        duckdb_conn_id: str,
        delta_lake_conn_id: str,
        source_query: str,
        table_path: str,
        extensions: list[str] = None,
        rows_per_batch: int = 1000000,
        write_mode: Literal["error", "append", "overwrite", "ignore"] = "append",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.duckdb_conn_id = duckdb_conn_id
        self.delta_lake_conn_id = delta_lake_conn_id
        self.source_query = source_query
        self.table_path = table_path
        self.extensions = extensions
        self.rows_per_batch = rows_per_batch
        self.write_mode = write_mode

    def execute(self, context):
        logger.info("Create conn for duckdb")
        duckdb_conn = DuckDBHook.get_hook(self.duckdb_conn_id).get_conn()

        if self.extensions:
            logger.info("Install and load extensions")
            for extension in self.extensions:
                duckdb_conn.install_extension(extension)
                duckdb_conn.load_extension(extension)

        logger.info("Create conn for delta lake and populate storage_options")
        delta_lake_conn = BaseHook.get_connection(self.delta_lake_conn_id)
        connection_string = delta_lake_conn.extra_dejson.get("connection_string")

        logger.info("Create secret for duckdb")
        duckdb_conn.execute(f"""
            CREATE OR REPLACE SECRET delta_lake_secret (
                TYPE AZURE,
                CONNECTION_STRING '{connection_string}'
            );""")

        logger.info("Write data to delta lake")
        write_deltalake(self.table_path,
                        duckdb_conn.execute(self.source_query).fetch_record_batch(rows_per_batch=self.rows_per_batch),
                        storage_options=self.parse_storage_options(connection_string),
                        mode=self.write_mode)

    @staticmethod
    def parse_storage_options(connection_string: str) -> dict[str, str]:
        storage_options = {}
        for part in connection_string.split(';'):
            if part.startswith('AccountName='):
                storage_options['account_name'] = part.replace('AccountName=', '').strip()
            if part.startswith('AccountKey='):
                storage_options['account_key'] = part.replace('AccountKey=', '').strip()
        return storage_options
