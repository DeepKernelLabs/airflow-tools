from io import BytesIO
from typing import TYPE_CHECKING, Any, Callable, Literal

import jmespath
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

from airflow_tools.data_lake_facade import DataLakeFacade

if TYPE_CHECKING:
    from airflow.utils.context import Context
    from pandas._typing import CompressionOptions
    from requests.auth import AuthBase

SaveFormat = Literal['jsonl']


class HttpToDataLake(BaseOperator):
    conn_type = 'http_to_data_lake'
    template_fields = SimpleHttpOperator.template_fields + ('data_lake_path',)
    template_fields_renderers = SimpleHttpOperator.template_fields_renderers

    def __init__(
        self,
        http_conn_id: str,
        data_lake_conn_id: str,
        data_lake_path: str,
        save_format: SaveFormat = 'jsonl',
        compression: 'CompressionOptions' = None,
        endpoint: str | None = None,
        method: str = "POST",
        data: Any = None,
        headers: dict[str, str] | None = None,
        auth_type: type['AuthBase'] | None = None,
        jmespath_expression: str | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.data_lake_conn_id = data_lake_conn_id
        self.data_lake_path = data_lake_path
        self.save_format = save_format
        self.compression = compression
        self.endpoint = endpoint
        self.method = method
        self.data = data
        self.headers = headers
        self.auth_type = auth_type
        self.jmespath_expression = jmespath_expression

    def execute(self, context: 'Context') -> Any:
        data = SimpleHttpOperator(
            task_id='simple-http-operator',
            http_conn_id=self.http_conn_id,
            endpoint=self.endpoint,
            method=self.method,
            data=self.data,
            headers=self.headers,
            auth_type=self.auth_type,
            response_filter=self._response_filter,
        ).execute(context)

        data_lake_conn = BaseHook.get_connection(self.data_lake_conn_id)
        data_lake_facade = DataLakeFacade(
            conn=data_lake_conn.get_hook(),
        )

        file_path = self.data_lake_path.rstrip('/') + '/' + self._file_name()
        data_lake_facade.write(data, file_path)

    def _file_name(self) -> str:
        file_name = f'part0001.{self.save_format}'
        if self.compression:
            file_name += f'.{self.compression}'
        return file_name

    def _response_filter(self, response) -> Callable | None:
        if self.save_format == 'jsonl' and not self.jmespath_expression:
            return list_to_jsonl(response.json(), self.compression)
        elif self.save_format == 'jsonl' and self.jmespath_expression:
            return list_to_jsonl(
                jmespath.search(self.jmespath_expression, response.json()),
                self.compression,
            )
        else:
            return None


def list_to_jsonl(data: list[dict], compression: 'CompressionOptions') -> BytesIO:
    out = BytesIO()
    df = pd.DataFrame(data)
    df.to_json(out, orient='records', lines=True, compression=compression)
    out.seek(0)
    return out
