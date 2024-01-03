from io import BytesIO
from typing import TYPE_CHECKING, Any, Callable, Literal


import json
import jmespath
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.http.operators.http import HttpOperator

from airflow_tools.data_lake_facade import DataLakeFacade

if TYPE_CHECKING:
    from airflow.utils.context import Context
    from pandas._typing import CompressionOptions
    from requests.auth import AuthBase

SaveFormat = Literal['jsonl']


class HttpToDataLake(BaseOperator):
    conn_type = 'http_to_data_lake'
    template_fields = HttpOperator.template_fields + ('data_lake_path',)
    template_fields_renderers = HttpOperator.template_fields_renderers

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
        data = HttpOperator(
            task_id='http-operator',
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
        match self.save_format:
            case 'json':
                if not self.jmespath_expression:
                    self.data = response.json()
                else:
                    self.data = jmespath.search(self.jmespath_expression, response.json())
                
                return json_to_binary(self.data)
            
            case 'jsonl':
                if not self.jmespath_expression:
                    self.data = response.json()

                else:
                    self.data = jmespath.search(self.jmespath_expression, response.json())

                assert type(self.data) == list, 'Expected response can\'t be transformed to jsonl. It is not  list[dict]'
                return list_to_jsonl(self.data, self.compression)
            
            case _:
                raise NotImplementedError(f'Unknown save_format: {self.save_format}')



def list_to_jsonl(data: list[dict], compression: 'CompressionOptions') -> BytesIO:
    out = BytesIO()
    df = pd.DataFrame(data)
    df.to_json(out, orient='records', lines=True, compression=compression)
    out.seek(0)
    return out

def json_to_binary(data: dict) -> BytesIO:
    out = BytesIO()
    out.write(json.dumps(data).encode())
    out.seek(0)
    return out
