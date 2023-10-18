from io import BytesIO
from typing import Any, Callable, Literal
from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.hooks.base import BaseHook
from airflow.providers.http.operators.http import SimpleHttpOperator
import jmespath
import pandas as pd
from pandas._typing import CompressionOptions

from data_lake_facade import DataLakeFacade


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
        compression: CompressionOptions = None,
        endpoint: str | None = None,
        method: str = "POST",
        data: Any = None,
        headers: dict[str, str] | None = None,
        jmespath_expression: str | None = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.data_lake_conn_id = data_lake_conn_id
        self.data_lake_path = data_lake_path
        self.save_format = save_format
        self.compression = compression
        self.endpoint = endpoint
        self.method = method
        self.request_data = data
        self.headers = headers
        self.jmespath_expression = jmespath_expression

    def execute(self, context: Context) -> Any:
        data_lake_facade = DataLakeFacade(
            conn=BaseHook.get_connection(self.data_lake_conn_id)
        )

        data = SimpleHttpOperator(
            http_conn_id=self.http_conn_id,
            endpoint=self.endpoint,
            method=self.method,
            data=self.request_data,
            headers=self.headers,
            response_filter=self._response_filter()
        ).execute(context)

        data_lake_facade.write(data, self.data_lake_path)
    
    def _response_filter(self) -> Callable | None:
        if self.save_format == 'jsonl' and not self.jmespath_expression:
            return lambda response: list_to_jsonl(response.json(), self.compression)
        elif self.save_format == 'jsonl' and self.jmespath_expression:
            return lambda response: list_to_jsonl(jmespath.search(self.jmespath_expression, response.json()), self.compression)
        else:
            return None


def list_to_jsonl(data: list[dict], compression: CompressionOptions) -> BytesIO:
    out = BytesIO()
    df = pd.DataFrame(data)
    df.to_json(out, orient='records', lines=True, compression=compression)
    out.seek(0)
