import json
from io import BytesIO, StringIO
from typing import TYPE_CHECKING, Any, Callable, Literal

import jmespath
import pandas as pd
from airflow.hooks.base import BaseHook
from requests import Response

try:
    from airflow.providers.http.operators.http import HttpOperator
except ImportError:
    from airflow.providers.http.operators.http import SimpleHttpOperator as HttpOperator

from airflow.utils.context import Context

from airflow_tools.compression_utils import CompressionOptions, compress
from airflow_tools.data_lake_facade import DataLakeFacade
from airflow_tools.exceptions import ApiResponseTypeError

if TYPE_CHECKING:
    from requests.auth import AuthBase

SaveFormat = Literal['jsonl']


class HttpToDatalakePagination(HttpOperator):
    template_fields = list(HttpOperator.template_fields) + ['data_lake_path']
    template_fields_renderers = HttpOperator.template_fields_renderers
    n_pages = 1

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
        auth_type: type['AuthBase'] | None = None,
        jmespath_expression: str | None = None,
        pagination_function: Callable | None = None,
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
        self.data = data or {}
        self.headers = headers or {}
        self.auth_type = auth_type
        self.jmespath_expression = jmespath_expression
        self.pagination_function = pagination_function
        self.response_filter = self._response_filter

    def execute(self, context: 'Context') -> Any:
        self.execute_sync(context=context)

    def execute_sync(self, context: Context) -> Any:
        self.log.info("Calling HTTP method")
        response = self.hook.run(
            self.endpoint, self.data, self.headers, self.extra_options
        )
        response = self.paginate_sync(context=context, response=response)
        return response

    def paginate_sync(
        self, context: Context, response: Response
    ) -> Response | list[Response]:
        all_responses = []

        while True:
            # Load response
            data_lake_conn = BaseHook.get_connection(self.data_lake_conn_id)
            data_lake_facade = DataLakeFacade(
                conn=data_lake_conn.get_hook(),
            )

            file_path = self.data_lake_path.rstrip('/') + '/' + self._file_name()
            processed_response = self.process_response(
                context=context, response=response
            )
            all_responses.append(response)

            data_lake_facade.write(processed_response, file_path)

            if not self.pagination_function:
                return response

            next_page_params = self.pagination_function(response)

            if not next_page_params:
                break

            self.n_pages += 1

            response = self.hook.run(
                **self._merge_next_page_parameters(next_page_params)
            )

        return all_responses

    def _file_name(self) -> str:
        file_name = f'part{self.n_pages:04}.{self.save_format}'
        if self.compression:
            file_name += f'.{self.compression}'
        return file_name

    def _response_filter(self, response) -> BytesIO:
        # After pagination response can be a list of responses that needs to be unested to use .json()
        if isinstance(response, list):
            if not self.jmespath_expression:
                self.data = [r.json() for r in response]
            else:
                self.data = [
                    jmespath.search(self.jmespath_expression, r.json())
                    for r in response
                ]

        else:
            if not self.jmespath_expression:
                self.data = response.json()
            else:
                self.data = jmespath.search(self.jmespath_expression, response.json())

        match self.save_format:
            case 'json':
                return json_to_binary(self.data, self.compression)

            case 'jsonl':
                if isinstance(response, list):
                    self.data = [item for sublist in self.data for item in sublist]
                if not isinstance(self.data, list):
                    raise ApiResponseTypeError(
                        'Expected response can\'t be transformed to jsonl. It is not  list[dict]'
                    )
                return list_to_jsonl(self.data, self.compression)

            case _:
                raise NotImplementedError(f'Unknown save_format: {self.save_format}')


def list_to_jsonl(data: list[dict], compression: 'CompressionOptions') -> BytesIO:
    out = StringIO()
    df = pd.DataFrame(data)
    df.to_json(out, orient='records', lines=True, compression=compression)
    out.seek(0)
    return BytesIO(out.getvalue().encode())


def json_to_binary(data: dict, compression: 'CompressionOptions') -> BytesIO:
    json_string = json.dumps(data).encode()
    compressed_json = compress(compression, json_string)
    out = BytesIO(compressed_json)
    return out
