import json
import os
from pathlib import Path

import pendulum
import pytest

from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.fab.auth_manager.models import User
from airflow.providers.http.operators.http import HttpOperator
from airflow_tools.providers.filesystem.operators.http_to_filesystem import (  # noqa
    MultiHttpToFilesystem,
)


def print_token_fn(token):
    print(token)


def get_bearer_token(response):
    return 'Bearer ' + response.json()['access_token']


@pytest.mark.skip(
    reason='This test is very specific to montel api and requires credentials'
)
def test_data_lake_delete_operator(dag, local_fs_conn_params):
    os.environ[
        'AIRFLOW_CONN_MONTEL_API'
    ] = """{
        "conn_type": "http",
        "host": "https://api.montelnews.com"
    }"""
    os.environ['AIRFLOW_CONN_LOCAL_DATA_LAKE'] = json.dumps(local_fs_conn_params)
    (Path(local_fs_conn_params['extra']['path']) / 'ohlc/2023-10-01').mkdir(
        parents=True, exist_ok=True
    )
    with dag:
        get_access_token = HttpOperator(
            task_id='get_access_token',
            http_conn_id='montel_api',
            endpoint='gettoken',
            method='POST',
            data=Variable.get('montel_api_get_token_data'),
            response_filter=get_bearer_token,
        )
        print_token = PythonOperator(
            task_id='print_token',
            python_callable=print_token_fn,
            op_kwargs={
                'token': get_access_token.output,
            },
        )
        call_ohlc_endpoint = MultiHttpToFilesystem(
            task_id='ohlc_data',
            multi_data=[{"symbolKey": "ICE BRN M2"}, {"symbolKey": "NDX GNM DA"}],
            http_conn_id='montel_api',
            endpoint='derivatives/ohlc/get',
            method='GET',
            data={
                'fields': '["Settlement", "Close"]',
                'fromDate': '2025-01-29',
                'toDate': '2025-01-30',
                'sortType': 'ascending',
                'insertElementsWhenDataMissing': 'never',
                'continuous': 'True',
            },
            headers={
                'Authorization': get_access_token.output,
            },
            jmespath_expression='Elements',
            filesystem_conn_id='local_data_lake',
            filesystem_path='ohlc/{{ ds }}/',
            strict_response_schema=False,
        )
        get_access_token >> print_token >> call_ohlc_endpoint
    dag.test(execution_date=pendulum.datetime(2023, 10, 1))
