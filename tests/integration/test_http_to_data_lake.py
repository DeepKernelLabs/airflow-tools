import json

import pendulum
import pytest
from botocore.exceptions import ClientError as BotoClientError

from airflow_tools.exceptions import ApiResponseTypeError
from airflow_tools.providers.http_to_data_lake.operators.http_to_data_lake import (
    HttpToDataLake,
)


def test_http_to_data_lake(dag, s3_bucket, s3_resource, monkeypatch):
    """This test uses the mock API https://reqres.in/"""
    monkeypatch.setenv(
        'AIRFLOW_CONN_HTTP_TEST',
        json.dumps(
            {
                'conn_type': 'http',
                'host': 'https://reqres.in',
            }
        ),
    )
    with dag:
        HttpToDataLake(
            task_id='test_http_to_data_lake',
            http_conn_id='http_test',
            data_lake_conn_id='data_lake_test',
            data_lake_path=s3_bucket + '/source1/entity1/{{ ds }}/',
            endpoint='/api/users',
            method='GET',
            jmespath_expression='data[:2].{id: id, email: email}',
        )
    dag.test(execution_date=pendulum.datetime(2023, 10, 1))

    content = (
        s3_resource.Object(s3_bucket, 'source1/entity1/2023-10-01/part0001.jsonl')
        .get()['Body']
        .read()
        .decode('utf-8')
    )
    assert (
        content
        == """\
{"id":1,"email":"george.bluth@reqres.in"}
{"id":2,"email":"janet.weaver@reqres.in"}
"""
    )


def test_http_to_data_lake_response_format_jsonl_with_jmespath_expression(
    s3_bucket, monkeypatch
):
    monkeypatch.setenv(
        'AIRFLOW_CONN_HTTP_TEST',
        json.dumps(
            {
                'conn_type': 'http',
                'host': 'https://reqres.in',
            }
        ),
    )
    http_to_data_lake_op = HttpToDataLake(
        task_id='test_http_to_data_lake',
        http_conn_id='http_test',
        data_lake_conn_id='data_lake_test',
        data_lake_path=s3_bucket + '/source1/entity1/{{ ds }}/',
        endpoint='/api/users',
        method='GET',
        save_format='jsonl',
        jmespath_expression='data[:2].{id: id, email: email}',
    )
    http_to_data_lake_op.execute({"ds": "2024-01-03"})

    assert isinstance(http_to_data_lake_op.data, list)
    assert len(http_to_data_lake_op.data) == 2
    assert (
        'id' in http_to_data_lake_op.data[0] and 'email' in http_to_data_lake_op.data[0]
    )

    with pytest.raises(ApiResponseTypeError):
        response_origin_no_list = HttpToDataLake(
            task_id='test_http_to_data_lake',
            http_conn_id='http_test',
            data_lake_conn_id='data_lake_test',
            data_lake_path=s3_bucket + '/source1/entity1/{{ ds }}/',
            endpoint='/api/users/2',
            method='GET',
            save_format='jsonl',
            jmespath_expression='data.{id: id, email: email}',
        )
        response_origin_no_list.execute({"ds": "2024-01-03"})


def test_http_to_data_lake_response_format_jsonl_without_jmespath_expression(
    s3_bucket, monkeypatch
):
    # This is not a list without jmespath expression, so it should fail
    monkeypatch.setenv(
        'AIRFLOW_CONN_HTTP_TEST',
        json.dumps(
            {
                'conn_type': 'http',
                'host': 'https://reqres.in',
            }
        ),
    )
    with pytest.raises(ApiResponseTypeError):
        http_to_data_lake_op = HttpToDataLake(
            task_id='test_http_to_data_lake',
            http_conn_id='http_test',
            data_lake_conn_id='data_lake_test',
            data_lake_path=s3_bucket + '/source1/entity1/{{ ds }}/',
            endpoint='/api/users',
            method='GET',
            save_format='jsonl',
            jmespath_expression=None,
        )
        http_to_data_lake_op.execute({"ds": "2024-01-03"})

    # This is a list without requiring a jmespath expression, so it should pass
    monkeypatch.setenv(
        'AIRFLOW_CONN_HTTP_TEST_LIST',
        json.dumps(
            {
                'conn_type': 'http',
                'host': 'https://cat-fact.herokuapp.com',
            }
        ),
    )
    http_to_data_lake_list_op = HttpToDataLake(
        task_id='test_http_to_data_lake',
        http_conn_id='http_test_list',
        data_lake_conn_id='data_lake_test',
        data_lake_path=s3_bucket + '/source1/entity1/{{ ds }}/',
        endpoint='/facts',
        method='GET',
        save_format='jsonl',
        jmespath_expression=None,
    )
    http_to_data_lake_list_op.execute({"ds": "2024-01-03"})

    assert isinstance(http_to_data_lake_list_op.data, list)


def test_http_to_data_lake_response_format_json_with_jmespath_expression(
    s3_bucket, monkeypatch
):
    monkeypatch.setenv(
        'AIRFLOW_CONN_HTTP_TEST',
        json.dumps(
            {
                'conn_type': 'http',
                'host': 'https://reqres.in',
            }
        ),
    )
    http_to_data_lake_op = HttpToDataLake(
        task_id='test_http_to_data_lake',
        http_conn_id='http_test',
        data_lake_conn_id='data_lake_test',
        data_lake_path=s3_bucket + '/source1/entity1/{{ ds }}/',
        endpoint='/api/users',
        method='GET',
        save_format='json',
        jmespath_expression='{page:page,total:total}',
    )
    http_to_data_lake_op.execute({"ds": "2024-01-03"})

    assert isinstance(http_to_data_lake_op.data, dict)
    assert http_to_data_lake_op.data == {'page': 1, 'total': 12}


def test_http_to_data_lake_response_format_json_without_jmespath_expression(
    s3_bucket, monkeypatch
):
    monkeypatch.setenv(
        'AIRFLOW_CONN_HTTP_TEST',
        json.dumps(
            {
                'conn_type': 'http',
                'host': 'https://reqres.in',
            }
        ),
    )
    http_to_data_lake_op = HttpToDataLake(
        task_id='test_http_to_data_lake',
        http_conn_id='http_test',
        data_lake_conn_id='data_lake_test',
        data_lake_path=s3_bucket + '/source1/entity1/{{ ds }}/',
        endpoint='/api/users',
        method='GET',
        save_format='json',
        jmespath_expression=None,
    )
    http_to_data_lake_op.execute({"ds": "2024-01-03"})

    assert True


def test_http_to_data_lake_response_wrong_format(s3_bucket, monkeypatch):
    monkeypatch.setenv(
        'AIRFLOW_CONN_HTTP_TEST',
        json.dumps(
            {
                'conn_type': 'http',
                'host': 'https://reqres.in',
            }
        ),
    )
    with pytest.raises(NotImplementedError, match=r".*wrong_format.*"):
        http_to_data_lake_op = HttpToDataLake(
            task_id='test_http_to_data_lake',
            http_conn_id='http_test',
            data_lake_conn_id='data_lake_test',
            data_lake_path=s3_bucket + '/source1/entity1/{{ ds }}/',
            endpoint='/api/users',
            method='GET',
            save_format='wrong_format',
            jmespath_expression=None,
        )
        http_to_data_lake_op.execute({"ds": "2024-01-03"})
        assert False, 'This try should fail'


def reqres_pagination_function(response):
    current_page = response.json()['page']
    if current_page < response.json()['total_pages']:
        return {'data': {'page': current_page + 1}}


def test_http_to_datalake_pagination_jsonl(dag, s3_bucket, s3_resource, monkeypatch):
    """This test uses the mock API https://reqres.in/"""
    monkeypatch.setenv(
        'AIRFLOW_CONN_HTTP_TEST',
        json.dumps(
            {
                'conn_type': 'http',
                'host': 'https://reqres.in',
            }
        ),
    )
    with dag:
        HttpToDataLake(
            task_id='test_http_to_data_lake',
            http_conn_id='http_test',
            data_lake_conn_id='data_lake_test',
            data_lake_path=s3_bucket + '/source1/entity1/{{ ds }}/',
            endpoint='/api/users',
            method='GET',
            data={'page': 1},
            save_format='jsonl',
            jmespath_expression='data[:2].{id: id, email: email}',
            pagination_function=reqres_pagination_function,
        )
    dag.test(execution_date=pendulum.datetime(2023, 10, 1))

    content_part_1 = (
        s3_resource.Object(s3_bucket, 'source1/entity1/2023-10-01/part0001.jsonl')
        .get()['Body']
        .read()
        .decode('utf-8')
    )

    content_part_2 = (
        s3_resource.Object(s3_bucket, 'source1/entity1/2023-10-01/part0002.jsonl')
        .get()['Body']
        .read()
        .decode('utf-8')
    )
    assert (
        content_part_1
        == """\
{"id":1,"email":"george.bluth@reqres.in"}
{"id":2,"email":"janet.weaver@reqres.in"}
"""
    )
    assert (
        content_part_2
        == """\
{"id":7,"email":"michael.lawson@reqres.in"}
{"id":8,"email":"lindsay.ferguson@reqres.in"}
"""
    )


def test_http_to_datalake_pagination_json(dag, s3_bucket, s3_resource, monkeypatch):
    """This test uses the mock API https://reqres.in/"""
    monkeypatch.setenv(
        'AIRFLOW_CONN_HTTP_TEST',
        json.dumps(
            {
                'conn_type': 'http',
                'host': 'https://reqres.in',
            }
        ),
    )
    with dag:
        HttpToDataLake(
            task_id='test_http_to_data_lake',
            http_conn_id='http_test',
            data_lake_conn_id='data_lake_test',
            data_lake_path=s3_bucket + '/source1/entity1/{{ ds }}/',
            endpoint='/api/users',
            method='GET',
            data={'page': 1},
            save_format='json',
            jmespath_expression='{page:page,total:total}',
            pagination_function=reqres_pagination_function,
        )
    dag.test(execution_date=pendulum.datetime(2023, 10, 1))

    content_part1 = (
        s3_resource.Object(s3_bucket, 'source1/entity1/2023-10-01/part0001.json')
        .get()['Body']
        .read()
        .decode('utf-8')
    )
    content_part2 = (
        s3_resource.Object(s3_bucket, 'source1/entity1/2023-10-01/part0002.json')
        .get()['Body']
        .read()
        .decode('utf-8')
    )

    assert content_part1 == """{"page": 1, "total": 12}"""
    assert content_part2 == """{"page": 2, "total": 12}"""


def test_http_to_data_lake_check_one_page_data_is_duplicated(
    dag, s3_bucket, s3_resource, monkeypatch
):
    # Check if a second file (`part002.jsonl`) is created calling the API
    # for avoiding duplicates. If it exists, the test fails.

    """This test uses the mock API https://reqres.in/"""
    monkeypatch.setenv(
        'AIRFLOW_CONN_HTTP_TEST',
        json.dumps(
            {
                'conn_type': 'http',
                'host': 'https://reqres.in',
            }
        ),
    )
    with dag:
        HttpToDataLake(
            task_id='test_http_to_data_lake',
            http_conn_id='http_test',
            data_lake_conn_id='data_lake_test',
            data_lake_path=s3_bucket + '/source1/entity1/{{ ds }}/',
            endpoint='/api/users',
            method='GET',
            jmespath_expression='data[:2].{id: id, email: email}',
        )
    dag.test(execution_date=pendulum.datetime(2023, 10, 1))

    with pytest.raises(BotoClientError, match=r".*NoSuchKey.*"):
        _ = (
            s3_resource.Object(s3_bucket, 'source1/entity1/2023-10-01/part0002.jsonl')
            .get()['Body']
            .read()
            .decode('utf-8')
        )
