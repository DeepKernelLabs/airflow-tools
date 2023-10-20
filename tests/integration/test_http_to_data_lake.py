import json

import pendulum

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
