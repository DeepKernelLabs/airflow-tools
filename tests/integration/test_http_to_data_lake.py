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
    
def test_http_to_data_lake_response_format_jsonl_with_jmespath_expression(s3_bucket, monkeypatch):
    monkeypatch.setenv(
        'AIRFLOW_CONN_HTTP_TEST',
        json.dumps(
            {
                'conn_type': 'http',
                'host': 'https://reqres.in',
            }
        ),
    )
    response = HttpToDataLake(
        task_id='test_http_to_data_lake',
        http_conn_id='http_test',
        data_lake_conn_id='data_lake_test',
        data_lake_path=s3_bucket + '/source1/entity1/{{ ds }}/',
        endpoint='/api/users',
        method='GET',
        save_format='jsonl',
        jmespath_expression='data[:2].{id: id, email: email}',
    )
    response.execute("")

    assert type(response.data) == list
    assert len(response.data) == 2
    assert 'id' in response.data[0] and 'email' in response.data[0]
    
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
    try:
        response_origin_no_list.execute("")
        assert False, 'This try should fail'
    except Exception as assertion_exception:
        assert type(assertion_exception) == AssertionError
        assert str(assertion_exception) == 'Expected response can\'t be transformed to jsonl. It is not  list[dict]'
        
    
def test_http_to_data_lake_response_format_jsonl_without_jmespath_expression(s3_bucket, monkeypatch):
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
    response = HttpToDataLake(
        task_id='test_http_to_data_lake',
        http_conn_id='http_test',
        data_lake_conn_id='data_lake_test',
        data_lake_path=s3_bucket + '/source1/entity1/{{ ds }}/',
        endpoint='/api/users',
        method='GET',
        save_format='jsonl',
        jmespath_expression=None,
    )
    
    try:
        response.execute("")
        assert False, 'This try should fail'
    except Exception as assertion_exception:
        assert type(assertion_exception) == AssertionError
        assert str(assertion_exception) == 'Expected response can\'t be transformed to jsonl. It is not  list[dict]'
        
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
    response_list = HttpToDataLake(
        task_id='test_http_to_data_lake',
        http_conn_id='http_test_list',
        data_lake_conn_id='data_lake_test',
        data_lake_path=s3_bucket + '/source1/entity1/{{ ds }}/',
        endpoint='/facts',
        method='GET',
        save_format='jsonl',
        jmespath_expression=None,
    )
    response_list.execute("")
    
    assert type(response_list.data) == list


def test_http_to_data_lake_response_format_json_wit_jmespath_expression(s3_bucket, monkeypatch):
    monkeypatch.setenv(
        'AIRFLOW_CONN_HTTP_TEST',
        json.dumps(
            {
                'conn_type': 'http',
                'host': 'https://reqres.in',
            }
        ),
    )
    response = HttpToDataLake(
        task_id='test_http_to_data_lake',
        http_conn_id='http_test',
        data_lake_conn_id='data_lake_test',
        data_lake_path=s3_bucket + '/source1/entity1/{{ ds }}/',
        endpoint='/api/users',
        method='GET',
        save_format='json',
        jmespath_expression='{page:page,total:total}',
    )
    response.execute("")
    
    assert type(response.data) == dict
    assert response.data == {'page': 1, 'total': 12}

  
def test_http_to_data_lake_response_format_json_without_jmespath_expression(s3_bucket, monkeypatch):
    monkeypatch.setenv(
        'AIRFLOW_CONN_HTTP_TEST',
        json.dumps(
            {
                'conn_type': 'http',
                'host': 'https://reqres.in',
            }
        ),
    )
    response = HttpToDataLake(
        task_id='test_http_to_data_lake',
        http_conn_id='http_test',
        data_lake_conn_id='data_lake_test',
        data_lake_path=s3_bucket + '/source1/entity1/{{ ds }}/',
        endpoint='/api/users',
        method='GET',
        save_format='json',
        jmespath_expression=None,
    )
    response.execute("")
    
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
    response = HttpToDataLake(
        task_id='test_http_to_data_lake',
        http_conn_id='http_test',
        data_lake_conn_id='data_lake_test',
        data_lake_path=s3_bucket + '/source1/entity1/{{ ds }}/',
        endpoint='/api/users',
        method='GET',
        save_format='wrong_format',
        jmespath_expression=None,
    )
    try:
        response.execute("")
        assert False, 'This try should fail'
    except Exception as not_implemented_exception:
        assert type(not_implemented_exception) == NotImplementedError
        assert str(not_implemented_exception) == 'Unknown save_format: wrong_format'
   
