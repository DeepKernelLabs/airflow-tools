import json

from airflow_tools.providers.filesystem.operators.filesystem import SQLToFilesystem


def test_execute_with_a_single_output(sqlite_database, s3_bucket, monkeypatch):
    monkeypatch.setenv(
        'AIRFLOW_CONN_SQLITE_TEST',
        json.dumps(
            {
                'conn_type': 'sqlite',
                'host': str(sqlite_database),
            }
        ),
    )

    sql_to_filesystem_operator = SQLToFilesystem(
        task_id='test_sql_to_filesystem_operator',
        source_sql_conn_id='sqlite_test',
        destination_fs_conn_id='data_lake_test',
        sql='SELECT * FROM TESTING',
        destination_path=s3_bucket + '/source1/entity1/',
    )
    sql_to_filesystem_operator.execute({})

    assert len(sql_to_filesystem_operator.files) == 1
    assert (
        sql_to_filesystem_operator.files[0]
        == f'{s3_bucket}/source1/entity1/part0001.parquet'
    )


def test_execute_with_multiple_outputs(sqlite_database, s3_bucket, monkeypatch):
    monkeypatch.setenv(
        'AIRFLOW_CONN_SQLITE_TEST',
        json.dumps(
            {
                'conn_type': 'sqlite',
                'host': str(sqlite_database),
            }
        ),
    )

    sql_to_filesystem_operator = SQLToFilesystem(
        task_id='test_sql_to_filesystem_operator',
        source_sql_conn_id='sqlite_test',
        destination_fs_conn_id='data_lake_test',
        sql='SELECT * FROM TESTING',
        destination_path=s3_bucket + '/source1/entity1/',
        batch_size=2,
    )
    sql_to_filesystem_operator.execute({})

    assert len(sql_to_filesystem_operator.files) == 3
    assert (
        sql_to_filesystem_operator.files[0]
        == f'{s3_bucket}/source1/entity1/part0001.parquet'
    )
    assert (
        sql_to_filesystem_operator.files[1]
        == f'{s3_bucket}/source1/entity1/part0002.parquet'
    )
    assert (
        sql_to_filesystem_operator.files[2]
        == f'{s3_bucket}/source1/entity1/part0003.parquet'
    )
