import json

import pendulum
from airflow.hooks.base import BaseHook

from airflow_tools.providers.deltalake.operators.filesystem_to_database import (
    FilesystemToDatabaseOperator,
)


def test_execute_with_a_single_output(
    dag, sa_session, tmp_path, sqlite_database, monkeypatch
):
    monkeypatch.setenv(
        'AIRFLOW_CONN_SQLITE_TEST',
        json.dumps(
            {
                'conn_type': 'sqlite',
                'host': str(sqlite_database),
            }
        ),
    )
    monkeypatch.setenv(
        'AIRFLOW_CONN_LOCAL_FS_TEST',
        json.dumps({'conn_type': 'fs', 'extra': {'path': str(tmp_path)}}),
    )

    # Create folder (tmp dir) and csv file
    folder_path = tmp_path / 'data_lake/2023/10/01/'
    folder_path.mkdir(parents=True, exist_ok=True)
    file_path = folder_path / 'test.csv'
    file_path.write_text('a,b,c\n1,2,3\n4,5,6\n7,8,9')

    loaded_at = pendulum.now()
    with dag:
        FilesystemToDatabaseOperator(
            filesystem_conn_id='local_fs_test',
            database_conn_id='sqlite_test',
            filesystem_path='data_lake/{{ ds.replace("-", "/") }}/',
            db_table='test_table',
            task_id='filesystem_to_database_test',
            metadata={
                '_DS': '{{ ds }}',
                '_INTERVAL_START': '{{ data_interval_start }}',
                '_INTERVAL_END': '{{ data_interval_end }}',
                '_LOADED_AT': loaded_at.isoformat(),
            },
        )

    # @provide_session
    # def test_dag_with_session(session=None, **kwargs):
    #     dag_run = dag.test(session=session, **kwargs)
    #     session.add(dag_run)
    #     return dag_run

    # execution_date = pendulum.datetime(2023, 10, 1)
    # dag_run = test_dag_with_session(execution_date=execution_date)

    execution_date = pendulum.datetime(2023, 10, 1)
    dag_run = dag.test(execution_date=execution_date, session=sa_session)

    source_sql_hook = BaseHook.get_connection('sqlite_test').get_hook()
    df = source_sql_hook.get_pandas_df(sql='SELECT * FROM test_table', parse_dates=['_DS', '_INTERVAL_START', '_INTERVAL_END', '_LOADED_AT'])

    # Example of expected result
    #    a  b  c                         _DS            _INTERVAL_START              _INTERVAL_END                        _LOADED_AT       _LOADED_FROM
    # 0  1  2  3  2023-10-01 00:00:00.000000  2023-09-30T00:00:00+00:00  2023-10-01T00:00:00+00:00  2024-10-25T12:54:13.211896+02:00  /tmp/.../test.csv
    # 1  4  5  6  2023-10-01 00:00:00.000000  2023-09-30T00:00:00+00:00  2023-10-01T00:00:00+00:00  2024-10-25T12:54:13.211896+02:00  /tmp/.../test.csv
    # 2  7  8  9  2023-10-01 00:00:00.000000  2023-09-30T00:00:00+00:00  2023-10-01T00:00:00+00:00  2024-10-25T12:54:13.211896+02:00  /tmp/.../test.csv

    assert len(df) == 3
    assert str(df.iloc[0]['_DS']) == execution_date.to_datetime_string()
    assert (
        str(df.iloc[0]['_INTERVAL_START'])
        == (execution_date - pendulum.duration(days=1)).to_datetime_string()
    )
    assert str(df.iloc[0]['_INTERVAL_END']) == execution_date.to_datetime_string()
    assert df.iloc[0]['_LOADED_FROM'] == str(folder_path / 'test.csv')
    assert str(df.iloc[0]['_LOADED_AT']) == loaded_at.isoformat().replace('T', ' ')
