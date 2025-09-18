import json

from airflow_toolkit.providers.deltalake.sensors.filesystem_file import (
    FilesystemFileSensor,
)


def test_filesystem_file_sensor(tmp_path, sqlite_database, monkeypatch):
    """
    Test FilesystemFileSensor with a local filesystem.
    """

    monkeypatch.setenv(
        "AIRFLOW_CONN_SQLITE_TEST",
        json.dumps(
            {
                "conn_type": "sqlite",
                "host": str(sqlite_database),
            }
        ),
    )
    monkeypatch.setenv(
        "AIRFLOW_CONN_LOCAL_FS_TEST",
        json.dumps({"conn_type": "fs", "extra": {"path": str(tmp_path)}}),
    )

    monkeypatch.setenv("AIRFLOW__CORE__EXECUTOR", "SequentialExecutor")

    # Create folder (tmp dir) and csv file
    folder_path = tmp_path / "data_lake/2023/10/01/"
    folder_path.mkdir(parents=True, exist_ok=True)
    file_path = folder_path / "test.csv"
    file_path.write_text("a,b,c\n1,2,3\n4,5,6\n7,8,9")

    sensor1 = FilesystemFileSensor(
        task_id="check_file_existence_exist",
        filesystem_conn_id="local_fs_test",
        source_path="data_lake/2023/10/01/test.csv",
        poke_interval=2,
        timeout=4,
    )

    sensor2 = FilesystemFileSensor(
        task_id="test_check_file_existence_no_exist",
        filesystem_conn_id="local_fs_test",
        source_path="data_lake/2023/10/01/no-exist-test.csv",
        poke_interval=2,
        timeout=4,
    )

    result1 = sensor1.poke(context={})
    result2 = sensor2.poke(context={})

    assert result1 is True
    assert result2 is False
