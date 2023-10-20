from pathlib import Path

import pytest


@pytest.fixture
def project_path() -> Path:
    return Path(__file__).parent.parent.absolute()


@pytest.fixture
def source_path(project_path) -> Path:
    return project_path / 'src'


@pytest.fixture
def tests_path(project_path) -> Path:
    return project_path / 'tests'


@pytest.fixture
def load_airflow_test_config() -> Path:
    from airflow.configuration import conf

    conf.load_test_config()
