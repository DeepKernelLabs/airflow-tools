import logging
import subprocess
import venv

import pytest

logger = logging.getLogger(__file__)


@pytest.fixture
def virtual_environment(tmp_path):
    """Fixture to create a virtual environment, yield its path. Clean up is performed by the tmp_path fixture."""
    tmpdir = tmp_path / 'venv'
    tmpdir.mkdir()
    builder = venv.EnvBuilder(with_pip=True, system_site_packages=False)
    builder.create(tmpdir)
    logger.info(f'Created temporary virtualenv in {tmpdir}')

    yield tmpdir


def install_package(venv_path: str, package: str, cwd: str):
    """Install the package in the virtual environment."""
    subprocess.check_call([f"{venv_path}/bin/pip", "install", package], cwd=cwd)


def test_import_package(virtual_environment, project_path):
    """Test package import in the provided virtual environment."""
    venv_path = virtual_environment
    for package in [str(project_path)]:
        install_package(venv_path, package, cwd=str(project_path))

    # Test importing the package
    try:
        result = subprocess.check_output(
            [
                f"{venv_path}/bin/python",
                "-c",
                "from airflow.utils.entry_points import entry_points_with_dist; print(list(entry_points_with_dist('apache_airflow_provider')))",
            ],
            universal_newlines=True,
            stderr=subprocess.STDOUT,
        )
        assert 'airflow_tools.providers.package:get_provider_info' in result
        result = subprocess.check_output(
            [
                f"{venv_path}/bin/python",
                "-c",
                "from airflow.providers_manager import ProvidersManager; pm = ProvidersManager(); print(pm.providers['airflow-tools'].data['package-name'])",
            ],
            universal_newlines=True,
            stderr=subprocess.STDOUT,
        )
        assert 'airflow-tools' in result
        result = subprocess.check_output(
            [
                f"{venv_path}/bin/python",
                "-c",
                "from airflow_tools.providers.filesystem.operators.http_to_filesystem import HttpToFilesystem; print('Import Ok')",
            ],
            universal_newlines=True,
            stderr=subprocess.STDOUT,
        )
        assert 'Import Ok' in result

        # TODO: When we have custom hooks, check their installation is correct by using airflow.providers_manager.ProvidersManager.hooks
    except subprocess.CalledProcessError as e:
        logger.exception(e)
        pytest.fail(f"Failed to import the package in a clean environment: {e.output}")
