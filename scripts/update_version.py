import logging
import sys
from pathlib import Path

import toml

logger = logging.getLogger(__name__)


def main():
    # Load the pyproject.toml file
    project_root = Path(__file__).parent.parent
    pyproject_path = project_root / "pyproject.toml"
    pyproject = toml.load(pyproject_path)

    # Extract the current version
    version = pyproject["project"]["version"]

    # Write the current version to _version.py
    version_py_path = project_root / "src/airflow_toolkit/_version.py"
    version_py_content = f'__version__ = "{version}"\n'
    if version_py_path.read_text() == version_py_content:
        logger.info("Version up to date. Skipping...")
        sys.exit(0)
    version_py_path.write_text(version_py_content)
    logger.warning("Updated _version.py")
    sys.exit(1)


if __name__ == "__main__":
    main()
