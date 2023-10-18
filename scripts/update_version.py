from pathlib import Path
import toml


def main():
    # Load the pyproject.toml file
    project_root = Path(__file__).parent.parent
    pyproject_path = project_root / "pyproject.toml"
    pyproject = toml.load(pyproject_path)

    # Extract the current version
    version = pyproject["tool"]["poetry"]["version"]

    # Write the current version to _version.py
    version_py_path = project_root / "src/providers/_version.py"
    version_py_content = f'__version__ = "{version}"\n'
    version_py_path.write_text(version_py_content)
    print(f"Updated _version.py")


if __name__ == "__main__":
    main()
