import pytest
import mock

from airflow_tools.hooks.azure_fileshare_service_principal import AzureFileShareServicePrincipalHook
from azure.storage.fileshare import ShareClient

HOST = "somerandomhost.file.core.windows.net"
PROTOCOL = "https"
SHARE_NAME = "some-share-name"
TENANT_ID = "some-tentant-id"
LOGIN = "some-login-value"
PASSWORD ="some-password-value"
LIST_DIRECTORIES_AND_FILES_EXPECTED_RESULT = [{'name': 'Folder-test-name', 'last_modified': None, 'etag': None, 'server_encrypted': None, 'metadata': None}]


@pytest.fixture
def env_var_setup(monkeypatch):
    monkeypatch.setenv(
        'AIRFLOW_CONN_AZURE_FILESHARE_SP',
        f'''{{"conn_type": "azure_file_share_sp","host": "{HOST}", "login": "{LOGIN}","password": "{PASSWORD}","extra":{{"tenant_id": "{TENANT_ID}", "share_name": "{SHARE_NAME}", "protocol": "{PROTOCOL}"}}}}''',
    )

    return True

@mock.patch('azure.storage.fileshare.ShareClient.list_directories_and_files')
def test_azure_fileshare_service_principal(
    share_client_list_directories_and_files,
    env_var_setup
):
    share_client_list_directories_and_files.return_value = LIST_DIRECTORIES_AND_FILES_EXPECTED_RESULT
    
    azure_fileshare_sp_hook = AzureFileShareServicePrincipalHook(conn_id="AZURE_FILESHARE_SP")
    
    azure_fileshare_sp_conn = azure_fileshare_sp_hook.get_conn()

    assert azure_fileshare_sp_hook.account_url == f"{PROTOCOL}://{HOST}"
    assert azure_fileshare_sp_conn.list_directories_and_files() == LIST_DIRECTORIES_AND_FILES_EXPECTED_RESULT
    assert isinstance(azure_fileshare_sp_conn, ShareClient)