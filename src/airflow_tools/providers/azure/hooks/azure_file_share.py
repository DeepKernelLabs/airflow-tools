from airflow.hooks.base import BaseHook
from azure.identity import ClientSecretCredential


class AzureFileShareServicePrincipalHook(BaseHook):
    """
    Requires defined connection with this structure:
        conn_type: azure_file_share_sp
        host: <hostname>
        login: <service_principal_id>
        password: <service_principal_secret>
        extra: {"tenant_id": "<tenant_id>", "share_name": "<share_name>", "protocol": "https"}
    """

    def __init__(self, conn_id: str):
        super().__init__()
        self.conn_id = conn_id
        self.connection = self.get_connection(conn_id)
        self.host = self.connection.host
        self.service_principal_id = self.connection.login
        self.service_principal_secret = self.connection.password
        self.tenant_id = self.connection.extra_dejson.get("tenant_id")
        self.share_name = self.connection.extra_dejson.get("share_name")
        self.protocol = self.connection.extra_dejson.get("protocol", "https")

        self.credentials = self.get_token_credentials()
        self.account_url = f"{self.protocol}://{self.host}"

    def get_token_credentials(self):
        return ClientSecretCredential(
            client_id=self.service_principal_id,
            client_secret=self.service_principal_secret,
            tenant_id=self.tenant_id,
        )

    def get_conn(self):
        from azure.storage.fileshare import ShareClient

        return ShareClient(
            account_url=self.account_url,
            credential=self.credentials,
            share_name=self.share_name,
            token_intent='backup',
        )
