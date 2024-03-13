from airflow.hooks.base import BaseHook
from azure.identity import ManagedIdentityCredential


class AzureFileShareServicePrincipalHook(BaseHook):
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

    def get_token_credentials(self):
        return ManagedIdentityCredential(
            client_id=self.service_principal_id,
            secret=self.service_principal_secret,
            tenant_id=self.tenant_id,
        )

    def get_conn(self):
        from azure.storage.fileshare import ShareClient

        credentials = self.get_token_credentials()

        return ShareClient(
            f"{self.protocol}://{self.host}.file.core.windows.net",
            credential=credentials,
            share_name=self.share_name,
            token_intent='backup',
        )
