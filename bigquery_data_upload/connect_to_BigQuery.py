import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from config.config_reader import get_config_value
from bigquery_schema.table_schema import table_schema

from logging.logger import get_logger


class BigQueryClient:
    """ initializes a BigQueryClient with the specified configuration file path """

    def __init__(self, config_file_path: str) -> None:
        self.project_id: str = get_config_value(config_file_path, "BigQuery", "project_id")
        self.dataset_id: str = get_config_value(config_file_path, "BigQuery", "dataset_id")
        self.table_id: str = get_config_value(config_file_path, "BigQuery", "table_id")

        self.google_credentials: str = get_config_value(config_file_path, "Paths", "google_application_credentials_path")

        self.table_schema = table_schema

        self.logger = get_logger()

    def set_up_client(self) -> bigquery.Client:
        """ sets up and returns a google BigQueryClient """

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.google_credentials
        return bigquery.Client(project=self.project_id)

    def get_table_ref(self, client: bigquery.Client) -> bigquery.TableReference:
        """ retrieves or creates a table reference for the specified dataset and table """

        dataset_ref = client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(self.table_id)

        # check if the table exists in BigQuery if it doesn't log the creation of the table
        if not self.table_exists(client, table_ref):
            table = bigquery.Table(table_ref, schema=table_schema)
            client.create_table(table)

            self.logger.error(f"created BigQuery table as: {self.table_id}")

        return table_ref

    @staticmethod
    def table_exists(client, table_ref: bigquery.TableReference) -> bool:
        """
        checks if a table with the provided reference exists """

        try:
            client.get_table(table_ref)
            return True
        except NotFound:
            return False


