import os
from google.cloud import bigquery

from logger import get_logger
from get_mysql_data import GetMysqlData
from etl_project.config.config_reader import ReadConfigFile


class UploadDataToBigQuery:
    def __init__(self, config_file_path: str):
        config_reader = ReadConfigFile(config_file_path)
        self.google_credentials: str = config_reader.get_google_credentials()

        self.project_id: str = config_reader.get_bigquery_project_id()
        self.dataset_id: str = config_reader.get_bigquery_dataset_id()
        self.table_id: str = config_reader.get_bigquery_table_id()

        # getting logger instance for logging
        self.logger = get_logger()

        self.result_df = GetMysqlData(config_file_path).select_all_data()

    def upload_data(self) -> None:
        """ upload data from mysql dataframe to BigQuery"""

        try:
            # set google credentials and initialize BigQuery client
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.google_credentials
            client = bigquery.Client(project=self.project_id)

            # get dataset and table refrences
            dataset_ref = client.dataset(self.dataset_id)
            table_ref = dataset_ref.table(self.table_id)

            # load data into a Bigquery table and wait for it to complete
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
            )

            job = client.load_table_from_dataframe(self.result_df, table_ref, job_config=job_config)

            job.result()

            self.logger.info("committed changes to BigQuery")
        except Exception as error:

            self.logger.error(f"error uploading data to BigQuery: {error}")






