import os
from google.cloud import bigquery

from etl_project.config.config_reader import ReadConfigFile
from upload_data_to_mysql import GetMysqlData


class UploadDataToBigQuery:
    def __init__(self, config_file_path):
        config_reader = ReadConfigFile(config_file_path)
        self.google_credentials = config_reader.get_google_credentials()

        self.project_id = config_reader.get_bigquery_project_id()
        self.dataset_id = config_reader.get_bigquery_dataset_id()
        self.table_id = config_reader.get_bigquery_table_id()

        self.result_df = GetMysqlData(config_file_path).select_all_data()

    def upload_data(self):

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.google_credentials
        client = bigquery.Client(project=self.project_id)

        dataset_ref = client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(self.table_id)

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
        )

        job = client.load_table_from_dataframe(self.result_df, table_ref, job_config=job_config)

        job.result()




