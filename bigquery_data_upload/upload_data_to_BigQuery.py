import time
import pandas as pd
import pandas_gbq
from google.cloud import bigquery


from bigquery_schema.table_schema import table_schema
from bigquery_schema.format_schema import SchemaFormatter
from bigquery_data_upload.connect_to_BigQuery import BigQueryClient

from utils.logger import get_logger


class BigQueryDataUploader:
    def __init__(self, config_dict: dict, json_file_path: str) -> None:
        """ initializes an UploadDataToBigQuery instance """

        # self.config_file_path: str = config_file_path
        self.json_file_path: str = json_file_path
        self.data_file_path: str = config_dict['Paths']['data_file_path']

        self.logger = get_logger()

        self.schema: str = table_schema
        self.bigquery_client = BigQueryClient(config_dict)
        self.client = self.bigquery_client.set_up_client()
        self.table_ref = self.bigquery_client.get_table_ref(self.client)
        self.job_config = bigquery.LoadJobConfig()

        self.bigquery_df = pd.read_gbq(f"SELECT * FROM {self.table_ref}")
        self.json_df = pd.read_json(self.data_file_path, lines=True)

    def write_files_to_cloud(self) -> None:
        """ writes files to Google Cloud BigQuery
        this method sets up job configurations, adds new data, and logs the status of the upload
        """

        self.job_config.schema = SchemaFormatter.format_schema(self.schema)
        self.job_config.source_format = bigquery.SourceFormat.PARQUET

        self.job_config.write_disposition = "WRITE_APPEND"

        job = self.add_new_data()

        # wait for the job to complete
        while job.state != "DONE":
            job.reload()
            time.sleep(2)

        if job and job.errors:
            for error in job.errors:
                self.logger.error(error)
        else:
            self.logger.info("new data uploaded to BigQuery")

        job.result()

        self.modify_data()

    def add_new_data(self) -> bigquery.LoadJobConfig:
        """ adds new data to Google Cloud BigQuery
        this method modifies JSON data, reads it into a DataFrame, and loads it into BigQuery
        """

        # select data that is in json and not on BigQuery
        new_data = self.json_df[~self.json_df['csv_file_name'].isin(self.bigquery_df['csv_file_name'])]

        # load the new data into BigQuery
        job = self.client.load_table_from_dataframe(new_data, self.table_ref, job_config=self.job_config)

        return job

    def modify_data(self) -> None:
        """ modifies data in Google Cloud BigQuery
        this method identifies modified rows and updates them in BigQuery
        """

        common_data = pd.merge(self.json_df, self.bigquery_df, on='csv_file_name', how='inner')

        # identify rows with different hash values
        mask = common_data['csv_file_info_x'].apply(lambda x: x[0].get('hash') if x else None) != common_data[
            'csv_file_info_y'].apply(lambda x: x[0].get('hash') if x else None)

        # check if there are any True values in the mask
        if mask.any():
            # create a new dataframe with only the modified rows
            new_data = common_data[mask].copy()

            new_data['created_at'] = new_data['created_at_y']
            new_data['modified_at'] = new_data['modified_at_x']
            new_data['csv_file_info'] = new_data['csv_file_info_x']

            new_data = new_data[['csv_file_name', 'created_at', 'modified_at', 'csv_file_info']]

            # make sure the dataframe has a proper index
            new_data.reset_index(drop=True, inplace=True)

            # get modified csv file names to delete duplicate rows
            csv_file_names = new_data['csv_file_name'].tolist()

            delete_query = f"""
                DELETE FROM {self.table_ref}
                WHERE csv_file_name IN ({', '.join([f"'{name}'" for name in csv_file_names])})
            """

            # execute the delete query
            query_job = self.client.query(delete_query)
            query_job.result()

            # append the modified rows in BigQuery using pandas_gbq
            pandas_gbq.to_gbq(new_data, destination_table=str(self.table_ref), if_exists='append',
                              table_schema=self.schema)

            self.logger.info("csv data has been modified")
