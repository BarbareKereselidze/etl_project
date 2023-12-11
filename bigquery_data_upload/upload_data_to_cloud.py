import os
import time
import json
from datetime import datetime
from google.cloud import bigquery

from etl_project.logging.logger import get_logger
from etl_project.bigquery_data_upload.table_schema import table_schema
from etl_project.config.config_reader import get_config_value


class BigQueryClient:
    def __init__(self, config_file_path):
        self.project_id = get_config_value(config_file_path, "BigQuery", "project_id")
        self.dataset_id = get_config_value(config_file_path, "BigQuery", "dataset_id")
        self.table_id = get_config_value(config_file_path, "BigQuery", "table_id")

        self.google_credentials = get_config_value(config_file_path, "Paths", "google_application_credentials_path")

    def set_up_client(self):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.google_credentials
        return bigquery.Client(project=self.project_id)

    def get_table_ref(self, client):
        dataset_ref = client.dataset(self.dataset_id)
        return dataset_ref.table(self.table_id)


class JsonDataModifier:
    @staticmethod
    def modify_json(json_file_path, data_file_path):
        current_datetime = datetime.now()
        formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

        json_data = json.load(open(json_file_path))
        adjusted_json_data = [
            {
                'csv_file_name': list(row.keys())[0],
                'created_at': formatted_datetime,
                'modified_at': formatted_datetime,
                'csv_file_info': row[list(row.keys())[0]]
            }
            for row in json_data
        ]

        with open(data_file_path, "w") as adjusted_file:
            adjusted_file.write('\n'.join(json.dumps(row) for row in adjusted_json_data))


class SchemaFormatter:
    @staticmethod
    def format_schema(schema):
        formatted_schema = []
        for row in schema:
            if 'fields' in row:
                sub_schema = SchemaFormatter.format_schema(row['fields'])
                formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode'], fields=sub_schema))
            else:
                formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))

        return formatted_schema


class UploadDataToBigQuery:
    def __init__(self, config_file_path, json_file_path):
        self.config_file_path = config_file_path
        self.json_file_path = json_file_path
        self.data_file_path = get_config_value(config_file_path, "Paths", "data_file_path")
        self.schema = table_schema
        self.logger = get_logger()
        self.bigquery_client = BigQueryClient(config_file_path)

    def write_files_to_cloud(self):
        client = self.bigquery_client.set_up_client()
        table_ref = self.bigquery_client.get_table_ref(client)

        job_config = bigquery.LoadJobConfig()
        job_config.schema = SchemaFormatter.format_schema(self.schema)
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

        # job_config.write_disposition = "WRITE_APPEND"
        job_config.write_disposition = "WRITE_TRUNCATE"

        JsonDataModifier.modify_json(self.json_file_path, self.data_file_path)

        with open(self.data_file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

        while job.state != "DONE":
            job.reload()
            time.sleep(2)

        if job and job.errors:
            for error in job.errors:
                print(error)
        else:
            print("Job completed successfully.")

        print(job.result())


