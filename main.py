import os

from utils.config_reader import read_config_file

from generate_fake_data.csv_generator import GenerateCsv
from refactor_data.process_and_store_csv_data import ProcessAndStoreData
from refactor_data.modify_json_for_upload import JsonDataModifier

from bigquery_data_upload.upload_data_to_BigQuery import BigQueryDataUploader


def main():
    # get config file path
    script_directory = os.path.dirname(os.path.abspath(__file__))
    CONFIG_FILE_PATH = os.path.join(script_directory, 'config', 'config.ini')

    # getting file paths dict from a config file
    config_dict = read_config_file(CONFIG_FILE_PATH)

    # generating fake csv data
    generator = GenerateCsv(config_dict)
    generator.generate_csv()

    # storing csv data into jsons
    process_data = ProcessAndStoreData(config_dict)
    new_json_path = process_data.keep_data_in_json()
    JsonDataModifier.modify_json(new_json_path, config_dict)

    # uploading data to BigQuery
    upload_data_to_bigquery = BigQueryDataUploader(config_dict, new_json_path)
    upload_data_to_bigquery.write_files_to_cloud()


if __name__ == "__main__":
    main()
