from bigquery_data_upload.upload_data_to_cloud import UploadDataToBigQuery

from generate_fake_data.csv_generator import GenerateCsv
from refactor_data.process_and_store_csv_data import ProcessAndStoreData


def main():
    # getting file paths from a config file
    # :TODO If we include hardcoded path of config file, it must be relative path.
    # :TODO Also, while this is a constant, it should be UPPERCASE registry, according to the convention
    config_file_path = '/home/user/PycharmProjects/csv_project/etl_project/config/config.ini'

    # generating fake csv data
    generator = GenerateCsv(config_file_path)
    generator.generate_csv()

    # storing csv data into jsons
    process_data = ProcessAndStoreData(config_file_path)
    new_json_path = process_data.keep_data_in_json()

    # uploading data to BigQuery
    upload_data_to_bigquery = UploadDataToBigQuery(config_file_path, new_json_path)
    upload_data_to_bigquery.write_files_to_cloud()


if __name__ == "__main__":
    main()
