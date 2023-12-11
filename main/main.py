from etl_project.bigquery_data_upload.upload_data_to_cloud import UploadDataToBigQuery

from etl_project.generate_fake_data.csv_generator import GenerateCsv
from etl_project.refactor_data.process_and_store_csv_data import ProcessAndStoreData


def main():

    # getting file paths from a config file
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






