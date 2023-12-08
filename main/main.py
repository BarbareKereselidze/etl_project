from etl_project.config.config_reader import get_config_value

from etl_project.generate_fake_data.csv_generator import GenerateCsv
from etl_project.refactor_data.process_and_store_csv_data import ProcessAndStoreData

from etl_project.mysql_data_upload.connect_to_mysql import ConnectToMySql
from etl_project.mysql_data_upload.create_mysql_database_and_table import CreateDatabaseAndTable
from etl_project.mysql_data_upload.upload_data_to_mysql import UploadData

from etl_project.bigquery_data_upload.upload_data_to_cloud import UploadDataToBigQuery


def main():

    # getting file paths from a config file
    config_file_path = '/home/user/PycharmProjects/csv_project/etl_project/config/config.ini'

    csv_path = get_config_value(config_file_path, "Paths", "csv_path")
    json_path = get_config_value(config_file_path, "Paths", "json_path")

    # generating fake csv data
    generator = GenerateCsv(csv_path)
    generator.generate_csv()

    # storing csv data into jsons
    process_data = ProcessAndStoreData(csv_path, json_path)
    new_json_path = process_data.keep_data_in_json()

    # uploading json data into mysql
    create_db_instance = CreateDatabaseAndTable(config_file_path)
    create_db_instance.create_database()
    create_db_instance.conn.commit()

    create_table_instance = CreateDatabaseAndTable(config_file_path)
    create_table_instance.create_table()
    create_table_instance.conn.commit()

    upload_data_instance = UploadData(config_file_path)
    upload_data_instance.get_json_file(new_json_path)
    upload_data_instance.conn.commit()

    ConnectToMySql(config_file_path).commit_and_close_connection()

    # upload data to cloud
    upload_to_cloud = UploadDataToBigQuery(config_file_path)
    upload_to_cloud.upload_data()


if __name__ == "__main__":
    main()






