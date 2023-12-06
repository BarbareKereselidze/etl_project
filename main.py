from fake_csv_data_generator import GenerateFakeCsv
from csv_to_json import ProcessAndStoreData
from upload_data_to_mysql import ConnectToMySql, CreateDatabaseAndTable, UploadData
from config_reader import ReadConfigFile
from upload_data_to_cloud import UploadDataToBigQuery


def main():

    # getting file paths from a config file
    config_file_path = 'config.ini'
    conf_reader = ReadConfigFile(config_file_path)

    csv_path = conf_reader.get_csv_path()
    json_path = conf_reader.get_json_path()

    # generating fake csv data
    generator = GenerateFakeCsv()
    generator.generate_csv(csv_path)

    # storing csv data into jsons
    process_data = ProcessAndStoreData(csv_path, json_path)
    process_data.keep_data_in_json()

    # uploading json data into mysql
    create_db_instance = CreateDatabaseAndTable(config_file_path)
    create_db_instance.create_database()
    create_db_instance.conn.commit()

    create_table_instance = CreateDatabaseAndTable(config_file_path)
    create_table_instance.create_table()
    create_table_instance.conn.commit()

    upload_data_instance = UploadData(config_file_path, json_path)
    upload_data_instance.get_each_json_file()
    upload_data_instance.conn.commit()

    ConnectToMySql(config_file_path).commit_and_close_connection()

    # upload data to cloud
    upload_to_cloud = UploadDataToBigQuery(config_file_path)
    upload_to_cloud.upload_data()


if __name__ == "__main__":
    main()






