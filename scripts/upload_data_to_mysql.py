import os
import json
from datetime import datetime

from connect_to_mysql import ConnectToMySql


class CreateDatabaseAndTable(ConnectToMySql):
    """ creates a MySQL database if it does not already exist
    creates a table in the specified database if it does not already exist.
    the table has predefined columns for tracking information about uploaded csv files.
    """

    def create_database(self) -> None:
        self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")

    def create_table(self) -> None:
        self.cursor.execute(f"USE {self.database_name}")

        create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            csv_filename VARCHAR(255),
            csv_file_size_in_mb FLOAT,
            df_of_csv_rows_n INT,
            df_of_csv_columns_n FLOAT,
            df_of_column_size_in_mb FLOAT,
            df_size_in_mb FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        '''
        self.cursor.execute(create_table_query)


class UploadData:
    def __init__(self, config_file_path: str, json_file_path: str):
        connection = ConnectToMySql(config_file_path)

        self.database_name: str = connection.database_name
        self.table_name: str = connection.table_name

        self.conn: str = connection.conn
        self.cursor: str = connection.cursor

        self.json_file_path = json_file_path

    def get_json_files(self) -> list:
        """ retrieves a list of JSON files in the specified directory """

        return [json_file for json_file in os.listdir(self.json_file_path) if json_file.endswith('.json')]

    def get_each_json_file(self) -> None:
        """ processes each JSON file in the directory, cleaning and uploading the data to the MySQL table."""

        self.cursor.execute(f"USE {self.database_name}")

        for json_file in self.get_json_files():

            with open(os.path.join(self.json_file_path, json_file), 'r') as file:
                data = json.load(file)
                self.clean_data(data)

    def clean_data(self, data: dict) -> None:
        """ cleans and processes the data from each JSON file
        updates existing records or inserts new records into the MySQL table
        """

        for entry in data:
            filename, entry_data = entry.popitem()
            select_query = f"SELECT * FROM {self.table_name} WHERE csv_filename = %s"
            self.cursor.execute(select_query, (filename,))
            existing_data = self.cursor.fetchone()

            if existing_data:
                update_query = f"UPDATE {self.table_name} SET modified_at = %s WHERE csv_filename = %s"
                self.cursor.execute(update_query, (datetime.now(), filename))
            else:
                self.insert_data(filename, entry_data)

    def insert_data(self, filename: str, entry_data: dict) -> None:
        """ inserts data into mysql table"""

        insert_query = f"""
        INSERT INTO {self.table_name}
        (csv_filename, csv_file_size_in_mb, df_of_csv_rows_n, df_of_csv_columns_n, 
        df_of_column_size_in_mb, df_size_in_mb)
        VALUES (%s, %s, %s, %s, %s, %s);
        """

        values = (
            filename,
            entry_data['csv_file_size_in_mb'],
            entry_data['df_of_csv_rows_n'],
            entry_data['df_of_csv_columns_n'],
            entry_data['df_of_column_size_in_mb'],
            entry_data['df_size_in_mb'],
        )

        self.cursor.execute(insert_query, values)


