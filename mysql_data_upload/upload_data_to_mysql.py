import json
from datetime import datetime

from etl_project.mysql_data_upload.connect_to_mysql import ConnectToMySql


class UploadData:
    def __init__(self, config_file_path: str):
        connection = ConnectToMySql(config_file_path)

        self.database_name: str = connection.database_name
        self.table_name: str = connection.table_name

        self.conn: str = connection.conn
        self.cursor: str = connection.cursor

    def get_json_file(self, json_file: str) -> None:
        """ processes each JSON file in the directory, cleaning and uploading the data to the MySQL table."""

        self.cursor.execute(f"USE {self.database_name}")

        with open(json_file, 'r') as json_file:
            data = json.load(json_file)

            self.clean_data(data)

    def clean_data(self, data: str) -> None:
        """ cleans and processes the data from latest JSON file
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


