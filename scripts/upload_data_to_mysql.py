import os
import mysql.connector
import json
from datetime import datetime
import pandas as pd
from etl_project.config.config_reader import ReadConfigFile


class ConnectToMySql:
    def __init__(self, config_file_path):
        config_reader = ReadConfigFile(config_file_path)

        self.mysql_host = config_reader.get_mysql_host()
        self.mysql_user = config_reader.get_mysql_user()
        self.mysql_password = config_reader.get_mysql_password()
        self.mysql_charset = config_reader.get_mysql_charset()

        self.database_name = config_reader.get_mysql_database_name()
        self.table_name = config_reader.get_mysql_table_name()

        self.conn = self.connect_to_my_sql()
        self.cursor = self.create_cursor()

    def connect_to_my_sql(self):
        conn = mysql.connector.connect(
            host=self.mysql_host,
            user=self.mysql_user,
            password=self.mysql_password,
            charset=self.mysql_charset
        )
        return conn

    def create_cursor(self):
        return self.conn.cursor()

    def commit_and_close_connection(self):
        try:
            self.conn.commit()
        except Exception as error:
            self.conn.rollback()
            print(f"Error: {error}")
        finally:
            self.cursor.close()
            self.conn.close()


class CreateDatabaseAndTable:
    def __init__(self, config_file_path):
        connection = ConnectToMySql(config_file_path)

        self.database_name = connection.database_name
        self.table_name = connection.table_name

        self.conn = connection.conn
        self.cursor = connection.cursor

    def create_database(self):
        self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")

    def create_table(self):
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
    def __init__(self, config_file_path, json_file_path):
        connection = ConnectToMySql(config_file_path)

        self.database_name = connection.database_name
        self.table_name = connection.table_name

        self.conn = connection.conn
        self.cursor = connection.cursor

        self.json_file_path = json_file_path

    def get_json_files(self):
        return [json_file for json_file in os.listdir(self.json_file_path) if json_file.endswith('.json')]

    def get_each_json_file(self):
        self.cursor.execute(f"USE {self.database_name}")
        for json_file in self.get_json_files():
            with open(os.path.join(self.json_file_path, json_file), 'r') as file:
                data = json.load(file)
                self.clean_data(data)

    def clean_data(self, data):
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

    def insert_data(self, filename, entry_data):
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


class GetMysqlData:

    def __init__(self, config_file_path):
        connection = ConnectToMySql(config_file_path)

        self.database_name = connection.database_name
        self.table_name = connection.table_name

        self.conn = connection.conn
        self.cursor = connection.cursor

    def select_all_data(self):
        try:
            self.cursor.execute(f"USE {self.database_name}")
            select_query = f"SELECT * FROM {self.table_name}"
            self.cursor.execute(select_query)

            # Fetch all records and get column names
            records = self.cursor.fetchall()
            column_names = [desc[0] for desc in self.cursor.description]

            # Create a Pandas DataFrame
            df = pd.DataFrame(records, columns=column_names)
            return df

        except Exception as e:
            print(f"Error: {e}")
            return None




















