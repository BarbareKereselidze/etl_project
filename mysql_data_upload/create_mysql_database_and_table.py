from etl_project.mysql_data_upload.connect_to_mysql import ConnectToMySql


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
