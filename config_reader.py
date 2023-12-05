import configparser


class ReadConfigFile:
    def __init__(self, config_file_path: str):
        self.config = configparser.ConfigParser()
        self.config.read(config_file_path)

    def get_csv_path(self) -> str:
        return self.config.get('Paths', 'csv_path')

    def get_json_path(self) -> str:
        return self.config.get('Paths', 'json_path')

    def get_google_credentials(self) -> str:
        return self.config.get('Paths', 'google_application_credentials_path')

    def get_mysql_host(self) -> str:
        return self.config.get('MySql', 'host')

    def get_mysql_user(self) -> str:
        return self.config.get('MySql', 'user')

    def get_mysql_password(self) -> str:
        return self.config.get('MySql', 'password')

    def get_mysql_charset(self) -> str:
        return self.config.get('MySql', 'charset')

    def get_mysql_database_name(self) -> str:
        return self.config.get('Database', 'mysql_database_name')

    def get_mysql_table_name(self) -> str:
        return self.config.get('Database', 'mysql_table_name')

    def get_bigquery_project_id(self) -> str:
        return self.config.get('BigQuery', 'project_id')

    def get_bigquery_dataset_id(self) -> str:
        return self.config.get('BigQuery', 'dataset_id')

    def get_bigquery_table_id(self) -> str:
        return self.config.get('BigQuery', 'table_id')

