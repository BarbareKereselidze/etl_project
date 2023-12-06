import configparser


class ReadConfigFile:
    """
        class for reading configuration values from a specified config file

        Args:
            config_file_path (str): The path to the configuration file

        Attributes:
            config (ConfigParser): An instance of ConfigParser for parsing the config file
            paths (str): The section name for paths configuration
            mysql (str): The section name for MySQL configuration
            database (str): The section name for Database configuration
            bigquery (str): The section name for BigQuery configuration
        """

    def __init__(self, config_file_path: str):
        # initializing and reading the config file
        self.config = configparser.ConfigParser()
        self.config.read(config_file_path)

        # defining the section names for easier access
        self.paths = 'Paths'
        self.mysql = 'MySql'
        self.database = 'Database'
        self.bigquery = 'BigQuery'

    def get_csv_path(self) -> str:
        return self.config.get(self.paths, 'csv_path')

    def get_json_path(self) -> str:
        return self.config.get(self.paths, 'json_path')

    def get_google_credentials(self) -> str:
        return self.config.get(self.paths, 'google_application_credentials_path')

    def get_mysql_host(self) -> str:
        return self.config.get(self.mysql, 'host')

    def get_mysql_user(self) -> str:
        return self.config.get(self.mysql, 'user')

    def get_mysql_password(self) -> str:
        return self.config.get(self.mysql, 'password')

    def get_mysql_charset(self) -> str:
        return self.config.get(self.mysql, 'charset')

    def get_mysql_database_name(self) -> str:
        return self.config.get(self.database, 'mysql_database_name')

    def get_mysql_table_name(self) -> str:
        return self.config.get(self.database, 'mysql_table_name')

    def get_bigquery_project_id(self) -> str:
        return self.config.get(self.bigquery, 'project_id')

    def get_bigquery_dataset_id(self) -> str:
        return self.config.get(self.bigquery, 'dataset_id')

    def get_bigquery_table_id(self) -> str:
        return self.config.get(self.bigquery, 'table_id')

