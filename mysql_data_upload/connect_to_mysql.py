import mysql.connector

from etl_project.logging.logger import get_logger
from etl_project.config.config_reader import get_config_value


class ConnectToMySql:
    """ creating a class to connect to MYSQL server, return the connection and cursor
    and eventually commit and close the connection
    """

    def __init__(self, config_file_path: str):

        # getting logger instance for logging
        self.logger = get_logger()

        self.mysql_host: str = get_config_value(config_file_path, "MySql", "host")
        self.mysql_user: str = get_config_value(config_file_path, "MySql", "user")
        self.mysql_password: str = get_config_value(config_file_path, "MySql", "password")
        self.mysql_charset: str = get_config_value(config_file_path, "MySql", "charset")

        self.database_name: str = get_config_value(config_file_path, "Database", "mysql_database_name")
        self.table_name: str = get_config_value(config_file_path, "Database", "mysql_table_name")

        self.conn = self.connect_to_my_sql()
        self.cursor = self.create_cursor()

    def connect_to_my_sql(self) -> mysql.connector.connection.MySQLConnection:
        """ connects to MySQL using the provided credentials"""

        conn = mysql.connector.connect(
            host=self.mysql_host,
            user=self.mysql_user,
            password=self.mysql_password,
            charset=self.mysql_charset
        )
        return conn

    def create_cursor(self) -> mysql.connector.cursor.MySQLCursor:
        """ creating a cursor to execute SQL queries"""

        return self.conn.cursor()

    def commit_and_close_connection(self) -> None:
        """ commits changes to MySQL and closes the connection
        in case of an error during the commit, it rolls back changes and logs the error
        """

        try:
            self.conn.commit()

        except Exception as error:
            self.conn.rollback()

            self.logger.error(f"error: {error}")
        finally:
            self.cursor.close()
            self.conn.close()

