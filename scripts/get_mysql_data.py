import pandas as pd

from connect_to_mysql import ConnectToMySql


class GetMysqlData(ConnectToMySql):
    """ select all data from MYSQL to upload it to cloud after """

    def select_all_data(self) -> pd.DataFrame:
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

        except Exception as error:
            self.logger.error(f"error: {error}")

            # returning an empty dataframe in case of an error
            return pd.DataFrame()

