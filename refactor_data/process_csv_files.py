import os
import pandas as pd


class ProcessCSV:
    """ class to process csv files and extract information about their size and structure """

    def __init__(self, csv_directory: str):
        self.csv_path = csv_directory
        self.df = pd.read_csv(csv_directory)

    # get csv file size in mbs
    def get_csv_size(self) -> float:
        csv_size_in_bytes = os.path.getsize(self.csv_path)
        csv_size_in_mb = csv_size_in_bytes / (1024 ** 2)

        return csv_size_in_mb.__round__(3)

    # get the row amount of csv file
    def get_row_amount(self) -> float:
        row_count = self.df.shape[0]

        return row_count

    # get the column amount of csv file
    def get_column_amount(self) -> float:
        column_count = self.df.shape[1]

        return column_count

    # get size of each column in RAM
    def get_column_sizes(self) -> list:
        column_size_in_bytes = self.df.memory_usage(deep=True)
        column_size_in_mb = column_size_in_bytes / (1024 ** 2)

        """the first column size is the index column size which is basically 0 and is not in the list of columns,
        so I start iterating from index 1 instead of 0
        """
        column_sizes_in_mb = column_size_in_mb.iloc[1:]

        column_names = self.df.columns.tolist()

        column_size_list = [
            {column_name: round(size, 3)
             for column_name, size in zip(column_names, column_sizes_in_mb)
             }
        ]

        return column_size_list

    # get size of one column in RAM
    def get_one_column_size(self):
        df_size_in_bytes = self.df.memory_usage(deep=True).iloc[1]
        one_df_size_in_mb = df_size_in_bytes / (1024 ** 2)

        return one_df_size_in_mb.__round__(3)

    # get the size of csv dataframe in RAM
    def get_df_size(self) -> float:
        df_size_in_bytes = self.df.memory_usage(deep=True).sum()
        df_size_in_mb = df_size_in_bytes / (1024 ** 2)

        return df_size_in_mb.__round__(3)
