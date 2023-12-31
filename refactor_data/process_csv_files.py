import os
import hashlib
import pandas as pd

from utils.file_size_wrapper import get_file_size_in_mb


class ProcessCSV:
    """ class to process csv files and extract information about their size and structure """

    def __init__(self, csv_directory: str) -> None:
        self.csv_path = csv_directory
        self.df = pd.read_csv(csv_directory)

    # get csv file size in mbs
    @get_file_size_in_mb
    def get_csv_size(self) -> float:
        csv_size_in_bytes = os.path.getsize(self.csv_path)

        return csv_size_in_bytes

    # get the row amount of csv file
    def get_row_amount(self) -> float:
        row_count = self.df.shape[0]

        return row_count

    # get the column amount of csv file
    def get_column_amount(self) -> float:
        column_count = self.df.shape[1]

        return column_count

    # get size of each column in RAM
    @get_file_size_in_mb
    def get_column_sizes(self) -> list:
        column_size_in_bytes = self.df.memory_usage(deep=True)
        column_size_in_mb = column_size_in_bytes

        """the first column size is the index column size which is basically 0 and is not in the list of columns,
        so I start iterating from index 1 instead of 0
        """
        column_sizes_in_mb = column_size_in_mb.iloc[1:]

        column_names = self.df.columns.tolist()

        column_size_list = [
            {column_name: size
             for column_name, size in zip(column_names, column_sizes_in_mb)
             }
        ]

        return column_size_list

    # get size of one column in RAM
    @get_file_size_in_mb
    def get_one_column_size(self):
        df_size_in_bytes = self.df.memory_usage(deep=True).iloc[1]

        return df_size_in_bytes

    # get the size of csv dataframe in RAM
    @get_file_size_in_mb
    def get_df_size(self) -> float:
        df_size_in_bytes = self.df.memory_usage(deep=True).sum()

        return df_size_in_bytes

    # generate unique hash for each of the csvs
    def get_hash(self) -> str:
        hasher = hashlib.sha256()

        with open(self.csv_path, 'rb') as file:
            for chunk in iter(lambda: file.read(4096), b''):
                hasher.update(chunk)

        return hasher.hexdigest()
