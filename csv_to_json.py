import os
import pandas as pd
import json
from datetime import datetime


class GetCsvFiles:
    def __init__(self, csv_directory):
        self.directory_path = csv_directory
        self.csv_paths = []

    def get_csv_paths(self) -> list:
        for filename in os.listdir(self.directory_path):
            if filename.endswith(".csv"):
                self.csv_paths.append(os.path.join(self.directory_path, filename))
        return self.csv_paths


class ProcessCSV:
    def __init__(self, csv_directory):
        self.csv_path = csv_directory
        self.df = pd.read_csv(csv_directory, encoding='latin1')

    def get_csv_size(self) -> float:
        csv_size_in_bytes = os.path.getsize(self.csv_path)
        csv_size_in_mb = csv_size_in_bytes / (1024 ** 2)

        return csv_size_in_mb.__round__(3)

    def get_row_amount(self) -> float:
        row_count = self.df.shape[0]

        return row_count

    def get_column_amount(self) -> float:
        column_count = self.df.shape[1]

        return column_count

    def get_column_size(self) -> list:
        column_size_in_bytes = self.df.memory_usage(deep=True)
        column_size_in_mb = column_size_in_bytes / (1024 ** 2)

        column_names = self.df.columns.tolist()

        column_size_list = [
            {column_name: round(size, 3)}
            for column_name, size in zip(column_names, column_size_in_mb)

        ]

        return column_size_list

    def get_df_size(self) -> float:
        df_size_in_bytes = self.df.memory_usage(deep=True).sum()
        df_size_in_mb = df_size_in_bytes / (1024 ** 2)

        return df_size_in_mb.__round__(3)


class ProcessAndStoreData:
    def __init__(self, csv_directory, json_directory):
        self.csv_directory = csv_directory
        self.json_directory = json_directory
        self.get_files = GetCsvFiles(csv_directory).get_csv_paths()
        self.all_csv_data = []

    def turn_data_into_dict(self) -> list:

        for self.csv_directory in self.get_files:
            csv_data = {}

            processor = ProcessCSV(self.csv_directory)
            filename = os.path.basename(self.csv_directory)

            csv_data[filename] = {
                'csv_file_size_in_mb': processor.get_csv_size(),
                'df_of_csv_rows_n': processor.get_row_amount(),
                'df_of_csv_columns_n': processor.get_column_amount(),
                'df_size_in_mb': processor.get_df_size(),
                'df_of_column_size_in_mb': processor.get_column_size()
            }

            self.all_csv_data.append(csv_data)

        return self.all_csv_data

    def keep_data_in_json(self) -> None:
        current_timestamp = datetime.now().timestamp()
        json_directory_path = self.json_directory
        filename = f"{json_directory_path}/summary_{current_timestamp}.json"

        while True:
            try:

                with open(filename, 'w') as json_file:
                    json.dump(self.turn_data_into_dict(), json_file, indent=4)

                break

            except FileNotFoundError:
                os.makedirs(json_directory_path, exist_ok=True)
