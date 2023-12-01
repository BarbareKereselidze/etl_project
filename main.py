import os
import pandas as pd
import json
from datetime import datetime

from fake_csv_data_generator import GenerateFakeCsv

path = "csv_files"


class GetCsvFiles:
    def __init__(self, directory):
        self.directory_path = directory
        self.csv_paths = []

    def get_csv_paths(self):
        for filename in os.listdir(self.directory_path):
            if filename.endswith(".csv"):
                self.csv_paths.append(os.path.join(self.directory_path, filename))
        return self.csv_paths


class ProcessCSV:
    def __init__(self, csv_path):
        self.csv_path = csv_path
        self.df = pd.read_csv(csv_path, encoding='latin1')

    def get_csv_size(self):
        csv_size_in_bytes = os.path.getsize(self.csv_path)
        csv_size_in_mb = csv_size_in_bytes / (1024 ** 2)

        return csv_size_in_mb.__round__(3)

    def get_row_amount(self):
        row_count = self.df.shape[0]

        return row_count

    def get_column_amount(self):
        column_count = self.df.shape[1]

        return column_count

    def get_column_size(self):
        column_size_in_bytes = self.df.memory_usage(deep=True)
        column_size_in_mb = column_size_in_bytes / (1024 ** 2)

        column_names = self.df.columns.tolist()

        column_size_list = [
            {column_name: round(size, 3)}
            for column_name, size in zip(column_names, column_size_in_mb)

        ]

        return column_size_list

    def get_df_size(self):
        df_size_in_bytes = self.df.memory_usage(deep=True).sum()
        df_size_in_mb = df_size_in_bytes / (1024 ** 2)

        return df_size_in_mb.__round__(3)


class KeepDataInDict:
    def __init__(self):
        self.get_files = GetCsvFiles("csv_files").get_csv_paths()
        self.all_csv_data = []

    def process_and_store_data(self):

        for csv_path in self.get_files:
            csv_data = {}

            processor = ProcessCSV(csv_path)
            filename = os.path.basename(csv_path)
            # print(filename)

            csv_data[filename] = {
                'csv_file_size_in_mb': processor.get_csv_size(),
                'df_of_csv_rows_n': processor.get_row_amount(),
                'df_of_csv_columns_n': processor.get_column_amount(),
                'df_size_in_mb': processor.get_df_size(),
                'df_of_column_size_in_mb': processor.get_column_size()
            }

            self.all_csv_data.append(csv_data)

        return self.all_csv_data


def main():

    generator = GenerateFakeCsv()
    generator.generate_csv("csv_files")

    data_keeper = KeepDataInDict()
    aaa = data_keeper.process_and_store_data()

    current_timestamp = datetime.now().timestamp()

    with open(f'json_files/summary_{current_timestamp}.json', 'w') as json_file:
        json.dump(aaa, json_file, indent=4)


if __name__ == "__main__":
    main()


