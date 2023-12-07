import os
import json
from datetime import datetime

from etl_project.refactor_data.process_csv_files import ProcessCSV
from etl_project.refactor_data.get_csv_files import GetCsvFiles

from etl_project.logging.logger import get_logger


class ProcessAndStoreData:
    """ class to process csv files and store summary data in a JSON file """

    def __init__(self, csv_directory, json_directory):
        self.get_files = GetCsvFiles(csv_directory).get_csv_paths()

        # getting logger instance for logging
        self.logger = get_logger()

        self.csv_directory = csv_directory
        self.json_directory = json_directory
        self.all_csv_data = []

    def turn_data_into_dict(self) -> list:
        """ convert csv data into a list of dictionaries """

        for self.csv_directory in self.get_files:
            csv_data = {}

            processor = ProcessCSV(self.csv_directory)
            filename = os.path.basename(self.csv_directory)

            csv_data[filename] = {
                'csv_file_size_in_mb': processor.get_csv_size(),
                'df_of_csv_rows_n': processor.get_row_amount(),
                'df_of_csv_columns_n': processor.get_column_amount(),
                'df_size_in_mb': processor.get_df_size(),
                'df_of_column_size_in_mb': processor.get_one_column_size(),
                'df_of_all_column_size_in_mb': processor.get_column_sizes()
            }

            self.all_csv_data.append(csv_data)

        return self.all_csv_data

    def keep_data_in_json(self) -> None:
        """ store summary data in a JSON file """

        current_timestamp = datetime.now().timestamp()
        json_directory_path = self.json_directory
        filename = f"{json_directory_path}/summary_{current_timestamp}.json"

        while True:
            try:

                with open(filename, 'w') as json_file:
                    json.dump(self.turn_data_into_dict(), json_file, indent=4)

                break

            # create the directory where JSONs are kept if it doesn't exist
            except FileNotFoundError:
                os.makedirs(json_directory_path, exist_ok=True)

                self.logger.error(f"created directory as: {self.json_directory}")
