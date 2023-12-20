import os
import json
from datetime import datetime

from refactor_data.process_csv_files import ProcessCSV
from utils.get_csv_files import get_csv_paths

from utils.logger import get_logger


class ProcessAndStoreData:
    """ class to process csv files and store summary data in a JSON file """

    def __init__(self, config_dict: dict) -> None:

        # getting logger instance for logging
        self.logger = get_logger()

        self.csv_directory = config_dict['Paths']['csv_path']
        self.json_directory = config_dict['Paths']['json_path']
        self.all_csv_data = []

        self.get_files = get_csv_paths(self.csv_directory)

    def turn_data_into_dict(self) -> list:
        """ convert csv data into a list of dictionaries """

        for self.csv_directory in self.get_files:
            csv_data = {}

            processor = ProcessCSV(self.csv_directory)
            filename = os.path.basename(self.csv_directory)

            csv_data[filename] = [{
                'csv_file_size_in_mb': processor.get_csv_size(),
                'df_of_csv_rows_n': processor.get_row_amount(),
                'df_of_csv_columns_n': processor.get_column_amount(),
                'df_size_in_mb': processor.get_df_size(),
                'df_of_column_size_in_mb': processor.get_one_column_size(),
                'hash': processor.get_hash(),
                'df_of_all_column_size_in_mb': processor.get_column_sizes()
            }]

            self.all_csv_data.append(csv_data)

        return self.all_csv_data

    def keep_data_in_json(self) -> str:
        """ store summary data in a JSON file """

        current_timestamp = datetime.now().timestamp()

        filename = f"{self.json_directory}/summary_{current_timestamp}.json"

        while True:
            try:

                with open(filename, 'w') as json_file:
                    json.dump(self.turn_data_into_dict(), json_file, indent=4)

                return filename

            # create the directory where JSONs are kept if it doesn't exist
            except FileNotFoundError:
                os.makedirs(self.json_directory, exist_ok=True)

                self.logger.error(f"created directory as: {self.json_directory}")
