import os
import csv
from datetime import datetime

from config.config_reader import get_config_value
from generate_fake_data.fake_data_generator import GenerateFakeData

from logging.logger import get_logger


class GenerateCsv:
    """ class to generate csv files from fake user and fake job data """

    def __init__(self, config_file_path: str) -> None:
        fake_data_generator = GenerateFakeData()

        self.fake_user_data = fake_data_generator.generate_fake_user()
        self.fake_job_data = fake_data_generator.generate_fake_job()

        # path of where to store the files
        self.csv_path = get_config_value(config_file_path, "Paths", "csv_path")

        # getting logger instance for logging
        self.logger = get_logger()

    def create_csv(self, csv_data) -> None:
        """ create a csv file with the provided fake data """

        # getting current timestamp for csv name
        timestamp = datetime.now().timestamp()
        filename = f"{self.csv_path}/fake_data_{timestamp}.csv"

        while True:
            try:
                fake_data = csv_data

                with open(filename, 'w', newline='') as csvfile:
                    csv_writer = csv.writer(csvfile)
                    csv_writer.writerows(fake_data)

                break

            except FileNotFoundError:
                # create the directory where csvs are kept if it doesn't exist
                os.makedirs(self.csv_path, exist_ok=True)

                self.logger.error(f"created directory as: {self.csv_path}")

    def generate_csv(self) -> None:
        """ generate fake user and job data csv files """

        # :TODO Needs to add/extend capability to receive list of arguments
        # :TODO or at least have those two calls in for loop. By doing so, we
        # :TODO follow DRY rule (Don't repeat yourself)
        self.create_csv(self.fake_user_data)
        self.create_csv(self.fake_job_data)

