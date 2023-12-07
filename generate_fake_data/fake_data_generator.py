import os
import csv
from datetime import datetime

from etl_project.generate_fake_data.csv_generator import GenerateFakeData
from etl_project.logging.logger import get_logger


class GenerateCsv:
    """ class to generate csv files from fake user and fake job data """

    def __init__(self, csv_directory_path: str):
        fake_data_generator = GenerateFakeData()

        self.fake_user_data = fake_data_generator.generate_fake_user()
        self.fake_job_data = fake_data_generator.generate_fake_job()

        # path of where to store the files
        self.csv_path = csv_directory_path

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
        self.create_csv(self.fake_user_data)
        self.create_csv(self.fake_job_data)

