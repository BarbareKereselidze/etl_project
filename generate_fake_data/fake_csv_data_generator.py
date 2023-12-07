import os
import csv
from faker import Faker
from random import randint
from datetime import datetime

from etl_project.logging.logger import get_logger


class GenerateFakeData:
    """ class to generate fake user and job data using the Faker library """

    def __init__(self):
        self.fake = Faker()

        # create lists to store fake data
        self.fake_user_data: list = []
        self.fake_job_data: list = []

        # randomise the number of rows in the csv
        self.row_amount: int = randint(100, 1000)

    def generate_fake_user(self) -> list:
        """ generate fake user data and return a list of lists """

        header = ["name", "birthdate", "email", "address", "phone_number"]
        self.fake_user_data.append(header)

        for _ in range(self.row_amount):
            name = self.fake.name()
            new_name = name.replace(" ", "_")
            birthdate = self.fake.date_of_birth(minimum_age=18, maximum_age=80)
            email = self.fake.email()
            address = self.fake.address()
            new_address = address.replace("\n", "_")
            new_address = new_address.replace(" ", "_")
            phone_number = self.fake.phone_number()

            self.fake_user_data.append([new_name, birthdate, email, new_address, phone_number])

        return self.fake_user_data

    def generate_fake_job(self) -> list:
        """ generate fake job data and return a list of lists """

        header = ["name", "job", "company", "salary", "credit_card_number", "credit_card_expire"]
        self.fake_job_data.append(header)

        for _ in range(self.row_amount):
            name = self.fake.name()
            new_name = name.replace(" ", "_")
            job = self.fake.job()
            company = self.fake.company()
            salary = randint(1000, 10000)
            credit_card_number = self.fake.credit_card_number()
            credit_card_expire = self.fake.credit_card_expire()

            self.fake_job_data.append([new_name, job, company, salary, credit_card_number, credit_card_expire])

        return self.fake_job_data


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

