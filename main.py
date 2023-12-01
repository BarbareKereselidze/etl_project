from fake_csv_data_generator import GenerateFakeCsv
from csv_to_json import ProcessAndStoreData


def main():

    generator = GenerateFakeCsv()
    generator.generate_csv("csv_files")

    process_date = ProcessAndStoreData("csv_files", "json_files")
    process_date.keep_data_in_json()


if __name__ == "__main__":
    main()


