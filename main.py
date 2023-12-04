from fake_csv_data_generator import GenerateFakeCsv
from csv_to_json import ProcessAndStoreData
from config_reader import read_config_file


def main():

    file_path = 'config.ini'
    csv_path, json_path = read_config_file(file_path)

    generator = GenerateFakeCsv()
    generator.generate_csv(csv_path)

    process_date = ProcessAndStoreData(csv_path, json_path)
    process_date.keep_data_in_json()


if __name__ == "__main__":
    main()


