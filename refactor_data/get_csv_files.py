import os


class GetCsvFiles:
    """ class to get a list of csv file paths in the given directory """
    def __init__(self, csv_directory: str):
        self.directory_path = csv_directory
        self.csv_paths = []

    def get_csv_paths(self) -> list:
        for filename in os.listdir(self.directory_path):
            if filename.endswith(".csv"):
                self.csv_paths.append(os.path.join(self.directory_path, filename))
        return self.csv_paths
