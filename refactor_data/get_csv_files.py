import os


# :TODO No need to create separate class for this. Should be somewhere in Utils class, or
# :TODO have it just like normal function. When OOP solution is required, it does not mean,
# :TODO you should do every piece of code in each different class, like this. "OverOOP" solution exposed!
class GetCsvFiles:
    """ class to get a list of csv file paths in the given directory """
    def __init__(self, csv_directory: str) -> None:
        self.directory_path = csv_directory
        self.csv_paths = []

    def get_csv_paths(self) -> list:
        for filename in os.listdir(self.directory_path):
            if filename.endswith(".csv"):
                self.csv_paths.append(os.path.join(self.directory_path, filename))
        return self.csv_paths
