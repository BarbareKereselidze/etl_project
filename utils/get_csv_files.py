import os


def get_csv_paths(csv_directory: str) -> list:
    """ function to get a list of CSV file paths in the given directory """

    csv_paths = []
    for filename in os.listdir(csv_directory):
        if filename.endswith(".csv"):
            csv_paths.append(os.path.join(csv_directory, filename))
    return csv_paths
