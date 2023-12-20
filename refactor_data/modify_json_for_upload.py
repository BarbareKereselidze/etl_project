import json
from datetime import datetime


class JsonDataModifier:
    @staticmethod
    def modify_json(json_file_path: str, config_dict: dict):
        data_file_path = config_dict['Paths']['data_file_path']

        """ modifies a json file, adding timestamp information and creating a new adjusted json file """

        # get the current date and time
        current_datetime = datetime.now()
        formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

        # load json data from the input file
        json_data = json.load(open(json_file_path))

        # adjust the json data by adding timestamp information
        adjusted_json_data = [
            {
                'csv_file_name': list(row.keys())[0],
                'created_at': formatted_datetime,
                'modified_at': formatted_datetime,
                'csv_file_info': row[list(row.keys())[0]]
            }
            for row in json_data
        ]

        #  write the adjusted json data to the output file
        with open(data_file_path, "w") as adjusted_file:
            adjusted_file.write('\n'.join(json.dumps(row) for row in adjusted_json_data))
