import json
from datetime import datetime


class JsonDataModifier:
    @staticmethod
    def modify_json(json_file_path, data_file_path):
        current_datetime = datetime.now()
        formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

        json_data = json.load(open(json_file_path))
        adjusted_json_data = [
            {
                'csv_file_name': list(row.keys())[0],
                'created_at': formatted_datetime,
                'modified_at': formatted_datetime,
                'csv_file_info': row[list(row.keys())[0]]
            }
            for row in json_data
        ]

        with open(data_file_path, "w") as adjusted_file:
            adjusted_file.write('\n'.join(json.dumps(row) for row in adjusted_json_data))
