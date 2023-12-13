# ETL Project README
This Python ETL project is designed to automate the process of generating fake CSV data, processing and refactoring the data, 
and uploading it to Google BigQuery. 
The project is organized into several directories, each serving a specific purpose.


## Notes
* Two types of fake CSV data are generated with every run of the project.
* The project has error handling with loggers in try-except blocks which ensures smooth execution.
* Before running make sure to install the needed modules provided in requirements.txt and change config.ini file with your configuration details.
  

## Project Structure:
1. **Config**
   * **config.ini** <br>
     This configuration file is located in the config directory and contains sections for File paths and BigQuery configurations.
   * **config_reader.py**: <br>
     The get_config_value function returns the value from the config.ini file.
2. **Logging**
   * **logger.py** <br>
     The get_logger function returns a configured logger using the Python logging library. 
3. **Generate Fake Data**
   * **fake_csv_data_generator.py** <br>
     This module, located in the generate_fake_data directory, utilizes the Faker library to create fake user and job data.
   * **fake_data_generator.py** <br>
     Generates lists of fake user and job data.
   * **fake_data_to_csv.py** <br>
     Converts lists of fake data into CSV files, including timestamps in the filenames.
4. **Refactor Data**
   * **get_csv_files.py** <br>
     Retrieves the paths of CSV files.
   * **process_csv.py** <br>
     Analyzes CSV files and provides summary statistics.
   * **process_and_store_csv_data.py** <br>
     Processes CSV files and stores summary data in JSON files
   * **modify_json_for_upload.py** <br>
     Adds created_at and modified_fields to each csv_file description.
5. **Data**
   * **csv_files**
   * **json_files**
   * The data directory is where generated CSV and JSON files are stored. If the csv_files and json_files directories do not exist, the code will generate them and log the directory creation.
6. **BigQuery Schema**
   * **table_schema.py** <br>
     Holds the table schema for BigQuery.
   * **format_schema.py** <br>
     Formats the schema to turn them into BigQuery.SchemaField objects.
7. **BigQuery Data Upload**
   * **connect_to_bigquery.py** <br>
     Initializes a BigQueryClient, and connects to the table
   * **upload_data_to_cloud.py** <br>
     Uploads new or modified data into the BigQuery table.
8. **Main**
   * **main.py** <br>
   * The main script running the entire ETL process. It runs all the classes in a sequence, ensuring that data is generated, processed, and uploaded systematically.
9. **Requirements**
    * **requirements.txt**  <br>
         Lists project dependencies generated from pipreqs. Ensure these dependencies are installed using:
        ```
       pip install -r requirements.txt.
         ```

        
## How to run:

1. Install project dependencies by running:
   ```
   pip install -r requirements.txt
   ```
2. Modify the **config.ini** file in the config directory with your specific configuration details.
3. To automate the code execution, use the Linux cron job scheduler.



## Scheduling with Linux Cron:

To keep the data up-to-date, the program is scheduled to run daily every hour.

### To automate the program to run daily you can follow these steps:
1. Open your terminal.
2. Edit your crontab file by running: crontab -e
3. Add the following line at the end of the file:
   ```
   0 0 * * * /path/to/python /path/to/main.py
   0 * * * * /path/to/python /path/to/main.py
