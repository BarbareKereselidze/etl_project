# ETL Project README
This Python ETL project is designed to automate the process of generating fake CSV data, processing and refactoring the data, 
and uploading it to MySQL database which is used as an engine to upload data to Google BigQuery. 
The project is organized into several directories, each serving a specific purpose.


## Notes
* Two types of fake CSV data are generated with every run of the project.
* The project has error handling with loggers in try-except blocks which ensures smooth execution.
* Before running make sure to install the needed modules provided in requirements.txt and change config.ini file with your configuration details.
  

## Project Structure:
1. **Config**
   * **config.ini** <br>
     This configuration file is located in the config directory and contains sections for Paths, MySQL, Database, and BigQuery configurations.
   * **config_reader.py**: <br>
     The ReadConfigFile class in this module provides functions to read values from the config.ini file, ensuring that sensitive information is not hard-coded into the code.
2. **Logging**
   * **logger.py** <br>
     The get_logger function returns a configured logger using the Python logging library. Proper logging is essential for tracking the execution flow and diagnosing potential issues.
3. **Generate Fake Data**
   * **fake_csv_data_generator.py** <br>
     This module, located in the generate_fake_data directory, utilizes the Faker library to create fake user and job data.
   * **fake_data_generator.py** <br>
     Generates lists of fake user and job data.
   * **fake_data_to_csv.py** <br>
     Converts lists of fake data into CSV files, including timestamps in the filenames.
4. **Refactor Data**
   * **csv_to_json.py** <br>
     Converts CSV files to JSON format.
   * **get_csv_files.py** <br>
     Retrieves the paths of CSV files.
   * **process_csv.py** <br>
     Analyzes CSV files and provides summary statistics.
   * **process_and_store_csv_data.py** <br>
     Processes CSV files and stores summary data in JSON files
5. **Data**
   * **csv_files**
   * **json_files**
   * The data directory is where generated CSV and JSON files are stored. If the csv_files and json_files directories do not exist, the code will generate them and log the directory creation.
7. **MySql Data Upload**
   * **connect_to_mysql.py** <br>
     Establishes a connection to MySQL using configuration information.
    * **create_mysql_database_and_table.py**  <br>
     Creates a MySQL database and table if they don't already exist.
    * **upload_data_to_mysql.py** <br>
     Uploads data to MySQL, preventing duplicates.
8. **BigQuery Data Upload**
   * **get_mysql_data.py** <br>
     Retrieves data from MySQL.
   * **upload_data_to_cloud.py** <br>
     Uploads data to Google BigQuery.
9. **Main**
   * **main.py** <br>
   * The main script running the entire ETL process. It runs all the classes in a sequence, ensuring that data is generated, processed, and uploaded systematically.
10. **Requirements**
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

   ```
   path to main should be replaced by the actual path to your Main.py file.
   for example: /home/user/PycharmProjects/csv_project/etl_project/main/main.py
5. Save and exit the editor. The program will now run at every hour every day.