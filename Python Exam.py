import mysql.connector
import csv
import pandas as pd
from bokeh.plotting import figure, output_file, show
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

class DatabaseConnectionError(Exception):
    """Custom exception for database connection errors."""
    pass

class CSVImportError(Exception):
    """Custom exception for CSV import errors."""
    pass

class DataProcessingError(Exception):
    """Custom exception for data processing errors."""
    pass

class DatabaseConnector:
    """
    A class used to connect to a MySQL database.

    Attributes:
        host (str): The database host.
        user (str): The database user.
        password (str): The password for the database user.
        database (str): The name of the database.
        connection (mysql.connector.connection_cext.CMySQLConnection or None): The database connection object.
        cursor (mysql.connector.cursor_cext.CMySQLCursor or None): The database cursor object.
    """

    def __init__(self, host, user, password, database):
        """
        Initializes the DatabaseConnector class with the given parameters.
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None

    def connect(self):
        """
        Connects to the MySQL database.
        """
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.cursor = self.connection.cursor()
            print("Connected to the database.")
        except mysql.connector.Error as err:
            raise DatabaseConnectionError(f"Database connection failed: {err}")

    def disconnect(self):
        """
        Disconnects from the MySQL database.
        """
        if self.connection:
            self.cursor.close()
            self.connection.close()
            print("Disconnected from the database.")

class CSVImporter(DatabaseConnector):
    """
    A class used to import CSV data into a MySQL database table, inheriting from DatabaseConnector.
    """

    def __init__(self, host, user, password, database):
        """
        Initializes the CSVImporter class with the given parameters.
        """
        super().__init__(host, user, password, database)

    def import_csv_to_db(self, csv_file_path, table_name):
        """
        Imports data from a CSV file into the specified database table.

        Parameters:
            csv_file_path (str): The path to the CSV file.
            table_name (str): The name of the table to import data into.
        """
        try:
            with open(csv_file_path, 'r') as csvfile:
                csvreader = csv.reader(csvfile)
                next(csvreader)  # Skip the header row if present

                # Insert each row into the database
                for row in csvreader:
                    values = ', '.join(['"' + str(val) + '"' for val in row])
                    query = f"INSERT INTO {table_name} VALUES (NULL, {values})"
                    self.cursor.execute(query)

            # Commit the transaction
            self.connection.commit()
            print("Data imported successfully.")
        except FileNotFoundError as e:
            raise CSVImportError(f"CSV file not found: {e}")
        except mysql.connector.Error as err:
            raise CSVImportError(f"Error importing CSV to database: {err}")

class DataProcessor(DatabaseConnector):
    """
    A class used to process test data, match it with ideal functions, and save the results.
    """

    def __init__(self, host, user, password, database):
        """
        Initializes the DataProcessor class with the given parameters.
        """
        super().__init__(host, user, password, database)
        self.engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

    def process_test_data(self, test_csv_file_path, ideal_functions_table, result_table):
        """
        Processes test data, matches it with ideal functions, and saves the results.

        Parameters:
            test_csv_file_path (str): The path to the test CSV file.
            ideal_functions_table (str): The name of the ideal functions table.
            result_table (str): The name of the table to save the results.
        """
        try:
            # Read test data line-by-line
            with open(test_csv_file_path, 'r') as csvfile:
                csvreader = csv.reader(csvfile)
                next(csvreader)  # Skip the header row if present

                for row in csvreader:
                    x = float(row[0])
                    y = float(row[1])

                    # Find the ideal function with the minimum deviation
                    query = f"SELECT * FROM {ideal_functions_table}"
                    ideal_data = pd.read_sql(query, self.engine)
                    min_deviation = float('inf')
                    chosen_function = None

                    if ideal_data.empty:
                        raise DataProcessingError("Ideal functions table is empty.")

                    for column in ideal_data.columns[1:]:  # Skip the first column (x values)
                        ideal_row = ideal_data[ideal_data['x'] == x]
                        if ideal_row.empty:
                            continue
                        ideal_y = ideal_row[column].values[0]
                        deviation = abs(y - ideal_y)
                        if deviation < min_deviation:
                            min_deviation = deviation
                            chosen_function = column

                    if chosen_function is None:
                        continue  # Skip if no matching ideal function was found

                    # Save the result
                    result_query = f"INSERT INTO {result_table} (x, y, ideal_function, deviation) VALUES ({x}, {y}, '{chosen_function}', {min_deviation})"
                    self.cursor.execute(result_query)

                self.connection.commit()
                print("Test data processed and results saved successfully.")
        except FileNotFoundError as e:
            raise CSVImportError(f"CSV file not found: {e}")
        except mysql.connector.Error as err:
            raise CSVImportError(f"Error processing test data: {err}")
        except Exception as e:
            raise DataProcessingError(f"Unexpected error: {e}")

class DataVisualizer(DatabaseConnector):
    """
    A class used to visualize data from a MySQL database table, inheriting from DatabaseConnector.
    """

    def __init__(self, host, user, password, database):
        """
        Initializes the DataVisualizer class with the given parameters.
        """
        super().__init__(host, user, password, database)
        self.engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

    def visualize_data(self, train_table, test_table, result_table):
        """
        Visualizes data from the specified database tables using Bokeh.

        Parameters:
            train_table (str): The name of the train data table.
            test_table (str): The name of the test data table.
            result_table (str): The name of the result data table.
        """
        try:
            train_data = pd.read_sql(f"SELECT * FROM {train_table}", self.engine)
            test_data = pd.read_sql(f"SELECT * FROM {test_table}", self.engine)
            result_data = pd.read_sql(f"SELECT * FROM {result_table}", self.engine)

            # Example visualization with Bokeh
            output_file("data_visualization.html")
            p = figure(title="Data Visualization", x_axis_label='X', y_axis_label='Y')

            # Plot train data
            for y_col in train_data.columns[2:]:
                p.line(train_data['x'], train_data[y_col], legend_label=f"Train {y_col}", line_width=2)

            # Plot test data
            p.circle(test_data['x'], test_data['y'], legend_label="Test Data", size=10, color='red', alpha=0.5)

            # Plot result data
            p.square(result_data['x'], result_data['y'], legend_label="Result Data", size=10, color='green', alpha=0.5)

            show(p)
        except SQLAlchemyError as e:
            raise DatabaseConnectionError(f"Database query failed: {e}")

def main():
    """
    The main function to execute the database operations and visualize data.
    """
    try:
        host = "localhost"
        user = "root"
        password = ""
        database = "erasto_database"

        # Create and connect to the database
        db_connector = DatabaseConnector(host, user, password, "")
        db_connector.connect()
        cursor = db_connector.cursor

        sql_queries = [
            "DROP DATABASE IF EXISTS Erasto_database",
            "CREATE DATABASE Erasto_database",
            "CREATE TABLE erasto_database.train (id INT AUTO_INCREMENT PRIMARY KEY, x FLOAT, y1 FLOAT, y2 FLOAT, y3 FLOAT, y4 FLOAT)",
            "CREATE TABLE erasto_database.test (id INT AUTO_INCREMENT PRIMARY KEY, x FLOAT, y FLOAT)",
            "CREATE TABLE erasto_database.best_fit_func (id INT AUTO_INCREMENT PRIMARY KEY, x INT(255), y INT(255))",
            "CREATE TABLE erasto_database.mapping (id INT AUTO_INCREMENT PRIMARY KEY, x INT(255), y INT(255), ideal_function VARCHAR(255), deviation FLOAT)",
            "CREATE TABLE erasto_database.ideal (id INT AUTO_INCREMENT PRIMARY KEY, x INT(255))",
            *[f"ALTER TABLE erasto_database.ideal ADD COLUMN y{i+1} INT;" for i in range(0, 50)]
        ]

        for query in sql_queries:
            cursor.execute(query)

        db_connector.disconnect()

        # Import CSV data
        importer = CSVImporter(host, user, password, database)
        importer.connect()

        test_csv_file_path = r"C:\laragon\www\My python Projects\test.csv"
        train_csv_file_path = r"C:\laragon\www\My python Projects\train.csv"
        ideal_csv_file_path = r"C:\laragon\www\My python Projects\ideal.csv"
        test_table_name = "erasto_database.test"
        train_table_name = "erasto_database.train"
        ideal_table_name = "erasto_database.ideal"
        result_table_name = "erasto_database.mapping"

        importer.import_csv_to_db(test_csv_file_path, test_table_name)
        importer.import_csv_to_db(train_csv_file_path, train_table_name)
        importer.import_csv_to_db(ideal_csv_file_path, ideal_table_name)

        # Process test data and save results
        processor = DataProcessor(host, user, password, database)
        processor.connect()
        processor.process_test_data(test_csv_file_path, ideal_table_name, result_table_name)
        processor.disconnect()

        # Visualize data
        visualizer = DataVisualizer(host, user, password, database)
        visualizer.connect()
        visualizer.visualize_data(train_table_name, test_table_name, result_table_name)
        visualizer.disconnect()

    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
