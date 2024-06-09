import mysql.connector
import csv
import pandas as pd
from bokeh.plotting import figure, output_file, show
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import unittest

# Custom exceptions
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
    """
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None

    def connect(self):
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
        if self.connection:
            self.cursor.close()
            self.connection.close()
            print("Disconnected from the database.")

class CSVImporter(DatabaseConnector):
    def __init__(self, host, user, password, database):
        super().__init__(host, user, password, database)

    def import_csv_to_db(self, csv_file_path, table_name):
        try:
            with open(csv_file_path, 'r') as csvfile:
                csvreader = csv.reader(csvfile)
                next(csvreader)  # Skip the header row if present

                # Insert each row into the database
                for row in csvreader:
                    values = ', '.join(['"' + str(val) + '"' for val in row])
                    query = f"INSERT INTO {table_name} VALUES (NULL, {values})"
                    self.cursor.execute(query)

            self.connection.commit()
            print("Data imported successfully.")
        except FileNotFoundError as e:
            raise CSVImportError(f"CSV file not found: {e}")
        except mysql.connector.Error as err:
            raise CSVImportError(f"Error importing CSV to database: {err}")

class DataProcessor(DatabaseConnector):
    def __init__(self, host, user, password, database):
        super().__init__(host, user, password, database)
        self.engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

    def process_test_data(self, test_csv_file_path, ideal_functions_table, result_table):
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
    def __init__(self, host, user, password, database):
        super().__init__(host, user, password, database)
        self.engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

    def visualize_data(self, train_table, test_table, result_table):
        try:
            train_data = pd.read_sql(f"SELECT * FROM {train_table}", self.engine)
            test_data = pd.read_sql(f"SELECT * FROM {test_table}", self.engine)
            result_data = pd.read_sql(f"SELECT * FROM {result_table}", self.engine)

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
    try:
        host = "localhost"
        user = "root"
        password = ""
        database = "erasto_database"

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

        importer = CSVImporter(host, user, password, database)
        importer.connect()

        csv_file_path = r"C:\laragon\www\My python Projects\test.csv"
        train_csv_file_path = r"C:\laragon\www\My python Projects\train.csv"
        ideal_csv_file_path = r"C:\laragon\www\My python Projects\ideal.csv"
        test_table_name = "erasto_database.test"
        train_table_name = "erasto_database.train"
        ideal_table_name = "erasto_database.ideal"
        result_table_name = "erasto_database.mapping"

        importer.import_csv_to_db(csv_file_path, test_table_name)
        importer.import_csv_to_db(train_csv_file_path, train_table_name)
        importer.import_csv_to_db(ideal_csv_file_path, ideal_table_name)

        processor = DataProcessor(host, user, password, database)
        processor.connect()
        processor.process_test_data(csv_file_path, ideal_table_name, result_table_name)
        processor.disconnect()

        visualizer = DataVisualizer(host, user, password, database)
        visualizer.connect()
        visualizer.visualize_data(train_table_name, test_table_name, result_table_name)
        visualizer.disconnect()

    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()

# Unit tests
class TestDatabaseConnector(unittest.TestCase):
    def setUp(self):
        self.db_connector = DatabaseConnector("localhost", "root", "", "erasto_database")
        self.db_connector.connect()

    def tearDown(self):
        self.db_connector.disconnect()

    def test_connection(self):
        self.assertIsNotNone(self.db_connector.connection)
        self.assertIsNotNone(self.db_connector.cursor)

class TestCSVImporter(unittest.TestCase):
    def setUp(self):
        self.importer = CSVImporter("localhost", "root", "", "erasto_database")
        self.importer.connect()
        self.importer.cursor.execute("CREATE TABLE IF NOT EXISTS test_import (id INT AUTO_INCREMENT PRIMARY KEY, x FLOAT, y FLOAT)")

    def tearDown(self):
        self.importer.cursor.execute("DROP TABLE IF EXISTS test_import")
        self.importer.disconnect()

    def test_import_csv_to_db(self):
        test_csv = r"C:\laragon\www\My python Projects\test.csv"
        self.importer.import_csv_to_db(test_csv, "test_import")
        self.importer.cursor.execute("SELECT COUNT(*) FROM test_import")
        count = self.importer.cursor.fetchone()[0]
        self.assertGreater(count, 0)

class TestDataProcessor(unittest.TestCase):
    def setUp(self):
        self.processor = DataProcessor("localhost", "root", "", "erasto_database")
        self.processor.connect()
        self.processor.cursor.execute("CREATE TABLE IF NOT EXISTS ideal_test (id INT AUTO_INCREMENT PRIMARY KEY, x FLOAT, y1 FLOAT, y2 FLOAT)")
        self.processor.cursor.execute("INSERT INTO ideal_test (x, y1, y2) VALUES (1, 2, 3), (2, 3, 4)")
        self.processor.cursor.execute("CREATE TABLE IF NOT EXISTS mapping_test (id INT AUTO_INCREMENT PRIMARY KEY, x FLOAT, y FLOAT, ideal_function VARCHAR(255), deviation FLOAT)")

    def tearDown(self):
        self.processor.cursor.execute("DROP TABLE IF EXISTS ideal_test")
        self.processor.cursor.execute("DROP TABLE IF EXISTS mapping_test")
        self.processor.disconnect()

    def test_process_test_data(self):
        test_csv = r"C:\laragon\www\My python Projects\test.csv"
        self.processor.process_test_data(test_csv, "ideal_test", "mapping_test")
        self.processor.cursor.execute("SELECT COUNT(*) FROM mapping_test")
        count = self.processor.cursor.fetchone()[0]
        self.assertGreater(count, 0)

class TestDataVisualizer(unittest.TestCase):
    def setUp(self):
        self.visualizer = DataVisualizer("localhost", "root", "", "erasto_database")
        self.visualizer.connect()
        self.visualizer.cursor.execute("CREATE TABLE IF NOT EXISTS train_vis (id INT AUTO_INCREMENT PRIMARY KEY, x FLOAT, y1 FLOAT, y2 FLOAT)")
        self.visualizer.cursor.execute("INSERT INTO train_vis (x, y1, y2) VALUES (1, 2, 3), (2, 3, 4)")
        self.visualizer.cursor.execute("CREATE TABLE IF NOT EXISTS test_vis (id INT AUTO_INCREMENT PRIMARY KEY, x FLOAT, y FLOAT)")
        self.visualizer.cursor.execute("INSERT INTO test_vis (x, y) VALUES (1, 2), (2, 3)")
        self.visualizer.cursor.execute("CREATE TABLE IF NOT EXISTS mapping_vis (id INT AUTO_INCREMENT PRIMARY KEY, x FLOAT, y FLOAT, ideal_function VARCHAR(255), deviation FLOAT)")
        self.visualizer.cursor.execute("INSERT INTO mapping_vis (x, y, ideal_function, deviation) VALUES (1, 2, 'y1', 0.1), (2, 3, 'y2', 0.2)")

    def tearDown(self):
        self.visualizer.cursor.execute("DROP TABLE IF EXISTS train_vis")
        self.visualizer.cursor.execute("DROP TABLE IF EXISTS test_vis")
        self.visualizer.cursor.execute("DROP TABLE IF EXISTS mapping_vis")
        self.visualizer.disconnect()

    def test_visualize_data(self):
        self.visualizer.visualize_data("train_vis", "test_vis", "mapping_vis")
        # Since Bokeh does not return a value we can easily test, we will just ensure no exceptions are raised

if __name__ == '__main__':
    unittest.main()
