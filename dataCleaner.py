import pandas as pd
import os
import logging

class DataCleaner:

    """
    A class for cleaning and transforming data from PARQUET files.

    Attributes:
    - file_path (str): The path to the PARQUET file to be processed.

    Methods:
    - __init__(self, file_path): Constructor method to initialize the DataCleaner object.
    - _setup_logger(self): Private method to set up logging configuration.
    - Cleaner(self, file_path): Method to clean and transform the data in the specified PARQUET file.

    Usage:
    ```
    cleaner = DataCleaner(file_path='/path/to/your/file.parquet')
    cleaner.Cleaner(file_path='/path/to/another/file.parquet')
    ```
    """
    def __init__(self, file_path):
        #self.file_path = "/Users/shilpa/Documents/Data_Engineer_BootCamp_Projects/Prototyping_Datapipeline/yellow_tripdata_2023-01.parquet"
        self.df = pd.read_parquet(file_path)
        logging.basicConfig(filename='dataCleaner_log.txt', level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')
        try:
            self.df = pd.read_parquet(file_path)
        except Exception as e:
            logging.error(f"Error reading PARQUET file: {e}")
            raise ValueError("Failed to initialize DataCleaner.")



def Cleaner(self, file_path):
    """
        Method to clean and transform the data in the specified PARQUET file.

        Parameters:
        - file_path (str): The path to the PARQUET file to be processed.
    """

    logging.info(f"\nCleaning data for file: {file_path}")
    print("Columns in the file {file_path}:")
    print(df.head())

    # Look for missing data (null values)
    print("\nMissing Data:")
    print(df.isnull().sum())

    # Display columns that contain negative values
    print("\nColumns with Negative Values:")
    numeric_columns = df.select_dtypes(include=['number']).columns
    negative_columns = (df[numeric_columns] < 0).any()
    print(negative_columns[negative_columns].index)

    # replace all the numeric columns that consists of negative values to '0'
    for column in numeric_columns:
        df.loc[df[column] < 0, column] = 0

    # checking for duplicates
    print("\nDuplicate Rows:")
    duplicates = df[df.duplicated()]
    print(duplicates)


    # Remove duplicates
    df = df.drop_duplicates()

    # Replace null values with 0
    df = df.fillna(0)

    # Display cleaned data
    print("\nCleaned Data:")
    print(df.head())

    """
        STEP 3: Transform the data
                After once I clean the data in PARQUE file, I transform them into a CSV that's reading to be loaded into structured data storage systems
                split the filename and its extension. [0] extracts the first part of the result, which is the filename without the extension
    """  
    csv_file_path = os.path.splitext(file_path)[0] + "_cleaned.csv"
    df.to_csv(csv_file_path, index=False)

    logging.info(f"\nCleaned data saved to: {csv_file_path}\n")

#def main(self):
    #file_path = input("Enter the path to the PARQUET file:")
    #cleaner = DataCleaner(file_path=file_path)

#if __name__ == "__main__":
    #runner = DataCleaner()
    #runner.main()

