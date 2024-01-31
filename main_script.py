from dataAcquisition import ParqueDownloader
from dataCleaner import DataCleaner
from dataLoading import DataLoader
import os
import sys
from logger import DataLogger

def main():
# Step 1: Data Acquisition
        url = input("Enter the URL from where you want to download the dataset")

        # creating a class instance
        downloader = ParqueDownloader(url)

        # look for parque links in the provided URL and get those links
        parque_links = downloader.get_parque_links()

        if not parque_links:
            print("No Parque links found.")
            return
        
        # Provide the destination path name to store the parque files, create a directory if it doesn't exists
        dest_path = input("Enter the destination  path where you want to store the parque files: ")
        os.makedirs(dest_path, exist_ok = True)

        # download the Parque files
        downloader.download_parque_files(parque_links, dest_path)
        logger = DataLogger(log_file="main_log.txt")
        logger.log_info("Data acquisition completed.")

# Step 2: Data Cleaning
        file_path = input("Enter the path to the PARQUET file:")
        cleaner = DataCleaner(file_path=file_path)
        cleaner.Cleaner()
        logger.log_info("Data cleaning completed.")

# Step 3: Data Loading
        try:
            print("\nDo you have a database/schema created on your MySQL server?")
            user_response = input("Enter 'yes' if you have a database, or 'no' to exit: ")

            if user_response.lower() != 'yes':
                print("Exiting the script. Please create a database on your MySQL server.")
                sys.exit(1)
            connection = DataLoader.get_db_connection()
            # load_third_party(connection, '/Users/shilpa/Documents/Data_Engineer_BootCamp_Projects/Prototyping_Datapipeline/yellow_tripdata_2023-01_cleaned.csv')
            if len(sys.argv) < 2:
                print("Please provide the path to the CSV file.")
                print("Usage: python your_script.py /path/to/your/csv/file.csv")
                sys.exit(1)

            # Get the CSV file path from the command-line argument
            csv_file_path = sys.argv[1]

            connection = DataLoader.get_db_connection()
            DataLoader.create_db_table(connection)
            DataLoader.load_yellow_taxi_records(connection, csv_file_path)
            logger.log_info("Data storage completed.")
            DataLoader.query_yellow_taxi_records(connection)

        except Exception as e:
            print(f"Main function error: {e}")
        finally:
            connection.close()


if __name__ == "__main__":
    main()
