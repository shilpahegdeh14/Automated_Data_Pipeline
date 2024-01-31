import mysql.connector
import csv
from datetime import datetime
import sys

class DataLoader():
    def get_db_connection(): 
        connection=None 
        try: 
            connection=mysql.connector.connect(
                user='test_user', 
                password='Shils@007', 
                host= '127.0.0.1', 
                port='3306', 
                # database='yellow_taxi_data_test'
                database='Yellow_Taxi_Trip_Records'
                ) 
        except Exception as error: 
            print("Error while connecting to database for job tracker",error) 
        return connection

    def create_db_table(connection):   
        try:
            cursor = connection.cursor()
            cursor.execute("DROP TABLE IF EXISTS Yellow_Taxi_Trip_Records")
            sql = '''
                CREATE TABLE Yellow_Taxi_Trip_Records (
                    VendorID INT,
                    tpep_pickup_datetime DATETIME,
                    tpep_dropoff_datetime DATETIME,
                    passenger_count FLOAT,  
                    trip_distance FLOAT, 
                    RatecodeID FLOAT, 
                    store_and_fwd_flag VARCHAR(50), 
                    PULocationID INT, 
                    DOLocationID INT,
                    payment_type INT, 
                    fare_amount FLOAT, 
                    extra FLOAT, 
                    mta_tax FLOAT, 
                    tip_amount FLOAT, 
                    tolls_amount FLOAT, 
                    improvement_surcharge FLOAT, 
                    total_amount FLOAT, 
                    congestion_surcharge FLOAT, 
                    airport_fee   FLOAT       
                )
            '''
            cursor.execute(sql)  
            print(" create database table query executed successfully!")
        except Exception as e:
            print(f"Error: {e}")  
        finally:
            cursor.close()
            print("Closed database cursor.")
        return


    def load_yellow_taxi_records(connection,file_path_csv): 
        # print("Inserting csv file to database")
        try:
            cursor=connection.cursor()
            
            # SQL insert statement
            insert_query = """
            INSERT INTO Yellow_Taxi_Trip_Records 
            (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID,
                        payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            #open and read csv file
            with open(file_path_csv, 'r') as csv_file:
                print(f"Reading CSV file: {file_path_csv}")
                csv_reader = csv.reader(csv_file)
                next(csv_reader)  # Skip the header row if present

                for row in csv_reader:
                # Convert data types if needed
                    VendorID = int(row[0])
                    tpep_pickup_datetime = datetime.strptime(row[1],'%Y-%m-%d %H:%M:%S').date() if row[1] else None
                    tpep_dropoff_datetime = datetime.strptime(row[2],'%Y-%m-%d %H:%M:%S').date() if row[2] else None
                    passenger_count = float(row[3])
                    trip_distance  = float(row[4])
                    RatecodeID = float(row[5])
                    store_and_fwd_flag  = row[6]
                    PULocationID = int(row[7])
                    DOLocationID = int(row[8])
                    payment_type = int(row[9])
                    fare_amount   = float(row[10])
                    extra = float(row[11])
                    mta_tax = float(row[12])
                    tip_amount = float(row[13])
                    tolls_amount = float(row[14])
                    improvement_surcharge = float(row[15])
                    total_amount = float(row[16])
                    congestion_surcharge = float(row[17])
                    airport_fee = float(row[18])

                    row_data = (
                        VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID,
                        payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee
                    )
                    # Execute the insert query
                    cursor.execute(insert_query, row_data)
                    print(f"Inserted data: {row_data}")
            connection.commit()
            print("Committed changes to the database.")
        
        except Exception as e:
            print(f"Error: {e}")
        finally:
            cursor.close()
        # print("Closed database cursor.")
        return
    def query_yellow_taxi_records(connection): 
    #Get the most popular ticket in the pastmonth 
        print("Querying the database:")
        try:
            sql_statement = """ 
                SELECT *
                FROM Yellow_Taxi_Trip_Records.Yellow_Taxi_Trip_Records
                LIMIT 10;
            """
        
            cursor=connection.cursor() 
            print("Executing the SQL statement")
            cursor.execute(sql_statement) 
            # print("Fetching the Data")
            records = cursor.fetchall() 

            # printing each row in the record set
            print("Here are the top records for Yellow Taxi Trip:")
            for record in records:
                print(record)     
    
        except Exception as e:
            print(f"Error: {e}")
        finally:
            cursor.close() 
        return records
"""
    def main():
        try:
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
            DataLoader.query_yellow_taxi_records(connection)

        except Exception as e:
            print(f"Main function error: {e}")
        finally:
            connection.close()




if __name__ == "__main__":
    # Execute the main function
    main()
"""