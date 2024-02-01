# Automated Data Pipeline for NYC Yellow Taxi Records

## Overview

This repository contains a set of Python scripts that implement an end-to-end automated data pipeline. The pipeline encompasses data acquisition, cleaning/transformation, and storage. The scripts follow object-oriented programming (OOP) concepts, providing modularity and maintainability.

## Contents

1. **dataAcquisition.py**: Module responsible for downloading data from a specified source URL.

2. **dataCleaner.py**: Module for cleaning and transforming the acquired data.

3. **dataLoading.py**: Module for loading the data stored in CSV to a structured storage system, either locally or in the cloud. Here it's using MySQL to create and store the Taxi records.

4. **logger.py**: Module handling logging functionalities for relevant information, warnings, and errors.

5. **main_script.py**: Master script that orchestrates the entire data pipeline by calling the activities in order.

## Usage

1. **Clone the Repository:**
   ```
   git clone https://github.com/your_username/automated-data-pipeline.git
   cd automated-data-pipeline
   ```

2. **Install Dependencies:**
   ```
   pip install -r requirements.txt
   ```
3. **Configuration:**
  While running the master_script.py, you need to provide the URL from where you wan to download the actual dataset.

4. **Run the Data Pipeline:**
     ```
     python master_script.py

  If you're using Python3, use
     ```
       python3 master_script.py
     ```
     
5. Logging:

    Check the generated log file (main_log.txt, dataCleaner_log.txt, dataAquisition_log.txt) for information, warnings, and errors related to each step of the pipeline.

## Notes
  The pipeline performs a basic cleaning and transformation of the dataset in the data_cleaner.py script. You can adjust the data cleaning according to your specific requirements.

  The data_storage.py script contains a placeholder for storage logic. You can modify the storage to a local or cloud location based on your needs.

## Author
  - Shilpa Hegde (shilpahegdeh14@gmail.com)


