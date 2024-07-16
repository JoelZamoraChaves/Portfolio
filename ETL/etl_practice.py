'''
Create a folder data_source. Create a file etl_practice.py in this folder.

Download and unzip the data available in the link shared above.
https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip

The data available has four headers: 'car_model', 'year_of_manufacture', 'price', 'fuel'. Implement the extraction process for the CSV, JSON, and XML files.

Transform the values under the 'price' header such that they are rounded to 2 decimal places.

Implement the loading function for the transformed data to a target file, transformed_data.csv.

Implement the logging function for the entire process and save it in log_file.txt.

Test the implemented functions and log the events as done in the lab.
'''

import pandas as pd
import glob
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt"
target_file = "transformed_data.csv"

def extract_from_csv(file):
    df = pd.read_csv(file)
    return df

def extract_from_xml(file):
    df = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
    root = ET.parse(file).getroot()
    for item in root:
        car_model = item.find("car_model").text
        year_of_manufacture = item.find("year_of_manufacture").text
        price = float(item.find("price").text)
        fuel = item.find("fuel").text
        df = pd.concat([df, pd.DataFrame([{"car_model": car_model, "year_of_manufacture": year_of_manufacture, "price": price, "fuel": fuel}])], ignore_index=True)
    return df

def extract():
    df = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
    csv_files = glob.glob("*.csv")
    xml_files = glob.glob("*.xml")
    for csv_file in csv_files:
        df = pd.concat([df, pd.DataFrame(extract_from_csv(csv_file))], ignore_index=True)
    for xml_file in xml_files:
        df = pd.concat([df, pd.DataFrame(extract_from_xml(xml_file))], ignore_index=True)
    return df


def transform(data):
    data["price"] = round(data["price"], 2)
    return data

def load(data, target_file):
    data.to_csv(target_file)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + "," + message + "\n")


# Execution
 
log_progress("ETL Job Started") 
log_progress("Extract phase Started") 
extracted_data = extract() 
log_progress("Extract phase Ended") 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
log_progress("Transform phase Ended") 
log_progress("Load phase Started") 
load(transformed_data, target_file) 
log_progress("Load phase Ended") 
log_progress("ETL Job Ended") 