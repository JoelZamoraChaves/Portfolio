from datetime import datetime
import requests as req
from bs4 import BeautifulSoup as bs4
import pandas as pd
import numpy as np
import sqlite3

url = 'https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
log_file = 'code_log.txt'
exchange_rate = 'exchange_rate.csv'
csv_path = 'Largest_banks_data.csv'
table_attr = ["Bank_name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'

'''
1) Upload the image ‘Task_1_log_function.png’. This should show the code for the function 
`log_progress()` used in the project. (1 point)
'''
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + "," + message + "\n")
# Task_1_log_function.png

'''
2) Upload the image ‘Task_2a_extract.png’. This should be the snapshot of the html code 
obtained by inspecting the table on the webpage. 
The contents of the first row should be expanded and visible. (1 point)
'''
# Task_2a_extract.png

'''
3) Upload the image ‘Task_2b_extract.png’. This should show the code for the function 
‘extract()’ used in the project. (1 point)
'''
def extract(url, table_attr):
    html_text = req.get(url).text
    html_object = bs4(html_text, 'html.parser')
    table = html_object.findAll('tbody')[0]
    rows = table.findAll('tr')
    df = pd.DataFrame(columns=table_attr)
    for row in rows:
        data = row.findAll('td')
        if len(data) != 0:
            data_dict = {
                'Bank_name': data[1].findAll('a')[1].contents[0],
                'MC_USD_Billion': float(data[2].contents[0])
            }
            new_df = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, new_df], ignore_index=True)
    return df
# Task_2b_extract.png

'''
4) Upload the image ‘Task_2c_extract.png’. This should be the output obtained by 
executing the function call. (1 point)
'''
log_progress('Extracting data')
df = extract(url, table_attr)
# Task_2c_extract.png

'''
5) Upload the image ‘Task_3a_transform.png’. This should show the code for the function 
‘transform’ used in the project. (1 point)
'''
def transform(df):
    exchange_rate_df = pd.read_csv('exchange_rate.csv')
    exchange_rate_dict = dict(zip(exchange_rate_df.iloc[:, 0], exchange_rate_df.iloc[:, 1]))
    df.insert(2, "MC_GBP_Billion", None)
    df.insert(3, "MC_EUR_Billion", None)
    df.insert(4, "MC_INR_Billion", None)
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate_dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate_dict['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate_dict['INR'],2) for x in df['MC_USD_Billion']]
    return df
# Task_3a_transform.png

'''
6) Upload the image ‘Task_3b_transform.png’. This should be the output of the final 
transformed dataframe. (1 point)
'''
log_progress('Transforming data')
df = transform(df)
# Task_3b_transform.png

'''
8) Upload the image ‘Task_4_5_save_file.png’. This should show the code for both 
`load_to_csv()` and `load_to_db()` functions used in the project. (1 point)
'''
def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

# Task_4_5_save_file.png

'''
7) Upload the image ‘Task_4_CSV.png’. This should be the contents of the CSV file 
created from the final table. (1 point)
'''
log_progress('Loading data to .csv')
load_to_csv(df, csv_path)
# Task_4_CSV.png

'''
9) Upload the image ‘Task_6_SQL.png’. This should be the output of the SQL queries 
run on the database table. (1 point)
'''
def run_queries(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

log_progress('Loading data to sql and running queries')
sql_connection = sqlite3.connect(db_name)
load_to_db(df, sql_connection, table_name)
run_queries('SELECT * FROM Largest_banks', sql_connection)
run_queries('SELECT AVG(MC_GBP_Billion) FROM Largest_banks', sql_connection)
run_queries('SELECT Bank_name from Largest_banks LIMIT 5', sql_connection)
# Task_6_SQL.png

'''
10) Upload the image ‘Task_7_log_content.png’. This should be the contents of the 
log file ‘code_log.txt’. (1 point)
'''
# Task_7_log_content.png