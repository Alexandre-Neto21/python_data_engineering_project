import pandas as pd
import numpy as np
import requests
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime


# Log function:

def log_progress(log_message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # Get real time
    timestamp = now.strftime(timestamp_format)
    with open (log_file, 'a') as file:
        file.write(timestamp + ',' + log_message + '\n')


# Extract function:

def extract(url,attributes):
    ''' This function extracts the required information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    html_page = requests.get(url, verify=False).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns = attributes)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[1].find('a') is not None:
                data_dict = {'Name' : col[1].find_all("a")[1].get_text(strip=True),
                             'MC_USD_Billion' : float(col[2].contents[0])}
                df1 = pd.DataFrame(data_dict, index = [0])
                df = pd.concat([df, df1], ignore_index = True)

    return df

# Transform function:
def transform(df, csv_file):
    ''' This function accesses the CSV file for exchange rate information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to respective currencies'''
    exchange_db = pd.read_csv(csv_file)
    exchange_rate = exchange_db.set_index("Currency")["Rate"].to_dict()

    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion']*exchange_rate['GBP'],2)
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion']*exchange_rate['EUR'],2)
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion']*exchange_rate['INR'],2)

    return df

# Load to csv Function:
def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)

# Load to database Function:
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)

# Fuction to run SQL Queries:
def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_statement)
    print(query_output)

#Declaring Variables

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
output_csv_path = 'Largest_banks_data.csv'
initial_table_attributes = ['Name', 'MC_USD_Billion']
database = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'
exchange_rate_file = 'exchange_rate.csv'

log_progress("Preliminaries complete. Initiating ETL process") 

# Extraction:
extracted_df = extract(url,initial_table_attributes)
#print(extracted_df)

log_progress("Data extraction complete. Initiating Transformation process") 

# Transformation:
transformed_df = transform(extracted_df,exchange_rate_file)
#print(transformed_df)
#print(transformed_df['MC_EUR_Billion'][4])

log_progress("Data transformation complete. Initiating Loading process") 

# Loading to csv:
load_to_csv(transformed_df,output_csv_path)

log_progress("Data saved to CSV file") 

# Initiating database connection:
sql_connection = sqlite3.connect(database)
log_progress("SQL Connection initiated") 

# Loading table to database:
load_to_db(transformed_df, sql_connection, table_name)

log_progress("Data loaded to Database as a table, Executing queries") 

#Executing SQL queries:
query_statement = f'SELECT * FROM {table_name}'
run_query(query_statement, sql_connection)

query_statement = f'SELECT AVG(MC_GBP_Billion) FROM {table_name}'
run_query(query_statement, sql_connection)

query_statement = f'SELECT Name FROM {table_name} LIMIT 5'
run_query(query_statement, sql_connection)

log_progress("Process Complete") 

sql_connection.close()

log_progress("Server Connection closed") 