# Python Data Engineering Project — IBM Professional Certificate

This repository contains the final hands-on project from the IBM Data Engineering Professional Certificate, focused on implementing an end-to-end Python ETL pipeline.
The project extracts financial data from a web source, transforms it using exchange rate inputs, and loads the final results into both CSV and SQLite database formats.

## Project Overview

This project demonstrates a practical data engineering workflow using Python, covering:

1- Web scraping with requests and BeautifulSoup

2- Data transformation using pandas and numpy

3- Automated logging to track pipeline execution

4- ETL pipeline orchestration in Python

5- Loading data into CSV and a SQLite database

6- Running analytical SQL queries programmatically

The goal is to extract a list of the world’s largest banks by market capitalization, enrich the dataset with currency conversions, and store the processed data in structured formats.

## Key Features:

1 - Extraction:

Scrapes the archived version of Wikipedia’s Largest Banks list

Parses HTML using BeautifulSoup

Extracts bank names and market cap (USD billions)

2 - Transformation:

Reads exchange rates from exchange_rate.csv

Converts USD market cap into:

-GBP

-EUR

-INR

Adds new currency columns to the dataset

Uses numpy for rounding and efficient computation

3 - Loading:

Saves processed dataset to Largest_banks_data.csv
Loads enriched data into a SQLite database as table Largest_banks

4 - SQL Queries
Automatically executes:

SELECT * FROM Largest_banks

SELECT AVG(MC_GBP_Billion) FROM Largest_banks

SELECT Name FROM Largest_banks LIMIT 5

5 - Log every step of the code execution

## Repository Structure:
```pgsql
project/
│
├── final_project.py          # Main ETL pipeline
├── exchange_rate.csv         # Currency conversion input
├── code_log.txt              # Pipeline execution logs (generated)
├── Largest_banks_data.csv    # Output CSV (generated)
├── Banks.db                  # SQLite DB (generated)
└── README.md                 # Documentation

```

## How to Run the Project
1. Install dependencies
```bash
pip install pandas numpy requests bs4
```
2. Run the ETL pipeline
```bash
python final_project.py
```
## Output

Largest_banks_data.csv is created

Banks.db is created with table Largest_banks

code_log.txt logs all stages

Query results are printed to the terminal

Sample:

```python-repl
             Name  MC_USD_Billion  MC_GBP_Billion  MC_EUR_Billion  MC_INR_Billion
0  JPMorgan Chase            396.0          312.84           362.28          32741.4
1   Bank of America         278.0          219.20           253.18          22853.6
...

```

## Technologies Used:

Python

pandas

numpy

requests

BeautifulSoup4

SQLite

SQL

Jupyter (optional for exploration)

## Skills Demonstrated:

ETL pipeline design

Web scraping

Data transformation and enrichment

Logging and observability

SQL integration in Python

Database schema design

Reusable modular functions

Data quality checks

Automation best practices


## Author:

Alexandre Neto

IBM Data Engineering Professional Certificate

LinkedIn: https://www.linkedin.com/in/alexandre-neto-04a1a122b/

