'''
Modify the code to extract Film, Year, and Rotten Tomatoes' Top 100 headers.

Restrict the results to only the top 25 entries.

Filter the output to print only the films released in the 2000s (year 2000 included).
'''

import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
csv_path = 'top_25_films.csv'
df = pd.DataFrame(columns=["Rotten Tomatoes", "Film", "Year"])
count = 0

html_page = requests.get(url).text # Page code
data = BeautifulSoup(html_page, 'html.parser')

tables = data.findAll("tbody")
rows = tables[0].findAll("tr")

for row in rows:
    if count < 25:
        elements = row.findAll("td")
        if len(elements) != 0:
            data_dict = {
                "Film": elements[1].contents[0],
                "Rotten Tomatoes": elements[3].contents[0],
                "Year": elements[2].contents[0]
            }
            year = int(data_dict["Year"])
            if year >= 2000:
                df2 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df2], ignore_index=True)
                count += 1
    else: break

print(df)
df.to_csv(csv_path)