import os
import csv
import json
import time
import pandas as pd
import numpy as np
import sqlite3 as sql
from openpyxl.workbook import Workbook
from os.path import exists as file_exists

try:
    # Connect to database
    conn=sql.connect('/Users/<USER_NAME>/Desktop/happiness_correlation.db')
    cur = conn.cursor()
    print('------------------------------------')
    print('happiness_correlation Database connected.')
    print('------------------------------------')


    # Grab all the data from the database and convert them into dataframe format
    df = pd.DataFrame(pd.read_csv("stats_master.csv", converters = {'year': lambda x: str(x)}))
    df1 = pd.DataFrame(pd.read_csv("2015.csv", converters = {'Year': lambda x: str(x), 'Happiness.Rank': lambda x: str(x)}))
    df2 = pd.DataFrame(pd.read_csv("2016.csv", converters = {'Year': lambda x: str(x), 'Happiness.Rank': lambda x: str(x)}))
    df3 = pd.DataFrame(pd.read_csv("2017.csv", converters = {'Year': lambda x: str(x), 'Happiness.Rank': lambda x: str(x)}))
    df4 = pd.DataFrame(pd.read_csv("2018.csv", converters = {'Year': lambda x: str(x), 'Happiness.Rank': lambda x: str(x)}))
    df5 = pd.DataFrame(pd.read_csv("2019.csv", converters = {'Year': lambda x: str(x), 'Happiness.Rank': lambda x: str(x)}))
    
    
    # Check if newly created table exists in SQLite database
    cur.execute(''' SELECT * FROM sqlite_master WHERE type = 'table' AND name = 'suicide_data' ''')
    
    ret = bool(cur.fetchone())
    
    if not ret:
        print("NO SUCH TABLE EXISTS: suicide_data")
        df.to_sql('suicide_data', conn)
        print("suicide_data Table creation successful.")
    print('Suicide data successfully exported.')
    print('------------------------------------')
    
    rows = cur.fetchall()
    for row in rows:
        print(row)
        
        
    # Create 2015 Database in SQLite database
    cur.execute(''' SELECT * FROM sqlite_master WHERE type = 'table' AND name = '2015_data' ''')
    
    ret = bool(cur.fetchone())
    
    if not ret:
        print("NO SUCH TABLE EXISTS: 2015_data")
        df1.to_sql('2015_data', conn)
        print("2015_data Table creation successful.")
        
    # print("******2015 Table Data*******")
    # cur.execute('''SELECT * FROM 2015_data''')
    print('2015 data successfully exported.')
    print('------------------------------------')
    rows = cur.fetchall()
    
    for row in rows:
        print(row)
    
    
    # Create 2016 Database in SQLite database    
    cur.execute(''' SELECT * FROM sqlite_master WHERE type = 'table' AND name = '2016_data' ''')
    ret = bool(cur.fetchone())
    
    if not ret:
        print("NO SUCH TABLE EXISTS: 2016_data")
        df2.to_sql('2016_data', conn)
        print("2016_data Table creation successful.")
        
    # print("******2016 Table Data*******")
    # cur.execute('''SELECT * FROM 2016_data''')
    print('2016 data successfully exported.')
    print('------------------------------------')
    rows = cur.fetchall()
    
    for row in rows:
        print(row)
        
        
    # Create 2017 Database in SQLite database
    cur.execute(''' SELECT * FROM sqlite_master WHERE type = 'table' AND name = '2017_data' ''')
    
    ret = bool(cur.fetchone())
    
    if not ret:
        print("NO SUCH TABLE EXISTS: 2017_data")
        df3.to_sql('2017_data', conn)
        print("2017_data Table creation successful.")
        
    # print("******2017 Table Data*******")
    # cur.execute('''SELECT * FROM 2017_data''')
    print('2017 data successfully exported.')
    print('------------------------------------')
    rows = cur.fetchall()
    
    for row in rows:
        print(row)
        
        
    # Create 2018 Database in SQLite database
    cur.execute(''' SELECT * FROM sqlite_master WHERE type = 'table' AND name = '2018_data' ''')
    
    ret = bool(cur.fetchone())
    
    if not ret:
        print("NO SUCH TABLE EXISTS: 2018_data")
        df4.to_sql('2018_data', conn)
        print("2018_data Table creation successful.")
        
    # print("******2018 Table Data*******")
    # cur.execute('''SELECT * FROM 2018_data''')
    print('2018 data successfully exported.')
    print('------------------------------------')

    rows = cur.fetchall()
    
    for row in rows:
        print(row)
        
        
    # Create 2019 Database in SQLite database   
    cur.execute(''' SELECT * FROM sqlite_master WHERE type = 'table' AND name = '2019_data' ''')
    
    ret = bool(cur.fetchone())
    
    if not ret:
        print("NO SUCH TABLE EXISTS: 2019_data")
        df5.to_sql('2019_data', conn)
        print("2019_data Table creation successful.")
        
    # print("******2019 Table Data*******")
    # cur.execute('''SELECT * FROM 2019_data''')
    print('2019 data successfully exported.')
    print('------------------------------------')
    rows = cur.fetchall()
    
    for row in rows:
        print(row)
    
    
    # Execute SQL Query into CSV file for DBMS or Data Visualization tools
    print ("Exporting data into CSV...")
    cursor = conn.cursor()
    cursor.execute(''' WITH master_suicide AS (
                        SELECT country, year, sex, age, population, [suicides/100k pop] 'suicides_per_100k', [gdp_for_year($)] 'annual_gdp($)', generation
                        FROM suicide_data
                    ),
                    data_2015_2016 AS (
                        SELECT a.Year, a.Country, a.Region, a.[Happiness Score], a.[Economy (GDP per Capita)] 'gdp_per_capita', a.[Health (Life Expectancy)] 'life_expectancy', a.Freedom, a.[Trust (Government Corruption)] 'trust_factor', a.[Dystopia Residual] 'dystopia_residual' 
                        FROM [2015_data] a
                            UNION
                        SELECT b.Year, b.Country, b.Region, b.[Happiness Score], b.[Economy (GDP per Capita)] 'gdp_per_capita', b.[Health (Life Expectancy)] 'life_expectancy', b.Freedom, b.[Trust (Government Corruption)] 'trust_factor', b.[Dystopia Residual] 'dystopia_residual' 
                        FROM [2016_data] b
                    )
                    SELECT DENSE_RANK () OVER ( 
                            PARTITION BY s.country 
                            ORDER BY s.suicides_per_100k DESC
                        ) suicide_rate_rank, s.country, d.Region, s.year, s.sex, s.age, s.population, s.suicides_per_100k, 
                        LAG ( s.suicides_per_100k, 1, 0 ) OVER (
                            PARTITION BY s.country
                            ORDER BY s.suicides_per_100k 
                        ) next_stat_100k,
                        (s.suicides_per_100k - LAG ( s.suicides_per_100k, 1, 0 ) OVER (
                            PARTITION BY s.country
                            ORDER BY s.suicides_per_100k 
                        )) nextstat_per_100k_diff, s.[annual_gdp($)], s.generation, d.[Happiness Score], d.[gdp_per_capita], d.life_expectancy, d.Freedom, d.trust_factor, d.dystopia_residual
                    FROM master_suicide s
                    LEFT JOIN data_2015_2016 d
                    ON s.country = d.Country AND s.year = d.Year
                    WHERE (s.year = '2015' OR s.year = '2016')
                    AND d.Country IS NOT NULL
                    ''')
    data = cursor.fetchall()
    
    
    # Reads SQL results and exports to a CSV file
    with open("final_data.csv", "w") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
        for row in data:
            csv_writer.writerow(row)

    dirpath = os.getcwd() + "/final_data.csv"
    print ("Data exported Successfully into {}".format(dirpath))
    
    # Export as Excel file
    read_file = pd.read_csv(r'/Users/<USER_NAME>/Desktop/happiness_correlation/final_data.csv')
    read_file.to_excel (r'/Users/<USER_NAME>/Desktop/happiness_correlation/final_data_excel.xlsx', index = None, header=True)
    print('Excel file succesfully created.')
    
    
except sql.Error as e:
    print(e)
    
finally:
    conn.close()
    
