import pandas as pd
import numpy as np
from datetime import datetime
from src.main.python.dbConnection import *

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 2000)
pd.set_option('display.max_colwidth', None)


fileName="../../../resources/inbound/US_COVID_SHORT_SAMPLE_DataChallenge.csv"

#get variables from config File


executionMode = read_config('executionMode')


table = "Covid_Data_Analysis"
conn_type="alchemy"

dfRaw =pd.read_csv(fileName)

print ("Count after loading raw data :" + str(dfRaw.submission_date.count()))

dfModified = dfRaw.copy()
dfModified['submission_date'] = pd.to_datetime(dfModified['submission_date'],format='%m/%d/%Y')
dfModified['total_cases'] = dfModified['total_cases'].str.replace(',','').astype('int')
dfModified['new_case'] = dfModified['new_case'].str.replace(',','').astype('int')
dfModified['total_deaths'] = dfModified['total_deaths'].str.replace(',','').astype('int')
dfModified['new_death'] = dfModified['new_death'].str.replace(',','').astype('int')

conditions_covid_case_rate = [
    (dfModified['new_case'] > 50),
    (dfModified['new_case'] > 20) & (dfModified['new_case'] <= 50),
    (dfModified['new_case'] <= 20)
    ]
conditions_covid_death_rate = [
    (dfModified['total_deaths'] > 10),
    (dfModified['total_deaths'] > 5) & (dfModified['total_deaths'] <= 10),
    (dfModified['total_deaths'] <= 5)
    ]
conditions_values = ['HIGH','MEDIUM','LOW']


dfModified['covid_case_rate'] = np.select(conditions_covid_case_rate, conditions_values)
dfModified['covid_death_rate'] = np.select(conditions_covid_death_rate, conditions_values)

print ("Count after modifying data :" + str(dfModified.submission_date.count()))
conn, cur  = db_connection_open(conn_type)

try:
    dfModified.to_sql(table, conn,if_exists='replace',index=False)
except p.Error as e:
    print("Error while sending request for deleting and inserting data")
    print(e)

db_connection_close(conn_type,conn,cur)




