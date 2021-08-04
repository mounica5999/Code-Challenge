import pyspark
from pyspark.sql import SparkSession
from datetime import datetime
from src.main.python.dbConnection import *
import os

sparkClassPath = os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.1.1 pyspark-shell'

fileName="../../../resources/inbound/US_COVID_SHORT_SAMPLE_DataChallenge.csv"

executionMode = read_config('executionMode')
mode = read_config('dataWriteMode')
url_connect = read_config('urlConnect')
properties = read_config('connProperties')

table = "Covid_Data_Analysis"
conn_type="jdbc"

spark = SparkSession.builder.appName("DemoApp").master(executionMode).getOrCreate()
dfRaw = spark.read.format('csv').option('sep',',').option("header", "true").schema('submission_date string, state string, total_cases string, new_case string, total_deaths string, new_death string ').load(fileName)
print ("Count after loading raw data :" + str(dfRaw.count()))

dfRaw.createOrReplaceTempView("rawData")

modifiedDF = spark.sql(""" select 
                                TO_DATE(CAST(UNIX_TIMESTAMP(submission_date, 'MM/dd/yyyy')AS TIMESTAMP)) as submission_date,
                                state,
                                INT(replace(total_cases,",","")) as total_cases,
                                INT(replace(new_case,",","")) as new_case,
                                INT(replace(total_deaths,",","")) as total_deaths,
                                INT(replace(new_death,",","")) as new_death, 
                                case when INT(replace(new_case,",","")) > 50 then 'HIGH' when INT(replace(new_case,",","")) >20 and INT(replace(new_case,",","")) <=50 then 'MEDIUM' when INT(replace(new_case,",","")) <=20 then 'LOW' end as covid_case_rate , 
                                case when INT(replace(new_death,",",""))  > 10 then 'HIGH' when INT(replace(new_death,",",""))  >5 and INT(replace(new_death,",",""))  <=10 then 'MEDIUM' when INT(replace(new_death,",",""))  <=5 then 'LOW' end as covid_death_rate 
                            from rawData 
                            """)

print ("Count after loading new columns :" + str(dfRaw.count()))

url, properties  = db_connection_open(conn_type)

try:
    modifiedDF.write.jdbc(url=url, table=table, mode="overwrite", properties=properties)
except p.Error as e:
    print("Error while sending request for deleting and inserting data")
    print(e)





